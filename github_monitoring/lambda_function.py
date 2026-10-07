"""
GitHub Events Ingestion Lambda
Fetches public GitHub events, runs data quality checks, and lands raw JSON
in date-partitioned S3 for downstream Athena querying.
"""

import json
import os
import time
import logging
from datetime import datetime, timezone
from collections import Counter

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config (all from env vars, nothing hardcoded) ─────────────────────────────
GITHUB_TOKEN    = os.environ["GITHUB_TOKEN"]
S3_BUCKET       = os.environ["S3_BUCKET"]
SNS_TOPIC_ARN   = os.environ.get("SNS_TOPIC_ARN", "")
PER_PAGE        = int(os.environ.get("PER_PAGE", "100"))      # max GitHub allows
PAGES           = int(os.environ.get("PAGES", "3"))            # pages per run
STALENESS_HOURS = int(os.environ.get("STALENESS_HOURS", "2")) # freshness threshold
VOLUME_DROP_PCT = float(os.environ.get("VOLUME_DROP_PCT", "50"))  # alert threshold

GITHUB_API      = "https://api.github.com/events"
REQUIRED_FIELDS = {"id", "type", "actor", "repo", "created_at", "public", "payload"}

s3  = boto3.client("s3")
sns = boto3.client("sns") if SNS_TOPIC_ARN else None


# ── GitHub API client ─────────────────────────────────────────────────────────

def fetch_events(page: int) -> list[dict]:
    """Fetch one page of public GitHub events."""
    resp = requests.get(
        GITHUB_API,
        headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        params={"per_page": PER_PAGE, "page": page},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_pages() -> list[dict]:
    """Fetch PAGES pages, deduplicating by event id across pages."""
    seen_ids: set[str] = set()
    all_events: list[dict] = []

    for page in range(1, PAGES + 1):
        events = fetch_events(page)
        if not events:
            break
        new = [e for e in events if e.get("id") not in seen_ids]
        seen_ids.update(e.get("id", "") for e in new)
        all_events.extend(new)
        logger.info("Page %d: %d events (%d new)", page, len(events), len(new))
        if page < PAGES:
            time.sleep(0.5)   # be polite to the API

    return all_events


# ── Data quality ──────────────────────────────────────────────────────────────

def check_freshness(events: list[dict]) -> tuple[bool, float | None]:
    """
    Return (is_fresh, oldest_event_age_hours).
    Fresh = at least one event within STALENESS_HOURS of now.
    """
    now = datetime.now(timezone.utc)
    ages = []
    for e in events:
        ts = e.get("created_at")
        if ts:
            try:
                ages.append((now - datetime.fromisoformat(ts.replace("Z", "+00:00"))).total_seconds() / 3600)
            except ValueError:
                pass
    if not ages:
        return False, None
    youngest = min(ages)
    return youngest <= STALENESS_HOURS, youngest


def check_schema(events: list[dict]) -> dict:
    """
    Return per-field completeness rates and a count of events missing
    any required field. Completeness = fraction of events that have the field
    and it's not None / empty string.
    """
    if not events:
        return {"completeness_by_field": {}, "incomplete_count": 0, "total": 0}

    field_hits: Counter = Counter()
    missing_required = 0

    for e in events:
        missing = REQUIRED_FIELDS - {k for k, v in e.items() if v is not None and v != ""}
        if missing:
            missing_required += 1
        for f in REQUIRED_FIELDS:
            v = e.get(f)
            if v is not None and v != "":
                field_hits[f] += 1

    n = len(events)
    return {
        "completeness_by_field": {f: round(field_hits[f] / n, 4) for f in REQUIRED_FIELDS},
        "incomplete_count": missing_required,
        "total": n,
    }


def check_duplicates(events: list[dict]) -> int:
    """Return the number of duplicate event IDs in the batch."""
    ids = [e.get("id") for e in events if e.get("id")]
    return len(ids) - len(set(ids))


def build_dq_summary(events: list[dict]) -> dict:
    """Run all DQ checks and return a combined summary dict."""
    is_fresh, youngest_age = check_freshness(events)
    schema = check_schema(events)
    dup_count = check_duplicates(events)

    event_type_dist = dict(Counter(e.get("type", "unknown") for e in events).most_common())

    summary = {
        "total_events": len(events),
        "freshness": {
            "is_fresh": is_fresh,
            "youngest_event_age_hours": round(youngest_age, 2) if youngest_age is not None else None,
            "threshold_hours": STALENESS_HOURS,
        },
        "schema": schema,
        "duplicates": {
            "duplicate_id_count": dup_count,
        },
        "event_type_distribution": event_type_dist,
    }

    issues = []
    if not is_fresh:
        issues.append(f"STALE: youngest event is {youngest_age:.1f}h old (threshold {STALENESS_HOURS}h)")
    if dup_count > 0:
        issues.append(f"DUPLICATES: {dup_count} duplicate event IDs in batch")
    if schema["incomplete_count"] > 0:
        pct = round(schema["incomplete_count"] / schema["total"] * 100, 1)
        issues.append(f"SCHEMA: {schema['incomplete_count']} events ({pct}%) missing required fields")

    summary["issues"] = issues
    summary["passed"] = len(issues) == 0
    return summary


# ── Volume comparison (day-over-day) ─────────────────────────────────────────

def get_yesterday_count(today_date: str) -> int | None:
    """
    Read yesterday's run manifest from S3 to get the event count.
    Returns None if no prior run found.
    """
    from datetime import date, timedelta
    yesterday = (date.fromisoformat(today_date) - timedelta(days=1)).isoformat()
    key = f"raw/date={yesterday}/manifest.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        manifest = json.loads(obj["Body"].read())
        return manifest.get("total_events")
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        logger.warning("Could not read yesterday's manifest: %s", exc)
        return None


# ── S3 storage ────────────────────────────────────────────────────────────────

def write_to_s3(events: list[dict], dq: dict, run_ts: str, today: str) -> str:
    """
    Write raw events JSON and a DQ manifest to date-partitioned S3 paths.
    Returns the S3 key for the events file.
    """
    run_id = run_ts.replace(":", "").replace("-", "")[:15]  # YYYYMMDDTHHMMss
    events_key  = f"raw/date={today}/run_{run_id}.json"
    manifest_key = f"raw/date={today}/manifest.json"

    # Events file
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=events_key,
        Body=json.dumps(events, ensure_ascii=False),
        ContentType="application/json",
    )

    # Manifest (DQ summary + pointer to events file)
    manifest = {
        "run_timestamp": run_ts,
        "events_key": events_key,
        **dq,
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
    )

    logger.info("Wrote %d events → s3://%s/%s", len(events), S3_BUCKET, events_key)
    return events_key


# ── Alerting ──────────────────────────────────────────────────────────────────

def maybe_alert(dq: dict, yesterday_count: int | None):
    """Send SNS alert if DQ issues or significant volume drop."""
    if not sns:
        return

    alerts = list(dq.get("issues", []))

    today_count = dq["total_events"]
    if yesterday_count is not None and yesterday_count > 0:
        drop_pct = (yesterday_count - today_count) / yesterday_count * 100
        if drop_pct >= VOLUME_DROP_PCT:
            alerts.append(
                f"VOLUME DROP: {today_count} events today vs {yesterday_count} yesterday "
                f"({drop_pct:.1f}% drop, threshold {VOLUME_DROP_PCT}%)"
            )

    if alerts:
        message = "GitHub Events ingestion alerts:\n\n" + "\n".join(f"• {a}" for a in alerts)
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject="GitHub Events DQ Alert", Message=message)
        logger.warning("SNS alert sent: %s", alerts)


# ── Lambda entrypoint ─────────────────────────────────────────────────────────

def lambda_handler(event, context):
    run_ts = datetime.now(timezone.utc).isoformat()
    today  = run_ts[:10]  # YYYY-MM-DD

    logger.info("Run start: %s — fetching %d pages × %d events", run_ts, PAGES, PER_PAGE)

    # 1. Fetch
    events = fetch_all_pages()
    if not events:
        logger.error("No events returned from GitHub API")
        return {"statusCode": 500, "body": "No events returned"}

    # 2. DQ checks
    dq = build_dq_summary(events)
    logger.info("DQ summary: %s", json.dumps(dq))

    # 3. Volume comparison
    yesterday_count = get_yesterday_count(today)

    # 4. Store
    events_key = write_to_s3(events, dq, run_ts, today)

    # 5. Alert if needed
    maybe_alert(dq, yesterday_count)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "run_timestamp": run_ts,
            "events_key": events_key,
            "total_events": dq["total_events"],
            "dq_passed": dq["passed"],
            "issues": dq["issues"],
        }),
    }
