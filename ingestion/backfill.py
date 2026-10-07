"""
GitHub Events backfill script.

The main Lambda only captures the live public event stream, which has a
rolling window of ~300 events — anything older is gone. This script pulls
multiple pages and writes them into the same date-partitioned S3 structure,
so the Athena views have historical data to work with rather than just
"since the first Lambda run."

Run once after initial deployment:
    python -m ingestion.backfill

Config (from .env or environment):
    GITHUB_TOKEN          required
    S3_BUCKET             required for --mode=s3 (default)
    BACKFILL_PAGES        how many pages to pull (default: 10, max ~300/per_page events total)
    PER_PAGE              events per page (default: 100, max GitHub allows)
    STORAGE_BACKEND       "s3" (default) or "local" (writes to ./data/raw/)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

GITHUB_TOKEN     = os.environ["GITHUB_TOKEN"]
S3_BUCKET        = os.environ.get("S3_BUCKET", "")
BACKFILL_PAGES   = int(os.environ.get("BACKFILL_PAGES", "10"))
PER_PAGE         = int(os.environ.get("PER_PAGE", "100"))
STORAGE_BACKEND  = os.environ.get("STORAGE_BACKEND", "s3")

GITHUB_API = "https://api.github.com/events"


def fetch_page(page: int) -> list[dict]:
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
    logger.info(
        "Page %d — X-RateLimit-Remaining: %s",
        page,
        resp.headers.get("X-RateLimit-Remaining", "?"),
    )
    return resp.json()


def bucket_by_date(events: list[dict]) -> dict[str, list[dict]]:
    """
    Group events by their own created_at date (not the run date).
    This keeps backfilled data in the right partition so the Lambda's
    regular runs and the backfill live in the same table without special-casing.
    """
    by_date: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        ts = e.get("created_at", "")
        date = ts[:10] if len(ts) >= 10 else "unknown"
        by_date[date].append(e)
    return dict(by_date)


def write_local(by_date: dict[str, list[dict]], run_id: str):
    base = Path("./data/raw")
    for date, events in by_date.items():
        out_dir = base / f"date={date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"backfill_{run_id}.json"
        # Write NDJSON (one event per line) — matches what Athena's SerDe expects
        with open(out_path, "w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        logger.info("Local: wrote %d events → %s", len(events), out_path)


def write_s3(by_date: dict[str, list[dict]], run_id: str):
    import boto3
    s3 = boto3.client("s3")
    for date, events in by_date.items():
        key = f"raw/date={date}/backfill_{run_id}.json"
        body = "\n".join(json.dumps(e, ensure_ascii=False) for e in events)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        logger.info("S3: wrote %d events → s3://%s/%s", len(events), S3_BUCKET, key)


def main():
    run_ts = datetime.now(timezone.utc).isoformat()
    run_id = run_ts.replace(":", "").replace("-", "")[:15]

    logger.info(
        "Backfill start — %d pages × %d events, backend=%s",
        BACKFILL_PAGES, PER_PAGE, STORAGE_BACKEND,
    )

    all_events: list[dict] = []
    seen_ids: set[str] = set()

    for page in range(1, BACKFILL_PAGES + 1):
        events = fetch_page(page)
        if not events:
            logger.info("Empty page %d — stopping", page)
            break
        new = [e for e in events if e.get("id") not in seen_ids]
        seen_ids.update(e.get("id", "") for e in new)
        all_events.extend(new)
        logger.info("Cumulative: %d unique events", len(all_events))
        time.sleep(0.5)

    if not all_events:
        logger.error("No events fetched — check GITHUB_TOKEN and network")
        return

    by_date = bucket_by_date(all_events)
    logger.info(
        "Bucketed into %d date partitions: %s",
        len(by_date),
        sorted(by_date.keys()),
    )

    if STORAGE_BACKEND == "local":
        write_local(by_date, run_id)
    else:
        if not S3_BUCKET:
            raise ValueError("S3_BUCKET must be set when STORAGE_BACKEND=s3")
        write_s3(by_date, run_id)

    logger.info(
        "Backfill complete — %d total events across %d dates",
        len(all_events),
        len(by_date),
    )
    logger.info(
        "Next step: in Athena run  MSCK REPAIR TABLE github_events_raw;  "
        "to pick up any new date= partitions"
    )


if __name__ == "__main__":
    main()
