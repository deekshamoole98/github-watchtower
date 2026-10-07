import os
import json
import boto3
import urllib.request
from datetime import datetime, timezone

sns = boto3.client("sns")
s3 = boto3.client("s3")

GITHUB_EVENTS_URL = "https://api.github.com/events"
REQUIRED_FIELDS = {"id", "type", "created_at"}
MIN_EVENT_COUNT = 10


def fetch_events():
    request = urllib.request.Request(
        GITHUB_EVENTS_URL,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "github-data-monitoring"
        }
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def validate_events(events):
    issues = []

    if not isinstance(events, list):
        return ["API response is not a list of events."]

    if len(events) < MIN_EVENT_COUNT:
        issues.append(
            f"Low event volume: received {len(events)} events; "
            f"expected at least {MIN_EVENT_COUNT}."
        )

    for index, event in enumerate(events):
        missing = REQUIRED_FIELDS - set(event.keys())
        if missing:
            issues.append(
                f"Event {index} is missing required fields: "
                f"{', '.join(sorted(missing))}"
            )

    return issues


def lambda_handler(event, context):
    bucket = os.environ["BUCKET"]
    topic_arn = os.environ["SNS_TOPIC_ARN"]

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    events = fetch_events()
    issues = validate_events(events)

    key = f"raw/github_events_{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(events, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    print(
        json.dumps({
            "timestamp": now.isoformat(),
            "event_count": len(events) if isinstance(events, list) else None,
            "s3_key": key,
            "validation_issues": issues
        })
    )

    if issues:
        sns.publish(
            TopicArn=topic_arn,
            Subject="GitHub Data Monitoring Alert",
            Message=(
                "GitHub data quality validation failed.\n\n"
                f"Time (UTC): {now.isoformat()}\n"
                f"Event count: {len(events) if isinstance(events, list) else 'N/A'}\n"
                f"Issues: {'; '.join(issues)}\n"
                f"Raw data: s3://{bucket}/{key}"
            )
        )

        return {
            "status": "alert_sent",
            "count": len(events) if isinstance(events, list) else None,
            "issues": issues,
            "s3": f"s3://{bucket}/{key}"
        }

    return {
        "status": "ok",
        "count": len(events),
        "s3": f"s3://{bucket}/{key}"
    }
