# GitHub Events Monitor

Pulls the public GitHub event stream, runs data quality checks, and lands raw JSON in S3 for Athena querying. Built on AWS (Lambda, S3, Athena) because the interesting question isn't "are events coming in" — it's "which repos and event types are trending, and is anything behaving strangely compared to yesterday."

**Stack:** Python · AWS Lambda · S3 · Athena · SNS · EventBridge

---

## Why

The GitHub public event stream is a live feed of pushes, creates, deletes, and more across every public repo. It's also a rolling window — events fall off after roughly 300 are added, so if you're not capturing continuously you lose the history. This pipeline captures that stream on a schedule, stores it date-partitioned in S3, and puts four Athena views on top so you can actually ask questions across days: which repos are most active, when is the stream busiest by hour, and is today's volume meaningfully different from yesterday's?

**Metrics it produces:**
- Event type distribution by day (PushEvent, CreateEvent, DeleteEvent, etc.)
- Most active repos and orgs by event volume
- Hourly activity pattern by UTC hour
- Day-over-day event volume change

---

## How it fits together

```
EventBridge (scheduled rule)
        │
        ▼
  Lambda (lambda_function.py)
        │
        ├── Fetches N pages of public events from GitHub API
        │
        ├── Data quality checks
        │     freshness · duplicate IDs · schema completeness rate · volume vs. yesterday
        │
        ├── S3  raw/date=YYYY-MM-DD/run_HHMMSS.json   (NDJSON, one event per line)
        │       raw/date=YYYY-MM-DD/manifest.json      (DQ summary)
        │
        └── SNS alert if DQ issues or day-over-day volume drops > threshold
                    │
                    ▼
        Athena external table (github_events_raw)
                    │
                    ▼
        Four metric views (vw_event_type_by_day, vw_top_repos,
                           vw_hourly_activity, vw_daily_volume_trend)
```

Events are stored as NDJSON (one JSON object per line) rather than a bare array because that's what the Athena SerDe expects. The manifest on each run captures the DQ summary and acts as a lightweight run log without needing CloudWatch for basic history.

---

## What's in here

| Path | Purpose |
|---|---|
| `lambda_function.py` | Lambda entrypoint: fetch, DQ check, store, alert |
| `ingestion/backfill.py` | One-off historical pull to seed date partitions before the schedule runs |
| `db/athena_schema.sql` | Athena external table DDL + partition repair command |
| `metrics/metrics.sql` | Four Athena views the dashboards are built on |
| `infra/iam/` | IAM trust and permission policies for the Lambda role |

---

## Running locally

```bash
pip install -r requirements.txt
cp .env.example .env    # add GITHUB_TOKEN, set STORAGE_BACKEND=local

# Run the Lambda logic in-process (no AWS needed)
python -c "import lambda_function; lambda_function.lambda_handler({}, {})"

# Seed some history before the schedule kicks in
python -m ingestion.backfill
```

With `STORAGE_BACKEND=local`, events write to `./data/raw/date=YYYY-MM-DD/`. That's the same partition structure S3 uses, so switching backends later is just a config change.

Do not commit AWS credentials or other secrets to this repository.

---

## Deploying

1. Create an S3 bucket
2. Create the Lambda (IAM policies in `infra/iam/`). Set `GITHUB_TOKEN`, `S3_BUCKET`, `SNS_TOPIC_ARN`, `STORAGE_BACKEND=s3`, and optionally `PAGES`, `PER_PAGE`, `STALENESS_HOURS`, `VOLUME_DROP_PCT`
3. Run the Lambda once and confirm `raw/date=YYYY-MM-DD/` appears in S3
4. In Athena: run `db/athena_schema.sql` → `MSCK REPAIR TABLE github_events_raw;` → `metrics/metrics.sql`
5. Optional: add an EventBridge schedule to trigger the Lambda automatically

---

## Data quality: four checks, one honest failure

**Freshness check** — the youngest event in the batch must be within `STALENESS_HOURS` of the run time. If the GitHub API goes stale or the token expires, this catches it immediately rather than letting the pipeline silently ingest old data.

**Duplicate ID detection** — the GitHub API paginates a live stream, so the same event can appear on page 1 and page 2 if new events arrive between requests. The Lambda deduplicates by event ID across pages before writing, and counts any remaining duplicates in the DQ summary.

**Schema completeness rate** — not just a boolean "does this field exist," but a per-field fraction across the whole batch. The `org` field is legitimately absent on personal-repo events (~60% of the stream), so a boolean would always fail. The rate surfaces which fields are actually sparse.

**Day-over-day volume comparison** — reads yesterday's manifest from S3 and alerts via SNS if today's count drops more than `VOLUME_DROP_PCT` (default 50%). A sudden drop usually means the token hit a rate limit or the EventBridge schedule misfired.

One thing that wasn't obvious from the API docs: the `id` field on a GitHub event is a string, not an integer, even though it looks like one. Treating it as an int causes silent truncation in Athena's BIGINT type. The schema uses STRING for event ID specifically because of this.

---

## Backfilling history

The live event stream is a rolling window of the last ~300 events — there's no historical endpoint for public events. `ingestion/backfill.py` pulls multiple pages at once and buckets them by each event's own `created_at` date rather than the run date, so the partitions reflect when the activity actually happened:

```bash
python -m ingestion.backfill
```

Then run `MSCK REPAIR TABLE github_events_raw;` in Athena to pick up the new partitions. The backfill and the Lambda write to the same `raw/date=YYYY-MM-DD/` structure, so Athena sees them as one table without any special-casing.

---

## Athena table

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS github_events_raw (
    id          STRING,
    type        STRING,
    actor       STRUCT<id: BIGINT, login: STRING, display_login: STRING,
                       gravatar_id: STRING, url: STRING, avatar_url: STRING>,
    repo        STRUCT<id: BIGINT, name: STRING, url: STRING>,
    payload     STRING,
    public      BOOLEAN,
    created_at  TIMESTAMP,
    org         STRUCT<id: BIGINT, login: STRING, gravatar_id: STRING,
                       url: STRING, avatar_url: STRING>
)
PARTITIONED BY (`date` STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'TRUE')
STORED AS TEXTFILE
LOCATION 's3://YOUR_BUCKET_NAME/raw/';
```

`payload` is stored as a raw STRING because its shape varies by event type — a PushEvent payload has `push_id`, `ref`, `head`, `before`; a CreateEvent has `ref_type`, `master_branch`, `description`. Parsing those in the view layer with `json_extract` is cleaner than trying to union the schemas at table definition time.

Full DDL in [`db/athena_schema.sql`](db/athena_schema.sql). Metric views in [`metrics/metrics.sql`](metrics/metrics.sql).

---

## What's next

The four metric views are most useful once a few weeks of scheduled ingestion have accumulated — the day-over-day trend view in particular needs actual days. Scheduling daily EventBridge ingestion is the obvious next step, along with a QuickSight dashboard pointed at the Athena views.

---

*Author: Deeksha Moole*
