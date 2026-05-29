"""
lambda_handler.py
-----------------
AWS Lambda handler for fetching ClinicalTrials.gov Phase 1 data into S3,
orchestrated by AWS Step Functions.

Architecture
------------
EventBridge (scheduled) → Step Functions execution
Step Functions calls this Lambda three ways:

  1. {"action": "start"}
     Initialises a run: generates a run_id, builds the query filter
     (phase + optional year), returns initial state.

  2. {"action": "fetch_page", "run_id": "...", "page_token": "...|null",
       "page_num": N, "total_records": N, "bucket": "...", "prefix": "...",
       "query_filter": "..."}
     Fetches one page from the API, uploads it as NDJSON to S3 only when
     the page contains records, returns updated state (including next
     page_token or null when done).

  3. {"action": "finalize", "run_id": "...", "page_num": N,
       "total_records": N, "bucket": "...", "prefix": "..."}
     Lists the uploaded files from S3 and writes a manifest.json.
     Raises RuntimeError if the run produced zero data files so Step
     Functions marks the execution as FAILED instead of silently succeeding.

Required environment variables
-------------------------------
  CT_S3_BUCKET   - target S3 bucket name
  CT_S3_PREFIX   - key prefix (default: clinicaltrials/phase1/)

Optional environment variables
-------------------------------
  CT_START_YEAR  - earliest study start year to include, e.g. "2020".
                   Appends AREA[StartDate]RANGE[YYYY-01-01,MAX] to the
                   API query so only studies that started on or after
                   January 1 of that year are returned.
                   Omit (or leave blank) to fetch all years.
  CT_PAGE_SIZE   - records per page, 1-1000 (default: 1000)
  CT_MAX_RETRIES - HTTP retry attempts (default: 5)
  CT_BACKOFF     - retry back-off base in seconds (default: 2.0)

Lambda settings
---------------
  Timeout  : 60 seconds is plenty for one page; 120 s recommended for safety.
  Memory   : 256 MB
  Runtime  : python3.12
  IAM perms: s3:PutObject, s3:ListBucket on your target bucket.
  boto3 is included in the Lambda runtime — only `requests` needs packaging.

Packaging (zip deployment)
--------------------------
  pip install requests -t package/
  cp lambda_handler.py package/
  cd package && zip -r ../clinicaltrials_lambda.zip .
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from io import BytesIO

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration (from environment variables)
# ---------------------------------------------------------------------------
API_BASE_URL         = "https://clinicaltrials.gov/api/v2/studies"
PHASE_FILTER         = "AREA[Phase]Phase1"
REQUEST_TIMEOUT      = 25             # seconds — stay well under Lambda timeout
MIN_REQUEST_INTERVAL = 0.5            # seconds between API calls (rate-limit courtesy)

S3_BUCKET   = os.environ.get("CT_S3_BUCKET", "")
S3_PREFIX   = os.environ.get("CT_S3_PREFIX", "clinicaltrials/phase1/").rstrip("/") + "/"
START_YEAR  = os.environ.get("CT_START_YEAR", "").strip()   # e.g. "2020"; "" = all years
PAGE_SIZE   = int(os.environ.get("CT_PAGE_SIZE",   "1000"))
MAX_RETRIES = int(os.environ.get("CT_MAX_RETRIES", "5"))
BACKOFF     = float(os.environ.get("CT_BACKOFF",   "2.0"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
def _build_session() -> requests.Session:
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"Accept": "application/json"})
    return session

# Module-level session — reused across warm Lambda invocations
_SESSION = _build_session()


# ---------------------------------------------------------------------------
# S3 client (module-level for connection reuse)
# ---------------------------------------------------------------------------
_S3 = boto3.client("s3")


# ---------------------------------------------------------------------------
# Helper: build the Essie query filter string
# ---------------------------------------------------------------------------
def _build_query_filter(start_year: str = "") -> str:
    """
    Combine the mandatory phase filter with an optional year lower-bound.

    Parameters
    ----------
    start_year : str
        Four-digit year string (e.g. "2020").  If empty or invalid, the year
        clause is omitted and all years are returned.

    Returns
    -------
    str
        An Essie expression ready for the ``query.term`` API parameter, e.g.:
        ``"AREA[Phase]Phase1 AND AREA[StartDate]RANGE[2020-01-01,MAX]"``
    """
    clauses = [PHASE_FILTER]

    if start_year:
        try:
            year = int(start_year)
            if year < 1900 or year > 2100:
                raise ValueError("Year out of plausible range")
            clauses.append(f"AREA[StartDate]RANGE[{year}-01-01,MAX]")
            logger.info("Year filter active: studies with StartDate >= %d-01-01", year)
        except ValueError:
            logger.warning(
                "CT_START_YEAR=%r is not a valid year — year filter will be skipped.",
                start_year,
            )

    return " AND ".join(clauses)


# ---------------------------------------------------------------------------
# Action: start
# ---------------------------------------------------------------------------
def handle_start(event: dict) -> dict:
    """
    Initialise a new extraction run.
    Returns the initial Step Functions state dict.
    """
    bucket = event.get("bucket") or S3_BUCKET
    prefix = event.get("prefix") or S3_PREFIX

    if not bucket:
        raise ValueError(
            "S3 bucket not provided. Set CT_S3_BUCKET env var or pass 'bucket' in event."
        )

    # Allow per-invocation year override (useful for backfills)
    start_year   = str(event.get("start_year", START_YEAR)).strip()
    query_filter = _build_query_filter(start_year)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger.info(
        "Starting run %s → s3://%s/%s | filter: %s",
        run_id, bucket, prefix, query_filter,
    )

    return {
        "action":        "fetch_page",
        "run_id":        run_id,
        "bucket":        bucket,
        "prefix":        prefix,
        "query_filter":  query_filter,   # threaded through every fetch_page call
        "start_year":    start_year,
        "page_token":    None,           # None = first page
        "page_num":      1,
        "total_records": 0,
        "done":          False,
    }


# ---------------------------------------------------------------------------
# Action: fetch_page
# ---------------------------------------------------------------------------
def handle_fetch_page(event: dict) -> dict:
    """
    Fetch one page from the ClinicalTrials.gov v2 API and upload to S3.
    Returns updated state for Step Functions.

    Empty pages (zero studies) are skipped — no file is written to S3 — and
    the run is marked done so no blank NDJSON files ever land in the bucket.
    """
    run_id        = event["run_id"]
    bucket        = event["bucket"]
    prefix        = event["prefix"]
    query_filter  = event.get("query_filter") or _build_query_filter(START_YEAR)
    page_token    = event.get("page_token")   # None on first call
    page_num      = int(event["page_num"])
    total_records = int(event["total_records"])

    if not bucket:
        raise ValueError("'bucket' missing from event — cannot upload to S3.")

    # --- Build request params ---
    params: dict = {
        "format":     "json",
        "query.term": query_filter,
        "pageSize":   PAGE_SIZE,
    }
    if page_token:
        params["pageToken"] = page_token

    logger.info(
        "Fetching page %d (pageToken=%s) | filter: %s",
        page_num, page_token or "<first>", query_filter,
    )

    # --- Call API ---
    resp = _SESSION.get(API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

    if resp.status_code != 200:
        logger.error("API error %s: %s", resp.status_code, resp.text[:500])
        resp.raise_for_status()

    data = resp.json()

    if page_num == 1:
        total_count = data.get("totalCount")
        if isinstance(total_count, int):
            logger.info("API reports %s total matching studies.", f"{total_count:,}")
        else:
            logger.info("API totalCount: %s", total_count)

    studies    = data.get("studies", [])
    next_token = data.get("nextPageToken")   # absent/null on last page

    # A page is "done" when the API omits nextPageToken.
    # We do NOT treat an empty studies list as done by itself — log a warning
    # instead so the anomaly is visible, then honour the API's own signal.
    if not studies and next_token:
        logger.warning(
            "Page %d returned 0 studies but API provided a nextPageToken. "
            "Continuing pagination — this may indicate a transient API issue.",
            page_num,
        )

    done = not bool(next_token)

    total_records += len(studies)
    logger.info(
        "Page %d: %d records fetched | running total: %d | done: %s",
        page_num, len(studies), total_records, done,
    )

    # --- Upload to S3 (only when there is actual data) ---
    if studies:
        ndjson_bytes = (
            "\n".join(json.dumps(r, ensure_ascii=False) for r in studies)
            .encode("utf-8")
        )
        s3_key = f"{prefix}{run_id}/page_{page_num:04d}.ndjson"

        try:
            _S3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=BytesIO(ndjson_bytes),
                ContentType="application/x-ndjson",
            )
            logger.info(
                "Uploaded s3://%s/%s (%.1f KB)", bucket, s3_key, len(ndjson_bytes) / 1024
            )
        except (BotoCoreError, ClientError) as exc:
            logger.error("S3 upload failed: %s", exc)
            raise
    else:
        logger.info("Page %d: 0 studies — skipping S3 upload.", page_num)

    # Rate-limit courtesy delay before next invocation
    time.sleep(MIN_REQUEST_INTERVAL)

    # --- Return state to Step Functions ---
    return {
        "action":        "finalize" if done else "fetch_page",
        "run_id":        run_id,
        "bucket":        bucket,
        "prefix":        prefix,
        "query_filter":  query_filter,
        "start_year":    event.get("start_year", ""),
        "page_token":    next_token,     # None signals last page
        "page_num":      page_num + 1,   # will equal total_pages+1 when finalize runs
        "total_records": total_records,
        "done":          done,
    }


# ---------------------------------------------------------------------------
# Action: finalize
# ---------------------------------------------------------------------------
def handle_finalize(event: dict) -> dict:
    """
    List all uploaded NDJSON files for this run and write a manifest.json.

    Raises RuntimeError if the run produced zero data files — this causes
    Step Functions to mark the execution FAILED rather than silently
    completing with an empty manifest.
    """
    run_id        = event["run_id"]
    bucket        = event["bucket"]
    prefix        = event["prefix"]
    query_filter  = event.get("query_filter", "")
    start_year    = event.get("start_year", "")
    total_records = int(event["total_records"])
    # page_num was incremented after the last fetch, so page_num - 1 == total pages
    total_pages   = int(event["page_num"]) - 1

    if not bucket:
        raise ValueError("'bucket' missing from event — cannot finalise run.")

    run_prefix = f"{prefix}{run_id}/"

    # List all NDJSON objects written during this run
    paginator = _S3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=run_prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".ndjson"):
                keys.append(k)
    keys.sort()

    # --- Guard: abort if no data was captured ---
    if not keys:
        msg = (
            f"Run {run_id} produced 0 data files "
            f"(total_records={total_records}, total_pages={total_pages}). "
            "No manifest will be written. Check the query filter and API response."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    if total_records == 0:
        # Files exist but record count is 0 — shouldn't happen, but log clearly.
        logger.warning(
            "Run %s: %d NDJSON files found in S3 but total_records counter is 0. "
            "The counter may have been corrupted in state. Writing manifest anyway.",
            run_id, len(keys),
        )

    manifest = {
        "run_timestamp":         run_id,
        "phase_filter":          PHASE_FILTER,
        "query_filter":          query_filter,
        "start_year_filter":     start_year or None,
        "api_base_url":          API_BASE_URL,
        "total_pages":           total_pages,
        "total_records_fetched": total_records,
        "s3_bucket":             bucket,
        "s3_prefix":             prefix,
        "files":                 keys,
        "completed_at":          datetime.now(timezone.utc).isoformat(),
    }

    manifest_key   = f"{run_prefix}manifest.json"
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")

    try:
        _S3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=BytesIO(manifest_bytes),
            ContentType="application/json",
        )
        logger.info("Manifest uploaded → s3://%s/%s", bucket, manifest_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("Manifest upload failed: %s", exc)
        raise

    logger.info(
        "Run %s complete — %d records, %d pages, %d files.",
        run_id, total_records, total_pages, len(keys),
    )

    return {
        "status":          "completed",
        "run_id":          run_id,
        "total_records":   total_records,
        "total_pages":     total_pages,
        "manifest_s3_key": manifest_key,
    }


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    """
    Routes to one of three actions based on event["action"]:
      "start"      → initialise run, return initial state
      "fetch_page" → fetch one page, upload NDJSON, return next state
      "finalize"   → list uploaded files, write manifest, return summary

    Step Functions invokes this Lambda for each state transition.
    EventBridge (or manual invocation) triggers with {"action": "start"}.

    To backfill a specific year range, pass {"action": "start", "start_year": "2022"}.
    """
    action = event.get("action", "start")
    logger.info("Lambda invoked with action=%s", action)

    if action == "start":
        return handle_start(event)
    elif action == "fetch_page":
        return handle_fetch_page(event)
    elif action == "finalize":
        return handle_finalize(event)
    else:
        raise ValueError(
            f"Unknown action: {action!r}. Expected: start, fetch_page, or finalize."
        )
