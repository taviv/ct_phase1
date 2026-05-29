"""
transform_lambda.py
-------------------
Reads NDJSON files produced by the fetcher Lambda from S3 and upserts
Phase 1 clinical trial data into Aurora PostgreSQL (IAM authentication).

Actions (pass in event["action"]):
  "create_tables"  – Creates all 9 tables + indexes (idempotent, safe to re-run).
  "process_file"   – Process a single NDJSON file.
                     Requires: event["bucket"], event["key"]
  "process_run"    – Process every NDJSON file for one run.
                     Requires: event["bucket"], event["run_id"]
                     Optional: event["prefix"]  (default: CT_S3_PREFIX env var)
  "process_all"    – Process every NDJSON file under a prefix (full backfill).
                     Requires: event["bucket"]
                     Optional: event["prefix"]
  S3 event trigger – Lambda can also be wired directly to an S3 ObjectCreated
                     notification; the event["Records"] path is handled automatically.

Required environment variables
-------------------------------
  CT_DB_HOST    – Aurora writer endpoint
  CT_S3_BUCKET  – Source bucket (used as default when not in event)

Optional environment variables
-------------------------------
  CT_DB_NAME    – Database name  (default: postgres)
  CT_DB_USER    – DB username    (default: postgres)
  CT_DB_PORT    – DB port        (default: 5432)
  CT_S3_PREFIX  – Key prefix     (default: clinicaltrials/phase1/)

Packaging
---------
  pip install pg8000 -t package/
  cp transform_lambda.py package/
  cd package && zip -r ../transform_lambda.zip .

Lambda settings
---------------
  Timeout  : 900 s (15 min) — needed for full-run backfills
  Memory   : 512 MB
  IAM perms: s3:GetObject, s3:ListBucket on source bucket
             rds-db:connect on the Aurora cluster
"""

import json
import logging
import os
import re
from datetime import date, datetime, timezone

import boto3
import pg8000

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_HOST    = os.environ.get("CT_DB_HOST", "")
DB_NAME    = os.environ.get("CT_DB_NAME", "postgres")
DB_USER    = os.environ.get("CT_DB_USER", "postgres")
DB_PORT    = int(os.environ.get("CT_DB_PORT", "5432"))
S3_BUCKET  = os.environ.get("CT_S3_BUCKET", "")
S3_PREFIX  = os.environ.get("CT_S3_PREFIX", "clinicaltrials/phase1/").rstrip("/") + "/"
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# AWS clients (module-level for warm reuse)
# ---------------------------------------------------------------------------
_S3  = boto3.client("s3")
_RDS = boto3.client("rds", region_name=AWS_REGION)
_CONN = None   # reused across warm invocations


# ---------------------------------------------------------------------------
# DB connection (IAM token, refreshed when stale)
# ---------------------------------------------------------------------------
def get_conn():
    global _CONN
    if _CONN is not None:
        try:
            _CONN.run("SELECT 1")
            return _CONN
        except Exception:
            try:
                _CONN.close()
            except Exception:
                pass
            _CONN = None

    token = _RDS.generate_db_auth_token(
        DBHostname=DB_HOST,
        Port=DB_PORT,
        DBUsername=DB_USER,
        Region=AWS_REGION,
    )
    _CONN = pg8000.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=token,
        ssl_context=True,
    )
    _CONN.autocommit = False
    logger.info("New DB connection established to %s/%s", DB_HOST, DB_NAME)
    return _CONN


# ---------------------------------------------------------------------------
# Bulk insert helper (replaces psycopg2.extras.execute_values)
# ---------------------------------------------------------------------------
def _bulk_insert(conn, sql: str, rows: list) -> None:
    """
    Executes a multi-row INSERT with pg8000.
    sql must contain exactly one VALUES %s placeholder.
    rows is a list of tuples, all the same width.
    """
    if not rows:
        return
    width       = len(rows[0])
    row_ph      = "(" + ", ".join(["%s"] * width) + ")"
    values_sql  = ", ".join([row_ph] * len(rows))
    full_sql    = sql.replace("VALUES %s", f"VALUES {values_sql}")
    flat_args   = [v for row in rows for v in row]
    cur = conn.cursor()
    cur.execute(full_sql, flat_args)
    cur.close()


# ---------------------------------------------------------------------------
# SQL: table + index creation
# ---------------------------------------------------------------------------
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS studies (
    nct_id                      VARCHAR(20)  PRIMARY KEY,
    brief_title                 TEXT,
    official_title              TEXT,
    acronym                     TEXT,
    org_study_id                TEXT,
    organization_name           TEXT,
    organization_class          TEXT,
    overall_status              TEXT,
    why_stopped                 TEXT,
    status_verified_date        TEXT,
    has_expanded_access         BOOLEAN,
    start_date                  DATE,
    start_date_type             TEXT,
    primary_completion_date     DATE,
    primary_completion_date_type TEXT,
    completion_date             DATE,
    completion_date_type        TEXT,
    study_first_submit_date     DATE,
    study_first_post_date       DATE,
    last_update_submit_date     DATE,
    last_update_post_date       DATE,
    study_type                  TEXT,
    allocation                  TEXT,
    intervention_model          TEXT,
    primary_purpose             TEXT,
    masking                     TEXT,
    enrollment_count            INTEGER,
    enrollment_type             TEXT,
    brief_summary               TEXT,
    detailed_description        TEXT,
    eligibility_criteria        TEXT,
    healthy_volunteers          BOOLEAN,
    sex                         TEXT,
    minimum_age                 TEXT,
    maximum_age                 TEXT,
    lead_sponsor_name           TEXT,
    lead_sponsor_class          TEXT,
    responsible_party_type      TEXT,
    has_dmc                     BOOLEAN,
    is_fda_regulated_drug       BOOLEAN,
    is_fda_regulated_device     BOOLEAN,
    is_us_export                BOOLEAN,
    ipd_sharing                 TEXT,
    has_results                 BOOLEAN,
    created_at                  TIMESTAMPTZ  DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS study_phases (
    id      SERIAL      PRIMARY KEY,
    nct_id  VARCHAR(20) NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    phase   TEXT        NOT NULL,
    UNIQUE(nct_id, phase)
);

CREATE TABLE IF NOT EXISTS study_conditions (
    id              SERIAL      PRIMARY KEY,
    nct_id          VARCHAR(20) NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    condition_name  TEXT        NOT NULL,
    UNIQUE(nct_id, condition_name)
);

CREATE TABLE IF NOT EXISTS study_interventions (
    id                  SERIAL      PRIMARY KEY,
    nct_id              VARCHAR(20) NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    intervention_type   TEXT,
    name                TEXT,
    description         TEXT
);

CREATE TABLE IF NOT EXISTS study_outcomes (
    id           SERIAL      PRIMARY KEY,
    nct_id       VARCHAR(20) NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    outcome_type TEXT        NOT NULL,   -- primary | secondary | other
    measure      TEXT,
    description  TEXT,
    time_frame   TEXT
);

CREATE TABLE IF NOT EXISTS study_locations (
    id        SERIAL       PRIMARY KEY,
    nct_id    VARCHAR(20)  NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    facility  TEXT,
    status    TEXT,
    city      TEXT,
    state     TEXT,
    country   TEXT,
    zip       TEXT,
    latitude  NUMERIC(9,6),
    longitude NUMERIC(9,6)
);

CREATE TABLE IF NOT EXISTS study_sponsors (
    id           SERIAL      PRIMARY KEY,
    nct_id       VARCHAR(20) NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    sponsor_type TEXT        NOT NULL,   -- lead | collaborator
    name         TEXT,
    class        TEXT,
    UNIQUE(nct_id, sponsor_type, name)
);

CREATE TABLE IF NOT EXISTS condition_mesh_terms (
    id          SERIAL      PRIMARY KEY,
    nct_id      VARCHAR(20) NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    mesh_id     TEXT,
    term        TEXT,
    is_ancestor BOOLEAN     DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS intervention_mesh_terms (
    id          SERIAL      PRIMARY KEY,
    nct_id      VARCHAR(20) NOT NULL REFERENCES studies(nct_id) ON DELETE CASCADE,
    mesh_id     TEXT,
    term        TEXT,
    is_ancestor BOOLEAN     DEFAULT FALSE
);

-- Query-pattern indexes
CREATE INDEX IF NOT EXISTS idx_studies_overall_status      ON studies(overall_status);
CREATE INDEX IF NOT EXISTS idx_studies_start_date          ON studies(start_date);
CREATE INDEX IF NOT EXISTS idx_studies_lead_sponsor_class  ON studies(lead_sponsor_class);
CREATE INDEX IF NOT EXISTS idx_studies_is_fda_drug         ON studies(is_fda_regulated_drug);
CREATE INDEX IF NOT EXISTS idx_conditions_name             ON study_conditions(condition_name);
CREATE INDEX IF NOT EXISTS idx_locations_country           ON study_locations(country);
CREATE INDEX IF NOT EXISTS idx_cond_mesh_term              ON condition_mesh_terms(term);
CREATE INDEX IF NOT EXISTS idx_intv_mesh_term              ON intervention_mesh_terms(term);

-- Tracks which S3 files have been successfully loaded (prevents reprocessing)
CREATE TABLE IF NOT EXISTS processed_files (
    s3_key           TEXT        PRIMARY KEY,
    studies_upserted INTEGER,
    processed_at     TIMESTAMPTZ DEFAULT NOW()
);
"""

# ---------------------------------------------------------------------------
# Column list for the studies table (defines row tuple order)
# ---------------------------------------------------------------------------
STUDY_COLUMNS = [
    "nct_id", "brief_title", "official_title", "acronym",
    "org_study_id", "organization_name", "organization_class",
    "overall_status", "why_stopped", "status_verified_date",
    "has_expanded_access",
    "start_date", "start_date_type",
    "primary_completion_date", "primary_completion_date_type",
    "completion_date", "completion_date_type",
    "study_first_submit_date", "study_first_post_date",
    "last_update_submit_date", "last_update_post_date",
    "study_type", "allocation", "intervention_model",
    "primary_purpose", "masking",
    "enrollment_count", "enrollment_type",
    "brief_summary", "detailed_description", "eligibility_criteria",
    "healthy_volunteers", "sex", "minimum_age", "maximum_age",
    "lead_sponsor_name", "lead_sponsor_class", "responsible_party_type",
    "has_dmc", "is_fda_regulated_drug", "is_fda_regulated_device",
    "is_us_export", "ipd_sharing", "has_results",
]

_STUDY_UPSERT_SQL = (
    "INSERT INTO studies ({cols}) VALUES %s "
    "ON CONFLICT (nct_id) DO UPDATE SET {updates}, updated_at = NOW()"
).format(
    cols    = ", ".join(STUDY_COLUMNS),
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in STUDY_COLUMNS if c != "nct_id"
    ),
)


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------
def _parse_date(value) -> date | None:
    """
    Accepts 'YYYY-MM-DD', 'YYYY-MM' (→ first of month), or 'YYYY' (→ Jan 1).
    Returns None for blank / unparseable values.
    """
    if not value:
        return None
    s = str(value).strip()
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s, "%Y-%m-%d").date()
        if re.fullmatch(r"\d{4}-\d{2}", s):
            return datetime.strptime(s + "-01", "%Y-%m-%d").date()
        if re.fullmatch(r"\d{4}", s):
            return date(int(s), 1, 1)
    except ValueError:
        pass
    logger.debug("Could not parse date: %r", value)
    return None


def _get(obj, *keys, default=None):
    """Safe nested dict getter: _get(d, 'a', 'b', 'c') == d.get('a',{}).get('b',{}).get('c')."""
    for key in keys:
        if not isinstance(obj, dict):
            return default
        obj = obj.get(key)
        if obj is None:
            return default
    return obj if obj is not None else default


def extract_study_row(record: dict) -> tuple:
    ps  = record.get("protocolSection", {})
    id_ = ps.get("identificationModule", {})
    st  = ps.get("statusModule", {})
    sp  = ps.get("sponsorCollaboratorsModule", {})
    ov  = ps.get("oversightModule", {})
    de  = ps.get("descriptionModule", {})
    ds  = ps.get("designModule", {})
    el  = ps.get("eligibilityModule", {})
    ip  = ps.get("ipdSharingStatementModule", {})

    di  = ds.get("designInfo", {})
    en  = ds.get("enrollmentInfo", {})

    row = {
        "nct_id":                       id_.get("nctId"),
        "brief_title":                  id_.get("briefTitle"),
        "official_title":               id_.get("officialTitle"),
        "acronym":                      id_.get("acronym"),
        "org_study_id":                 _get(id_, "orgStudyIdInfo", "id"),
        "organization_name":            _get(id_, "organization", "fullName"),
        "organization_class":           _get(id_, "organization", "class"),
        "overall_status":               st.get("overallStatus"),
        "why_stopped":                  st.get("whyStopped"),
        "status_verified_date":         st.get("statusVerifiedDate"),
        "has_expanded_access":          _get(st, "expandedAccessInfo", "hasExpandedAccess"),
        "start_date":                   _parse_date(_get(st, "startDateStruct", "date")),
        "start_date_type":              _get(st, "startDateStruct", "type"),
        "primary_completion_date":      _parse_date(_get(st, "primaryCompletionDateStruct", "date")),
        "primary_completion_date_type": _get(st, "primaryCompletionDateStruct", "type"),
        "completion_date":              _parse_date(_get(st, "completionDateStruct", "date")),
        "completion_date_type":         _get(st, "completionDateStruct", "type"),
        "study_first_submit_date":      _parse_date(st.get("studyFirstSubmitDate")),
        "study_first_post_date":        _parse_date(_get(st, "studyFirstPostDateStruct", "date")),
        "last_update_submit_date":      _parse_date(st.get("lastUpdateSubmitDate")),
        "last_update_post_date":        _parse_date(_get(st, "lastUpdatePostDateStruct", "date")),
        "study_type":                   ds.get("studyType"),
        "allocation":                   di.get("allocation"),
        "intervention_model":           di.get("interventionModel"),
        "primary_purpose":              di.get("primaryPurpose"),
        "masking":                      _get(di, "maskingInfo", "masking"),
        "enrollment_count":             en.get("count"),
        "enrollment_type":              en.get("type"),
        "brief_summary":                (de.get("briefSummary") or "").strip() or None,
        "detailed_description":         (de.get("detailedDescription") or "").strip() or None,
        "eligibility_criteria":         (el.get("eligibilityCriteria") or "").strip() or None,
        "healthy_volunteers":           el.get("healthyVolunteers"),
        "sex":                          el.get("sex"),
        "minimum_age":                  el.get("minimumAge"),
        "maximum_age":                  el.get("maximumAge"),
        "lead_sponsor_name":            _get(sp, "leadSponsor", "name"),
        "lead_sponsor_class":           _get(sp, "leadSponsor", "class"),
        "responsible_party_type":       _get(sp, "responsibleParty", "type"),
        "has_dmc":                      ov.get("oversightHasDmc"),
        "is_fda_regulated_drug":        ov.get("isFdaRegulatedDrug"),
        "is_fda_regulated_device":      ov.get("isFdaRegulatedDevice"),
        "is_us_export":                 ov.get("isUsExport"),
        "ipd_sharing":                  ip.get("ipdSharing"),
        "has_results":                  record.get("hasResults", False),
    }
    return tuple(row[c] for c in STUDY_COLUMNS)


def extract_phases(nct_id: str, record: dict) -> list[tuple]:
    phases = _get(record, "protocolSection", "designModule", "phases") or []
    return [(nct_id, p) for p in phases if p]


def extract_conditions(nct_id: str, record: dict) -> list[tuple]:
    conds = _get(record, "protocolSection", "conditionsModule", "conditions") or []
    return [(nct_id, c) for c in conds if c]


def extract_interventions(nct_id: str, record: dict) -> list[tuple]:
    items = _get(record, "protocolSection", "armsInterventionsModule", "interventions") or []
    return [
        (nct_id, iv.get("type"), iv.get("name"), iv.get("description"))
        for iv in items
    ]


def extract_outcomes(nct_id: str, record: dict) -> list[tuple]:
    mod = _get(record, "protocolSection", "outcomesModule") or {}
    rows = []
    for otype, key in [("primary", "primaryOutcomes"),
                       ("secondary", "secondaryOutcomes"),
                       ("other", "otherOutcomes")]:
        for o in mod.get(key, []):
            rows.append((nct_id, otype, o.get("measure"), o.get("description"), o.get("timeFrame")))
    return rows


def extract_locations(nct_id: str, record: dict) -> list[tuple]:
    locs = _get(record, "protocolSection", "contactsLocationsModule", "locations") or []
    rows = []
    for loc in locs:
        geo = loc.get("geoPoint") or {}
        rows.append((
            nct_id,
            loc.get("facility"), loc.get("status"),
            loc.get("city"), loc.get("state"), loc.get("country"), loc.get("zip"),
            geo.get("lat"), geo.get("lon"),
        ))
    return rows


def extract_sponsors(nct_id: str, record: dict) -> list[tuple]:
    sp = _get(record, "protocolSection", "sponsorCollaboratorsModule") or {}
    rows = []
    lead = sp.get("leadSponsor") or {}
    if lead.get("name"):
        rows.append((nct_id, "lead", lead["name"], lead.get("class")))
    for collab in sp.get("collaborators", []):
        if collab.get("name"):
            rows.append((nct_id, "collaborator", collab["name"], collab.get("class")))
    return rows


def extract_condition_mesh(nct_id: str, record: dict) -> list[tuple]:
    browse = _get(record, "derivedSection", "conditionBrowseModule") or {}
    rows = []
    for m in browse.get("meshes", []):
        rows.append((nct_id, m.get("id"), m.get("term"), False))
    for a in browse.get("ancestors", []):
        rows.append((nct_id, a.get("id"), a.get("term"), True))
    return rows


def extract_intervention_mesh(nct_id: str, record: dict) -> list[tuple]:
    browse = _get(record, "derivedSection", "interventionBrowseModule") or {}
    rows = []
    for m in browse.get("meshes", []):
        rows.append((nct_id, m.get("id"), m.get("term"), False))
    for a in browse.get("ancestors", []):
        rows.append((nct_id, a.get("id"), a.get("term"), True))
    return rows


# ---------------------------------------------------------------------------
# Parse an NDJSON file from S3 → list of raw records
# ---------------------------------------------------------------------------
def read_ndjson_from_s3(bucket: str, key: str) -> list[dict]:
    logger.info("Reading s3://%s/%s", bucket, key)
    response = _S3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")

    records = []
    for line_num, line in enumerate(body.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            logger.warning("Skipping malformed JSON on line %d of %s: %s", line_num, key, exc)
    return records


# ---------------------------------------------------------------------------
# Write one batch of records to the DB
# ---------------------------------------------------------------------------
def write_records(conn, records: list[dict]) -> int:
    """
    Upsert all studies from `records` into the 9 tables.
    Returns the number of studies successfully processed.
    """
    if not records:
        return 0

    # --- Collect rows for every table in one pass ---
    study_rows        = []
    phase_rows        = []
    condition_rows    = []
    intervention_rows = []
    outcome_rows      = []
    location_rows     = []
    sponsor_rows      = []
    cond_mesh_rows    = []
    intv_mesh_rows    = []
    nct_ids           = []

    for rec in records:
        nct_id = _get(rec, "protocolSection", "identificationModule", "nctId")
        if not nct_id:
            logger.warning("Record missing nctId — skipping.")
            continue

        study_rows.append(extract_study_row(rec))
        phase_rows.extend(extract_phases(nct_id, rec))
        condition_rows.extend(extract_conditions(nct_id, rec))
        intervention_rows.extend(extract_interventions(nct_id, rec))
        outcome_rows.extend(extract_outcomes(nct_id, rec))
        location_rows.extend(extract_locations(nct_id, rec))
        sponsor_rows.extend(extract_sponsors(nct_id, rec))
        cond_mesh_rows.extend(extract_condition_mesh(nct_id, rec))
        intv_mesh_rows.extend(extract_intervention_mesh(nct_id, rec))
        nct_ids.append(nct_id)

    if not study_rows:
        return 0

    try:
        # 1. Upsert studies (must come first — child tables FK to this)
        _bulk_insert(conn, _STUDY_UPSERT_SQL, study_rows)

        # 2. Clear child rows for these nct_ids then bulk-insert fresh data.
        id_list = list(set(nct_ids))
        cur = conn.cursor()
        for table in (
            "study_phases", "study_conditions", "study_interventions",
            "study_outcomes", "study_locations", "study_sponsors",
            "condition_mesh_terms", "intervention_mesh_terms",
        ):
            cur.execute(f"DELETE FROM {table} WHERE nct_id = ANY(%s)", (id_list,))
        cur.close()

        # 3. Bulk insert child tables (skip if no rows)
        _bulk_insert(conn,
            "INSERT INTO study_phases (nct_id, phase) VALUES %s ON CONFLICT DO NOTHING",
            phase_rows)
        _bulk_insert(conn,
            "INSERT INTO study_conditions (nct_id, condition_name) VALUES %s ON CONFLICT DO NOTHING",
            condition_rows)
        _bulk_insert(conn,
            "INSERT INTO study_interventions (nct_id, intervention_type, name, description) VALUES %s",
            intervention_rows)
        _bulk_insert(conn,
            "INSERT INTO study_outcomes (nct_id, outcome_type, measure, description, time_frame) VALUES %s",
            outcome_rows)
        _bulk_insert(conn,
            "INSERT INTO study_locations (nct_id, facility, status, city, state, country, zip, latitude, longitude) VALUES %s",
            location_rows)
        _bulk_insert(conn,
            "INSERT INTO study_sponsors (nct_id, sponsor_type, name, class) VALUES %s ON CONFLICT DO NOTHING",
            sponsor_rows)
        _bulk_insert(conn,
            "INSERT INTO condition_mesh_terms (nct_id, mesh_id, term, is_ancestor) VALUES %s",
            cond_mesh_rows)
        _bulk_insert(conn,
            "INSERT INTO intervention_mesh_terms (nct_id, mesh_id, term, is_ancestor) VALUES %s",
            intv_mesh_rows)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    return len(study_rows)


# ---------------------------------------------------------------------------
# Action handlers
# ---------------------------------------------------------------------------
def handle_create_tables() -> dict:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(CREATE_TABLES_SQL)
    cur.close()
    conn.commit()
    logger.info("All tables and indexes created (or already existed).")
    return {"status": "ok", "message": "Tables created successfully."}


def _is_already_processed(conn, key: str) -> bool:
    """Returns True if this S3 key has already been successfully loaded."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_files WHERE s3_key = %s", (key,))
    result = cur.fetchone()
    cur.close()
    return result is not None


def _mark_processed(conn, key: str, count: int) -> None:
    """Record that this S3 key has been successfully loaded."""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO processed_files (s3_key, studies_upserted) VALUES (%s, %s) "
        "ON CONFLICT (s3_key) DO UPDATE SET studies_upserted = EXCLUDED.studies_upserted, "
        "processed_at = NOW()",
        (key, count),
    )
    cur.close()
    conn.commit()


def handle_process_file(bucket: str, key: str, force: bool = False) -> dict:
    if not key.endswith(".ndjson"):
        return {"status": "skipped", "reason": "Not an NDJSON file", "key": key}

    conn = get_conn()

    # Skip files already successfully loaded unless force=True
    if not force and _is_already_processed(conn, key):
        logger.info("Skipping already-processed file: %s", key)
        return {"status": "skipped", "reason": "Already processed", "key": key}

    records = read_ndjson_from_s3(bucket, key)
    if not records:
        logger.warning("No records found in s3://%s/%s — skipping.", bucket, key)
        return {"status": "skipped", "reason": "Empty file", "key": key}

    count = write_records(conn, records)
    _mark_processed(conn, key, count)
    logger.info("s3://%s/%s → %d studies upserted.", bucket, key, count)
    return {"status": "ok", "key": key, "studies_upserted": count}


def handle_process_run(bucket: str, run_id: str, prefix: str = None, force: bool = False) -> dict:
    prefix = (prefix or S3_PREFIX).rstrip("/") + "/"
    run_prefix = f"{prefix}{run_id}/"

    paginator = _S3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=run_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".ndjson"):
                keys.append(obj["Key"])
    keys.sort()

    if not keys:
        raise RuntimeError(f"No NDJSON files found under s3://{bucket}/{run_prefix}")

    logger.info("Processing run %s — %d files found.", run_id, len(keys))
    total = 0
    skipped = 0
    for key in keys:
        result = handle_process_file(bucket, key, force=force)
        total   += result.get("studies_upserted", 0)
        skipped += 1 if result.get("reason") == "Already processed" else 0

    logger.info("Run %s done — %d loaded, %d skipped (already processed).", run_id, total, skipped)
    return {"status": "ok", "run_id": run_id, "files_processed": len(keys) - skipped,
            "files_skipped": skipped, "studies_upserted": total}


def handle_process_all(bucket: str, prefix: str = None, force: bool = False) -> dict:
    prefix = (prefix or S3_PREFIX).rstrip("/") + "/"

    paginator = _S3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".ndjson"):
                keys.append(obj["Key"])
    keys.sort()

    if not keys:
        raise RuntimeError(f"No NDJSON files found under s3://{bucket}/{prefix}")

    logger.info("Full backfill — %d NDJSON files found under %s.", len(keys), prefix)
    total = 0
    skipped = 0
    for key in keys:
        result = handle_process_file(bucket, key, force=force)
        total   += result.get("studies_upserted", 0)
        skipped += 1 if result.get("reason") == "Already processed" else 0

    logger.info("process_all done — %d loaded, %d skipped (already processed).", total, skipped)
    return {"status": "ok", "files_processed": len(keys) - skipped,
            "files_skipped": skipped, "studies_upserted": total}


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    """
    Routes based on event["action"] or S3 trigger event["Records"].

    Direct invocation examples
    --------------------------
    Create tables (run once after deploy):
        {"action": "create_tables"}

    Process a single file:
        {"action": "process_file", "bucket": "my-bucket", "key": "clinicaltrials/phase1/20260101T000000Z/page_0001.ndjson"}

    Process all files for a run:
        {"action": "process_run", "bucket": "my-bucket", "run_id": "20260101T000000Z"}

    Full backfill — skips already-processed files automatically:
        {"action": "process_all", "bucket": "my-bucket"}

    Force reprocess everything even if already loaded:
        {"action": "process_all", "bucket": "my-bucket", "force": true}
    """

    # --- S3 event trigger (ObjectCreated notification) ---
    if "Records" in event:
        results = []
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key    = record["s3"]["object"]["key"]
            logger.info("S3 trigger: s3://%s/%s", bucket, key)
            results.append(handle_process_file(bucket, key))
        return {"status": "ok", "results": results}

    # --- Direct invocation ---
    action = event.get("action", "")
    bucket = event.get("bucket") or S3_BUCKET
    force  = bool(event.get("force", False))
    logger.info("Invoked with action=%r force=%s", action, force)

    if action == "create_tables":
        return handle_create_tables()

    if not bucket:
        raise ValueError("'bucket' is required — set CT_S3_BUCKET env var or pass it in the event.")

    if action == "process_file":
        key = event.get("key")
        if not key:
            raise ValueError("'key' is required for action=process_file")
        return handle_process_file(bucket, key, force=force)

    if action == "process_run":
        run_id = event.get("run_id")
        if not run_id:
            raise ValueError("'run_id' is required for action=process_run")
        return handle_process_run(bucket, run_id, prefix=event.get("prefix"), force=force)

    if action == "process_all":
        return handle_process_all(bucket, prefix=event.get("prefix"), force=force)

    raise ValueError(
        f"Unknown action: {action!r}. "
        "Expected: create_tables | process_file | process_run | process_all"
    )
