"""
query_lambda.py (v2)
--------------------
Read-only query Lambda for the ClinicalTrials Phase 1 dashboards.
Exposed via a Lambda Function URL (HTTPS, no API Gateway needed).

New in v2
---------
- 5 parameterised duration queries (duration_summary, duration_histogram,
  duration_by_year, duration_by_sponsor, duration_studies) that accept a
  ``filters`` object in the POST body.
- countries_completed static query (for the country dropdown).
- Filters supported: year (int), country (str), multicountry (bool),
  healthy_volunteers (bool).

Deploy steps
------------
1. Zip with pg8000 layer.
2. Handler: query_lambda.lambda_handler
3. Env vars: CT_DB_HOST  CT_DB_NAME  CT_DB_USER  CT_DB_PORT
4. IAM: rds-db:connect on the cluster resource.
5. Function URL -> Auth: NONE -> CORS off (Lambda returns its own headers).
"""

import json
import logging
import os

import boto3
import pg8000

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_HOST    = os.environ.get("CT_DB_HOST", "")
DB_NAME    = os.environ.get("CT_DB_NAME", "postgres")
DB_USER    = os.environ.get("CT_DB_USER", "postgres")
DB_PORT    = int(os.environ.get("CT_DB_PORT", "5432"))
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_RDS  = boto3.client("rds", region_name=AWS_REGION)
_CONN = None


# ---------------------------------------------------------------------------
# DB connection (IAM auth, warm reuse)
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
        DBHostname=DB_HOST, Port=DB_PORT,
        DBUsername=DB_USER, Region=AWS_REGION,
    )
    _CONN = pg8000.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=token, ssl_context=True,
    )
    _CONN.autocommit = True
    return _CONN


# ---------------------------------------------------------------------------
# Static predefined queries (original dashboard -- unchanged)
# ---------------------------------------------------------------------------
QUERIES = {

    "summary_stats": """
        SELECT
            COUNT(*)                                                        AS total_studies,
            SUM(CASE WHEN overall_status = 'COMPLETED'            THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN overall_status = 'RECRUITING'           THEN 1 ELSE 0 END) AS recruiting,
            SUM(CASE WHEN overall_status = 'ACTIVE_NOT_RECRUITING' THEN 1 ELSE 0 END) AS active_not_recruiting,
            SUM(CASE WHEN overall_status = 'NOT_YET_RECRUITING'   THEN 1 ELSE 0 END) AS not_yet_recruiting,
            SUM(CASE WHEN overall_status = 'TERMINATED'           THEN 1 ELSE 0 END) AS terminated,
            SUM(CASE WHEN is_fda_regulated_drug  = TRUE           THEN 1 ELSE 0 END) AS fda_regulated_drug,
            SUM(CASE WHEN has_results            = TRUE           THEN 1 ELSE 0 END) AS has_results,
            SUM(CASE WHEN healthy_volunteers     = TRUE           THEN 1 ELSE 0 END) AS healthy_volunteers
        FROM studies
    """,

    "status_breakdown": """
        SELECT overall_status AS label, COUNT(*) AS value
        FROM studies
        GROUP BY overall_status
        ORDER BY value DESC
    """,

    "studies_by_year": """
        SELECT EXTRACT(YEAR FROM start_date)::int AS year, COUNT(*) AS count
        FROM studies
        WHERE start_date IS NOT NULL
          AND EXTRACT(YEAR FROM start_date) BETWEEN 1990 AND 2030
        GROUP BY year
        ORDER BY year
    """,

    "sponsor_class": """
        SELECT
            COALESCE(lead_sponsor_class, 'UNKNOWN') AS label,
            COUNT(*) AS value
        FROM studies
        GROUP BY lead_sponsor_class
        ORDER BY value DESC
    """,

    "top_conditions": """
        SELECT condition_name AS label, COUNT(*) AS value
        FROM study_conditions
        GROUP BY condition_name
        ORDER BY value DESC
        LIMIT 15
    """,

    "top_countries": """
        SELECT country AS label, COUNT(DISTINCT nct_id) AS value
        FROM study_locations
        WHERE country IS NOT NULL
        GROUP BY country
        ORDER BY value DESC
        LIMIT 15
    """,

    "fda_regulated": """
        SELECT
            CASE WHEN is_fda_regulated_drug THEN 'FDA Regulated' ELSE 'Not FDA Regulated' END AS label,
            COUNT(*) AS value
        FROM studies
        WHERE is_fda_regulated_drug IS NOT NULL
        GROUP BY is_fda_regulated_drug
        ORDER BY value DESC
    """,

    "intervention_types": """
        SELECT
            COALESCE(intervention_type, 'UNKNOWN') AS label,
            COUNT(*) AS value
        FROM study_interventions
        GROUP BY intervention_type
        ORDER BY value DESC
    """,

    "recent_studies": """
        SELECT
            nct_id,
            brief_title,
            overall_status,
            TO_CHAR(start_date, 'YYYY-MM-DD')       AS start_date,
            lead_sponsor_name,
            lead_sponsor_class,
            enrollment_count
        FROM studies
        WHERE start_date IS NOT NULL
        ORDER BY start_date DESC
        LIMIT 25
    """,

    "enrollment_distribution": """
        SELECT
            CASE
                WHEN enrollment_count <  10   THEN '< 10'
                WHEN enrollment_count <  50   THEN '10-49'
                WHEN enrollment_count < 100   THEN '50-99'
                WHEN enrollment_count < 250   THEN '100-249'
                WHEN enrollment_count < 500   THEN '250-499'
                WHEN enrollment_count < 1000  THEN '500-999'
                ELSE '1000+'
            END AS label,
            COUNT(*) AS value
        FROM studies
        WHERE enrollment_count IS NOT NULL
        GROUP BY label
        ORDER BY MIN(enrollment_count)
    """,

    # Populates the country dropdown on the duration dashboard
    "countries_completed": """
        SELECT DISTINCT l.country AS label
        FROM study_locations l
        JOIN studies s ON s.nct_id = l.nct_id
        WHERE s.overall_status = 'COMPLETED'
          AND l.country IS NOT NULL
        ORDER BY l.country
    """,
}


# ---------------------------------------------------------------------------
# Duration query helpers -- parameterised by filters dict
# ---------------------------------------------------------------------------

def build_filter_clause(filters: dict):
    """
    Build a SQL WHERE clause for completed-studies duration queries.

    Accepted filter keys
    --------------------
    year               : int  -- filter by EXTRACT(YEAR FROM completion_date)
    healthy_volunteers : bool -- True = HV only, False = non-HV only
    multicountry       : bool -- True = studies with locations in >1 country
    country            : str  -- studies with a location in this country
                                 (ignored when multicountry=True)

    Returns (where_str, args_list) ready for pg8000 parameterised execute.
    """
    clauses = [
        "s.overall_status = 'COMPLETED'",
        "s.start_end IS NOT NULL",
        "s.start_end >= 0",
    ]
    args = []

    year = filters.get("year")
    if year:
        clauses.append("EXTRACT(YEAR FROM s.completion_date)::int = %s")
        args.append(int(year))

    hv = filters.get("healthy_volunteers")
    if hv is not None:
        clauses.append("s.healthy_volunteers = %s")
        args.append(bool(hv))

    multicountry = filters.get("multicountry")
    country      = filters.get("country")

    if multicountry:
        clauses.append(
            "(SELECT COUNT(DISTINCT l2.country)"
            " FROM study_locations l2 WHERE l2.nct_id = s.nct_id) > 1"
        )
    elif country:
        clauses.append(
            "EXISTS (SELECT 1 FROM study_locations l2"
            "        WHERE l2.nct_id = s.nct_id AND l2.country = %s)"
        )
        args.append(country)

    return " AND ".join(clauses), args


def _duration_summary(filters: dict):
    where, args = build_filter_clause(filters)
    sql = f"""
        SELECT
            COUNT(*)                                                AS total_studies,
            ROUND(AVG(start_end))                                  AS avg_duration_days,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY start_end) AS median_duration_days,
            MIN(start_end)                                         AS min_duration_days,
            MAX(start_end)                                         AS max_duration_days
        FROM studies s
        WHERE {where}
    """
    return sql, args


def _duration_histogram(filters: dict):
    where, args = build_filter_clause(filters)
    sql = f"""
        SELECT
            CASE
                WHEN start_end <  180  THEN '< 6 mo'
                WHEN start_end <  365  THEN '6-12 mo'
                WHEN start_end <  730  THEN '1-2 yr'
                WHEN start_end <  1095 THEN '2-3 yr'
                WHEN start_end <  1825 THEN '3-5 yr'
                ELSE '5+ yr'
            END                  AS bucket,
            COUNT(*)             AS count,
            MIN(start_end)       AS sort_key
        FROM studies s
        WHERE {where}
        GROUP BY bucket
        ORDER BY MIN(start_end)
    """
    return sql, args


def _duration_by_year(filters: dict):
    where, args = build_filter_clause(filters)
    sql = f"""
        SELECT
            EXTRACT(YEAR FROM s.completion_date)::int AS year,
            COUNT(*)                                   AS studies,
            ROUND(AVG(s.start_end))                    AS avg_duration_days
        FROM studies s
        WHERE {where}
          AND s.completion_date IS NOT NULL
          AND EXTRACT(YEAR FROM s.completion_date) BETWEEN 1990 AND 2030
        GROUP BY year
        ORDER BY year
    """
    return sql, args


def _duration_by_sponsor(filters: dict):
    where, args = build_filter_clause(filters)
    sql = f"""
        SELECT
            COALESCE(s.lead_sponsor_class, 'UNKNOWN') AS sponsor_class,
            COUNT(*)                                   AS studies,
            ROUND(AVG(s.start_end))                    AS avg_duration_days
        FROM studies s
        WHERE {where}
        GROUP BY sponsor_class
        ORDER BY avg_duration_days DESC
    """
    return sql, args


def _duration_studies(filters: dict):
    where, args = build_filter_clause(filters)
    sql = f"""
        SELECT
            s.nct_id,
            s.brief_title,
            TO_CHAR(s.start_date,      'YYYY-MM-DD') AS start_date,
            TO_CHAR(s.completion_date, 'YYYY-MM-DD') AS completion_date,
            s.start_end                               AS duration_days,
            s.lead_sponsor_name,
            s.lead_sponsor_class,
            s.enrollment_count,
            s.healthy_volunteers,
            (SELECT COUNT(DISTINCT l2.country)
             FROM study_locations l2
             WHERE l2.nct_id = s.nct_id)             AS num_countries
        FROM studies s
        WHERE {where}
        ORDER BY s.start_end DESC
        LIMIT 100
    """
    return sql, args


DURATION_QUERIES = {
    "duration_summary":    _duration_summary,
    "duration_histogram":  _duration_histogram,
    "duration_by_year":    _duration_by_year,
    "duration_by_sponsor": _duration_by_sponsor,
    "duration_studies":    _duration_studies,
}

# Queries that return a single flat dict instead of column/rows
_SINGLE_ROW_QUERIES = {"summary_stats", "duration_summary"}


# ---------------------------------------------------------------------------
# CORS headers
# ---------------------------------------------------------------------------
CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type":                 "application/json",
}


def _response(status: int, body) -> dict:
    return {
        "statusCode": status,
        "headers":    CORS_HEADERS,
        "body":       json.dumps(body, default=str),
    }


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------
def run_query(query_name: str, filters: dict = None) -> dict:
    filters = filters or {}
    conn = get_conn()
    cur  = conn.cursor()

    # Parameterised duration queries
    if query_name in DURATION_QUERIES:
        sql, args = DURATION_QUERIES[query_name](filters)
        if args:
            cur.execute(sql, args)
        else:
            cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        if query_name in _SINGLE_ROW_QUERIES:
            return dict(zip(cols, rows[0])) if rows else {}
        return {"columns": cols, "rows": [list(r) for r in rows]}

    # Static queries
    if query_name not in QUERIES:
        available = sorted(list(QUERIES) + list(DURATION_QUERIES))
        return {"error": f"Unknown query '{query_name}'.", "available": available}

    cur.execute(QUERIES[query_name])
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()

    if query_name in _SINGLE_ROW_QUERIES:
        return dict(zip(cols, rows[0])) if rows else {}

    return {"columns": cols, "rows": [list(r) for r in rows]}


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    method = (
        event.get("requestContext", {})
             .get("http", {})
             .get("method", "GET")
             .upper()
    )

    if method == "OPTIONS":
        return _response(200, {})

    query_name = ""
    filters    = {}

    if method == "POST":
        try:
            body       = json.loads(event.get("body") or "{}")
            query_name = body.get("query", "")
            filters    = body.get("filters", {})
            if not isinstance(filters, dict):
                filters = {}
        except json.JSONDecodeError:
            return _response(400, {"error": "Invalid JSON body"})
    else:
        qs         = event.get("queryStringParameters") or {}
        query_name = qs.get("query", "")

    if not query_name:
        available = sorted(list(QUERIES) + list(DURATION_QUERIES))
        return _response(400, {"error": "Missing 'query' parameter", "available": available})

    logger.info("Running query: %s | filters: %s", query_name, filters)

    try:
        result = run_query(query_name, filters)
        return _response(200, result)
    except Exception as exc:
        logger.exception("Query failed: %s", exc)
        return _response(500, {"error": str(exc)})
