# ClinicalTrials Phase 1 Dashboard

An end-to-end AWS pipeline that fetches Phase 1 clinical trial data from [ClinicalTrials.gov](https://clinicaltrials.gov), stores it in Aurora PostgreSQL, and exposes it through two interactive dashboards hosted on S3.

---

## Architecture

```
EventBridge (scheduled)
        │
        ▼
  Step Functions
        │
        ▼
  Lambda: clinicaltrials-fetcher        ← fetches pages from ClinicalTrials.gov API
        │  (lambda_handler.py)
        │  writes NDJSON files per page
        ▼
       S3 bucket (raw NDJSON)
        │
        ▼
  Lambda: load-ct-ph1-db                ← transforms & loads into Aurora PostgreSQL
        │  (transform_lambda.py)
        ▼
  Aurora PostgreSQL (9 normalised tables)
        │
        ▼
  Lambda: query-ct-ph1-db               ← read-only query API via Function URL
        │  (query_lambda.py)
        ▼
  S3 Static Website                     ← two HTML dashboards
     dashboard.html                     ← overview (all statuses)
     dashboard_duration.html            ← completed study duration, with filters
```

---

## Project Structure

```
ct_phase1/
├── README.md
├── lambdas/
│   ├── lambda_handler.py      # Fetcher Lambda — pulls ClinicalTrials.gov API into S3
│   ├── transform_lambda.py    # Transform Lambda — loads NDJSON from S3 into Aurora
│   └── query_lambda.py        # Query Lambda — read-only API for dashboards
├── step_functions/
│   └── step_function.json     # Step Functions state machine definition
└── dashboard/
    ├── dashboard.html          # Overview dashboard
    └── dashboard_duration.html # Study duration dashboard (completed trials, filterable)
```

---

## Prerequisites

- AWS account with access to Lambda, Step Functions, S3, Aurora, EventBridge, IAM
- Aurora Serverless v2 PostgreSQL cluster with IAM authentication enabled
- Python 3.12 Lambda runtime
- pg8000 packaged as a Lambda Layer (pure Python PostgreSQL driver)

---

## Setup

### 1. S3 Bucket (raw data)

Create a bucket for raw NDJSON files:

```bash
aws s3 mb s3://YOUR-RAW-DATA-BUCKET --region us-east-2
```

### 2. Aurora PostgreSQL

Create an Aurora Serverless v2 PostgreSQL cluster. Enable IAM database authentication:

```bash
aws rds modify-db-cluster \
  --db-cluster-identifier YOUR-CLUSTER-ID \
  --enable-iam-database-authentication \
  --apply-immediately --region us-east-2
```

Grant the IAM role to the postgres user (from psql):

```sql
GRANT rds_iam TO postgres;
```

### 3. pg8000 Lambda Layer

Build the layer in a Linux environment (e.g. CloudShell):

```bash
mkdir -p python
pip install pg8000 -t python/ --break-system-packages
zip -r pg8000_layer.zip python/

aws lambda publish-layer-version \
  --layer-name pg8000 \
  --zip-file fileb://pg8000_layer.zip \
  --compatible-runtimes python3.12 \
  --region us-east-2
```

### 4. Fetcher Lambda (`lambda_handler.py`)

```bash
pip install requests -t package/
cp lambdas/lambda_handler.py package/
cd package && zip -r ../fetcher.zip . && cd ..

aws lambda create-function \
  --function-name clinicaltrials-fetcher \
  --runtime python3.12 \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://fetcher.zip \
  --timeout 120 --memory-size 256 \
  --role arn:aws:iam::ACCOUNT_ID:role/YOUR-LAMBDA-ROLE \
  --region us-east-2
```

**Environment variables:**

| Variable | Description |
|---|---|
| `CT_S3_BUCKET` | Target S3 bucket name |
| `CT_S3_PREFIX` | Key prefix (default: `clinicaltrials/phase1/`) |
| `CT_START_YEAR` | Earliest study start year, e.g. `2022` (blank = all years) |
| `CT_PAGE_SIZE` | Records per page, 1–1000 (default: `1000`) |

### 5. Step Functions State Machine

```bash
aws stepfunctions create-state-machine \
  --name clinicaltrials-ph1-fetcher \
  --definition file://step_functions/step_function.json \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/YOUR-SF-ROLE \
  --region us-east-2
```

**Execution input:**

```json
{
  "bucket": "YOUR-RAW-DATA-BUCKET",
  "prefix": "clinicaltrials/phase1/",
  "start_year": "2022"
}
```

Use `"start_year": ""` to fetch all years.

### 6. Transform Lambda (`transform_lambda.py`)

```bash
cp lambdas/transform_lambda.py package/
cd package && zip -r ../transform.zip . && cd ..

aws lambda create-function \
  --function-name load-ct-ph1-db \
  --runtime python3.12 \
  --handler transform_lambda.lambda_handler \
  --zip-file fileb://transform.zip \
  --timeout 900 --memory-size 512 \
  --layers arn:aws:lambda:us-east-2:ACCOUNT_ID:layer:pg8000:VERSION \
  --role arn:aws:iam::ACCOUNT_ID:role/YOUR-LAMBDA-ROLE \
  --region us-east-2
```

**Environment variables:**

| Variable | Description |
|---|---|
| `CT_DB_HOST` | Aurora cluster endpoint |
| `CT_DB_NAME` | Database name (default: `postgres`) |
| `CT_DB_USER` | Database user (default: `postgres`) |
| `CT_DB_PORT` | Port (default: `5432`) |
| `CT_S3_BUCKET` | S3 bucket with NDJSON files |
| `CT_S3_PREFIX` | Key prefix of NDJSON files |

**IAM permissions required:** `s3:GetObject`, `s3:ListBucket`, `rds-db:connect`

**Create tables and load data:**

```json
{"action": "create_tables"}
{"action": "process_all"}
```

The `process_all` action tracks processed files in a `processed_files` table — re-running it safely skips already-loaded files.

### 7. Query Lambda (`query_lambda.py`)

```bash
cp lambdas/query_lambda.py package/
cd package && zip -r ../query.zip . && cd ..

aws lambda create-function \
  --function-name query-ct-ph1-db \
  --runtime python3.12 \
  --handler query_lambda.lambda_handler \
  --zip-file fileb://query.zip \
  --timeout 30 --memory-size 256 \
  --layers arn:aws:lambda:us-east-2:ACCOUNT_ID:layer:pg8000:VERSION \
  --role arn:aws:iam::ACCOUNT_ID:role/YOUR-LAMBDA-ROLE \
  --region us-east-2
```

**Same DB environment variables as the transform Lambda.**

Enable a Function URL with CORS:

```bash
aws lambda create-function-url-config \
  --function-name query-ct-ph1-db \
  --auth-type NONE --region us-east-2

aws lambda update-function-url-config \
  --function-name query-ct-ph1-db \
  --cors '{"AllowOrigins":["*"],"AllowMethods":["GET","POST"],"AllowHeaders":["Content-Type"],"MaxAge":300}' \
  --region us-east-2
```

Copy the Function URL and paste it into both dashboard HTML files as `LAMBDA_URL`.

### 8. Dashboard (S3 Static Site)

```bash
# Create bucket
aws s3 mb s3://YOUR-DASHBOARD-BUCKET --region us-east-2

# Disable block public access
aws s3api put-public-access-block \
  --bucket YOUR-DASHBOARD-BUCKET \
  --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false"

# Apply bucket policy
aws s3api put-bucket-policy --bucket YOUR-DASHBOARD-BUCKET --policy '{
  "Version":"2012-10-17",
  "Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::YOUR-DASHBOARD-BUCKET/*"}]
}'

# Enable static website hosting
aws s3 website s3://YOUR-DASHBOARD-BUCKET \
  --index-document dashboard.html

# Upload dashboards
aws s3 cp dashboard/dashboard.html s3://YOUR-DASHBOARD-BUCKET/dashboard.html --content-type text/html
aws s3 cp dashboard/dashboard_duration.html s3://YOUR-DASHBOARD-BUCKET/dashboard_duration.html --content-type text/html
```

Access at:
`http://YOUR-DASHBOARD-BUCKET.s3-website.us-east-2.amazonaws.com/dashboard.html`

---

## Database Schema

| Table | Description |
|---|---|
| `studies` | Core study fields (status, dates, sponsor, enrollment) |
| `study_conditions` | Medical conditions per study |
| `study_interventions` | Drug / device interventions per study |
| `study_locations` | Site countries and facilities |
| `study_contacts` | Primary contacts |
| `study_outcomes` | Primary and secondary outcomes |
| `study_eligibility` | Eligibility criteria |
| `study_references` | Publications and citations |
| `processed_files` | File tracking table (prevents duplicate loads) |

The `start_end` column on `studies` stores the duration in days (`completion_date - start_date`) and powers the duration dashboard.

---

## Dashboards

### Overview Dashboard (`dashboard.html`)
All Phase 1 studies across all statuses. Charts: study status breakdown, studies started by year, top conditions, top countries, sponsor class, intervention types, enrollment distribution.

### Duration Dashboard (`dashboard_duration.html`)
Completed studies only. Filters: completion year, country, multi-country toggle, healthy volunteers toggle. Charts: duration histogram, average duration trend by year, average duration by sponsor class. Detail table of top 100 longest studies.

---

## Scheduled Refresh

Trigger the full pipeline on a schedule via EventBridge:

```bash
aws events put-rule \
  --name ct-ph1-weekly \
  --schedule-expression "cron(0 6 ? * MON *)" \
  --state ENABLED \
  --region us-east-2
```

---

## Notes

- The fetcher Lambda uses the [ClinicalTrials.gov v2 API](https://clinicaltrials.gov/api/v2/studies) with the Essie query `AREA[Phase]Phase1`.
- All Lambda-to-Aurora connections use IAM authentication (no stored passwords).
- pg8000 is used instead of psycopg2 to avoid C-extension platform compilation issues on Lambda.
- The transform Lambda is idempotent: re-running `process_all` only processes new files.
