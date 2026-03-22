# S&P 500 Financial Analytics Pipeline

An end-to-end ETL/ELT data pipeline that extracts S&P 500 company financial data from three sources, transforms it using Apache Spark and dbt, loads it into a data lake (AWS S3) and data warehouse (PostgreSQL), orchestrates everything with Apache Airflow, and visualises insights in Apache Superset.

---

## Architecture
```
Sources                    Extract              Transform           Load                    Serve
─────────────────────────────────────────────────────────────────────────────────────────────────
Kaggle CSV     ──────────► Python/pandas ──►
Alpha Vantage  ──────────► requests API  ──► PySpark (ETL) ──► AWS S3 (data lake)  ──► Athena SQL
SEC EDGAR      ──────────► HTTP JSON     ──► dbt (ELT)     ──► PostgreSQL (DWH)    ──► Superset
                                    ▲
                              Apache Airflow
                          (orchestration + scheduling)
```

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.13 | Extraction scripts, pipeline logic |
| Apache Airflow 2.8 | Orchestration, scheduling, monitoring |
| Apache Spark (PySpark) | Batch ETL transformations |
| dbt (data build tool) | ELT SQL transformations, data modelling |
| PostgreSQL 15 | Source database + data warehouse |
| AWS S3 | Data lake (raw + curated zones) |
| AWS Glue | Data catalogue |
| AWS Athena | Serverless SQL on S3 |
| Apache Superset | Business intelligence dashboards |
| Docker + Docker Compose | Local service orchestration |

## Data Sources

- **Kaggle** — S&P 500 company financials CSV (505 companies, 14 columns)
- **Alpha Vantage API** — Income statements, balance sheets, cash flow statements
- **SEC EDGAR** — Official XBRL financial filings (no API key required)

## Pipeline Phases

### Phase 1 — Environment setup
AWS IAM configuration, Python virtual environment, S3 data lake bucket creation with raw/ and curated/ zones.

### Phase 2 — Extraction
Three extraction scripts pulling from different source types:
- `extract/extract_csv.py` — flat file ingestion
- `extract/extract_api.py` — REST API with rate limiting
- `extract/extract_sec.py` — public HTTP JSON endpoint

### Phase 3 — Transform
Dual transformation approach demonstrating both ETL and ELT patterns:
- `transform/spark_transform.py` — PySpark batch transforms, Parquet output
- `sp500_dbt/` — dbt models: staging layer + mart layer with financial ratio calculations

### Phase 4 — Load
- `load/load_to_s3.py` — uploads raw and curated data to S3 data lake
- `load/load_to_postgres.py` — loads raw tables into PostgreSQL warehouse
- AWS Athena external tables for serverless querying

### Phase 5 — Orchestration
Apache Airflow DAG (`dags/sp500_pipeline.py`) with:
- 7 tasks running daily on a schedule
- Parallel extraction (3 sources simultaneously)
- Automatic retries on failure
- Full pipeline: extract → transform → dbt → load PostgreSQL → load S3

### Phase 6 — Visualisation
Apache Superset dashboard with 4 charts:
- Average market cap by sector
- Average P/E ratio by sector
- Company distribution by sector (pie)
- EBITDA vs market cap scatter plot (505 companies)

## Key Financial Metrics Calculated

| Metric | Formula |
|--------|---------|
| Gross margin % | Gross profit / Revenue × 100 |
| Net profit margin % | Net income / Revenue × 100 |
| Return on assets % | Net income / Total assets × 100 |
| Return on equity % | Net income / Shareholders equity × 100 |
| Debt ratio % | Total liabilities / Total assets × 100 |
| EBITDA to market cap | EBITDA / Market cap × 100 |

## Data Model
```
raw_financials          ──► stg_financials ──────────────────────────────┐
raw_income_statements   ──► stg_income_statements ──► mart_financials ◄──┤
raw_sec_facts           ──► stg_sec_facts ──────────────────────────────┘
```

## Running the Project

### Prerequisites
- Docker Desktop
- Python 3.11+
- AWS account (free tier)
- AWS CLI configured

### Setup
```bash
git clone https://github.com/ughitsashwin/sp500-etl-pipeline
cd sp500-etl-pipeline
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### Start services
```bash
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
docker-compose up -d
```

### Run pipeline manually
```bash
python extract/extract_csv.py
python extract/extract_api.py
python extract/extract_sec.py
python transform/spark_transform.py
python load/load_to_postgres.py
python load/load_to_s3.py
cd sp500_dbt && dbt run && cd ..
```

### Access services
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |

## Project Structure
```
sp500-etl-pipeline/
├── dags/                    # Airflow DAG
├── extract/                 # Extraction scripts (CSV, API, SEC)
├── transform/               # PySpark transformation
├── load/                    # PostgreSQL and S3 loaders
├── sp500_dbt/               # dbt project (staging + mart models)
├── data/raw/                # Raw data (gitignored)
├── notebooks/               # Jupyter analysis
├── docker-compose.yml       # All services
└── requirements.txt
```
