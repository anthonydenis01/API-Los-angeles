# API-Los-angeles

Production-ready Python pipeline that pulls Signal/PortOptimizer KPIs and writes manager-friendly KPI tables directly into SQL for Power BI refresh.

## What this pipeline does
When you run `python -m src.main` (or `make run`), it:
1. Calls the Signal/PortOptimizer XHR endpoints you already use.
2. Transforms JSON into KPI tables with stable, Power BI-friendly columns.
3. Writes tables to SQL via SQLAlchemy.
4. Produces clean logs and a GitHub-ready structure.

Excel/CSV is **not** required anywhere in the flow. Any exports are optional for debugging only.

## KPI definitions
**A) Volume Pressure**
- `rolling_4w_avg_teu` = 4-week rolling mean of inbound full containers.
- `volume_pressure_index` = inbound full containers / rolling 4-week average.
- Flags: **HIGH** if >= 1.15, **LOW** if <= 0.90, else **NORMAL**.

**B) Terminal Congestion**
- Congested buckets default to `9-12 Days`, `13+ Days` (configurable).
- `congested_pct` computed per load type.
- Flags: Loaded **HIGH** if >= 0.25; Empty **HIGH** if >= 0.50; else **NORMAL**.

**C) Outgate Stress**
- Status-only output (no mode).
- Slow buckets default to `5-8 Days`, `9-12 Days`, `13+ Days` (configurable).
- `slow_pct` computed within each status.
- Flags: **HIGH** if `slow_pct` >= 0.40, else **NORMAL**.

**D) Berth**
- `avg_time_at_berth_hours` with configurable HIGH threshold.
- Includes a summary row + top N vessels at berth.

## Output tables
The pipeline writes these tables (schema configurable):
1. `kpi_weekly_volume_pressure`
2. `kpi_terminal_congestion`
3. `kpi_outgate_stress_by_status`
4. `kpi_berth_snapshot`
5. `kpi_health_summary`

Every table includes `extraction_ts_utc`, plus `from_date`/`to_date` when provided.

## Setup
### 1) Install dependencies
```bash
pip install -r requirements.txt
```

### 2) Configure environment
Copy `.env.example` to `.env` and update the values:
```bash
cp .env.example .env
```

Required values:
- `DATABASE_URL` (Postgres or SQL Server via SQLAlchemy)
- Signal endpoint URLs and payloads
- Cookies or headers (if required)

Example `DATABASE_URL` formats:
- Postgres: `postgresql+psycopg2://user:pass@host:5432/dbname`
- SQL Server: `mssql+pyodbc://user:pass@host:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server`

### 3) Run the pipeline
```bash
python -m src.main
```
Or:
```bash
make run
```

## Power BI
Connect Power BI directly to the SQL tables:
1. **Get Data** → **SQL Server** (or Postgres connector).
2. Select the database/schema.
3. Load the KPI tables above.
4. Schedule refresh in Power BI Service (no Excel step required).

## Notes on auth + reliability
- Uses `requests.Session()` with retry/backoff for 429/5xx.
- Cookies/headers are provided via `.env` or `cookies.json` (ignored by git).
- Robust parsing raises clear errors if JSON shapes change or buckets are empty.

## Project structure
```
API-LOS ANGELES/
  src/
    __init__.py
    config.py
    client.py
    transforms.py
    kpis.py
    db.py
    main.py
  requirements.txt
  README.md
  .env.example
  Makefile
  .gitignore
```
