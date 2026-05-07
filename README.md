# U.S. Electricity Market Analytics

An end-to-end data pipeline and analysis project using the EIA Open Data API, Python, PostgreSQL, and SQL. Built as a portfolio project to demonstrate data engineering and analysis skills including API extraction, relational database design, and advanced SQL.

---

## Project Overview

This project pulls monthly U.S. electricity data from the U.S. Energy Information Administration (EIA) Open Data API, loads it into a PostgreSQL data warehouse, and analyzes it using SQL. The analysis covers electricity generation by state and fuel type, retail electricity prices by state and customer sector, and natural gas prices — spanning 2020 through 2024.

**Key questions explored:**
- Which states have the highest and lowest retail electricity prices?
- How has the U.S. generation mix (coal, gas, solar, wind) shifted over time?
- Which fuel type dominates generation in each state?
- Is there a relationship between natural gas prices and retail electricity prices by state?

---

## Architecture

```
EIA Open Data API
       │
       ▼
  extract.py          ← paginated API calls with retry logic
       │
       ▼
  transform.py        ← clean, rename, and map dimension IDs
       │
       ▼
  load_dims.py        ← populate dimension tables, build ID lookup maps
       │
       ▼
  load_facts.py       ← insert transformed fact rows into PostgreSQL
       │
       ▼
 PostgreSQL (Docker)  ← star schema data warehouse
       │
       ▼
  SQL Analysis        ← aggregations, CTEs, window functions
```

---

## Database Schema

A star schema with three fact tables and five dimension tables.

**Fact Tables**
- `fact_gen` — monthly electricity generation by state, fuel, and sector
- `fact_prices` — monthly retail electricity prices by state and customer sector
- `fact_fuel_prices` — monthly natural gas prices by state

**Dimension Tables**
- `dim_date` — date attributes (year, month, quarter)
- `dim_state` — state code and name
- `dim_fuel` — fuel type code and description
- `dim_gensector` — generation sector (e.g. Electric Utility, IPP, Industrial)
- `dim_pricesector` — customer sector (e.g. Residential, Commercial, Industrial)

---

## Data Sources

All data sourced from the [EIA Open Data API](https://www.eia.gov/opendata/). A free API key is required — register at https://www.eia.gov/opendata/.

| Dataset | API Route | Key Fields |
|---|---|---|
| Electricity generation by state and fuel | `/electricity/electric-power-operational-data` | period, stateid, fueltypeid, sectorid, generation |
| Retail electricity prices by state | `/electricity/retail-sales` | period, stateid, sectorid, price |
| Natural gas prices | `/natural-gas/pri/sum` | period, duoarea, value |

**Date range:** January 2020 — December 2024  
**Frequency:** Monthly

---

## Setup and Installation

### Prerequisites
- Python 3.10+
- Docker and Docker Compose
- Conda or pip for package management

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/EIA_Analytics.git
cd EIA_Analytics
```

### 2. Create and activate conda environment
```bash
conda create -n EIA python=3.11
conda activate EIA
pip install -r requirements.txt
```

### 3. Configure API key
Create a `config.py` file in the project root (this file is excluded from version control):
```python
EIA_KEY = "your_api_key_here"
```

### 4. Start the database
The database runs in Docker and is pre-configured with the schema via `Dockerfile` and `compose.yml`. Start it with:
```bash
docker compose up -d
```

This builds a PostgreSQL container, exposes it on port 5432, and automatically runs `schema.sql` to create all tables on first startup. No manual schema setup is required.

### 5. Run the pipeline
```bash
python pipeline.py
```

The pipeline extracts data from the EIA API, transforms it, and loads it into PostgreSQL. Expect the full run to take 15–30 minutes due to API rate limiting and per-state chunking.

---

## Project Structure

```
EIA_Analytics/
├── config.py               # API key (excluded from version control)
├── pipeline.py             # Orchestrates the full ETL pipeline
├── extract.py              # EIA API extraction with pagination and retry
├── transform.py            # Data cleaning and dimension ID mapping
├── load_dims.py            # Dimension table loading and ID map generation
├── schema.sql              # PostgreSQL schema definition
├── Dockerfile              # PostgreSQL image with schema pre-loaded
├── compose.yml             # Container config with volumes and port exposure
├── queries/
│   ├── basic_aggs.sql      # GROUP BY, HAVING queries
│   ├── ctes.sql            # CTE patterns including multi-fact queries
│   └── window_funcs.sql    # LAG, RANK, running totals
├── notebooks/
│   ├── Analysis.ipynb      # SQL queries with results and commentary
│   ├── Dimensions.ipynb    # Exploration of dimension table creation
│   ├── Extract.ipynb       # Exploration for writing extract script
│   ├── Transform.ipynb     # Exploration for writing transform script
│   └── Testing.ipynb       # Pipeline and data loading tests
├── data/
│   ├── raw/                # Raw CSVs from API (excluded from version control)
│   └── cleaned/            # Transformed CSVs (excluded from version control)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## SQL Analysis

Queries are organized in three files under `queries/` and presented with results in `notebooks/analysis.ipynb`.

**Aggregations** (`basic_aggs.sql`)
- Average electricity price by state
- Total generation by fuel type
- Average price by state and customer sector
- Top 5 states by natural gas price
- States with above-average electricity prices (HAVING)
- Generation mix percentage by state

**CTEs** (`ctes.sql`)
- States with prices above the national average
- Generation mix percentage using chained CTEs
- Peak price month per state
- Natural gas price vs electricity price comparison (multi-fact CTE)

**Window Functions** (`window_funcs.sql`)
- State price rankings using RANK()
- Year-over-year price change using LAG()
- Running cumulative generation by state and fuel
- Dominant fuel type per state using RANK() with PARTITION BY

---

## Key Engineering Challenges

**API pagination and server errors**
The EIA API returns a maximum of 5,000 rows per call. For a five-year date range, the generation dataset contains over 400,000 rows. Naively paginating with a high offset caused intermittent 500 errors from the EIA server. The solution was to chunk requests by state, keeping each request well under the pagination threshold, combined with exponential backoff retry logic for transient server errors.

**Star schema loading order**
Fact tables reference dimension table IDs via foreign keys, but dimension IDs are only assigned by PostgreSQL at insert time (SERIAL). The solution was a two-phase load: first insert all dimension records and then query the dimension tables back into in-memory lookup dictionaries (`{code: id}`), which are then used to map dimension IDs onto fact rows before inserting.

**State and region normalization**
State identifiers are not consistent across EIA datasets. The natural gas dataset uses a `duoarea` field with a leading character prefix rather than a standard two-letter state code. Some datasets include regional aggregates (e.g. "Pacific Contiguous") that don't map cleanly to states. These were filtered and normalized during the transform step.

**Sector overlap in generation data**
The generation dataset includes both granular sectors (e.g. "IPP Non-CHP", "Commercial CHP") and aggregate rollup sectors (e.g. "All Sectors", "Electric Power"). Loading both would cause double-counting in any aggregation. Only the granular sectors were loaded into the fact table, preserving the ability to aggregate correctly in SQL.

---

## Tools and Technologies

| Tool | Purpose |
|---|---|
| Python 3.11 | ETL pipeline |
| requests | EIA API extraction |
| pandas | Data transformation |
| SQLAlchemy + psycopg2 | PostgreSQL connection |
| PostgreSQL 15 (Docker) | Data warehouse |
| Docker + Docker Compose | Database containerisation and schema initialisation |
| DBeaver | SQL query development |
| Jupyter | Analysis presentation |

---

## Potential Extensions

- Automate pipeline on a monthly schedule using Apache Airflow or a cron job
- Add Power BI dashboard for interactive visualization
- Expand to include additional EIA datasets (coal, nuclear, renewable capacity)
- Add data quality checks and logging to the pipeline
