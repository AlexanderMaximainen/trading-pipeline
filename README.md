# Trading Pipeline

A personal data engineering project that processes trading data exported from Avanza and transforms it
into analytics-ready data for visualization in Power BI.

The pipeline is built around a real use case: a friend who actively trades wanted better insight into their trading 
performance. Trading data is exported as CSV files and automatically processed through a structured pipeline.

<hr>

## Purpose

The goal of this project was to build a complete data pipeline from scratch using real-world data from a friend.

Key objectives:
- Build a structured data model (Bronze-Silver-Gold)
- Implement incremental data loading
- Separate load, transformation, and presentation layers
- Enable analytics in Power BI

<hr>

## Architecture

The pipeline follows a Medallion Architecture:

- **Bronze** - Raw CSV data from Avanza
- **Silver** - Cleaned and transformed data (staging, dimensions, facts)
- **Gold** - Aggregated views used for analysis

<hr>

## Pipeline Flow

1. CSV files are exported from Avanza and placed in `data/raw`
2. Python scripts load new data into the **Bronze** layer
3. SQL procedures transform data into the **Silver** layer:
    - staging
    - dimension tables
    - fact tables
4. **Gold views** provide analytics-ready data
5. Data is visualized in Power BI

<hr>

## Tech Stack

- Python (pandas, SQLAlchemy, pyodbc)
- SQL Server
- Power BI

<hr>

## Project Structure

```
trading-pipeline/
│
├── data/
│   └── raw/
│
├── scripts/
│   ├── fetch_data.py
│   ├── load_data.py
│   └── run_pipeline.py
│
├── sql/
│   ├── ddl/
│   │   ├── 01_create_database.sql
│   │   ├── 02_create_bronze_table.sql
│   │   ├── 03_create_silver_staging_table.sql
│   │   ├── 04_create_silver_dim_and_fact_tables.sql
│   │   └── 05_create_gold_views.sql
│   │
│   └── etl/
│       ├── 01_create_silver_staging_procedure.sql
│       ├── 02_create_silver_dim_and_fact_procedures.sql
│       └── 03_create_silver_load_master_procedure.sql
│
├── .gitignore
├── requirements.txt
└── README.md
```

<hr>

## How to run

1. Place CSV files in: `data/raw`
2. Run the pipeline: `python scripts/run_pipeline.py`

The pipeline will:
- load new files into Bronze
- update Silver tables
- refresh Gold views

<hr>

## Design Choices

- Incremental loading based on `source_file`
- Separation between Python (orchestration) and SQL (transformations)
- `dim_date` used where needed rather than joined everywhere
- Instrument attributes are derived from naming patterns (asset, direction, issuer)
- Unknown values are left as NULL and handled in reporting

<hr>

## Future Improvements

- Add Power BI dashboard screenshots
- Improve instrument classification (move rules to mapping table)
- Add scheduled execution for the pipeline
- Add logging and monitoring