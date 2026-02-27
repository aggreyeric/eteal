# eteal

A lightweight Python ETL library for extracting data from SQL databases
(including **Microsoft SQL Server**), applying transformations, and loading
results to SQL targets or **MinIO / S3-compatible** object storage.

---

## Features

| Stage | What it does |
|-------|-------------|
| **Extract** | Query any SQLAlchemy-supported database (SQLite, PostgreSQL, MSSQL, …) and return a `pandas.DataFrame` |
| **Transform** | Pure-DataFrame helpers: deduplication, row filtering, column renaming, dtype casting |
| **Load** | Write DataFrames to SQL tables **or** upload CSV / Parquet files to MinIO / S3 |

---

## Project layout

```
eteal/
├── eteal/               # Library source
│   ├── extract/         # SqlExtractor
│   ├── transform/       # drop_duplicates, filter_rows, rename_columns, cast_columns
│   └── load/            # SqlLoader, MinioLoader
├── tests/               # pytest test suite
├── docker-compose.yml   # MSSQL + MinIO containers for integration tests
└── pyproject.toml       # Project metadata & dependencies
```

---

## Installation

```bash
# Core library
pip install eteal

# With MSSQL support (requires the ODBC 17/18 driver)
pip install "eteal[mssql]"

# Development (includes pytest, testcontainers, …)
pip install "eteal[dev]"
```

---

## Quick start

### Extract

```python
from sqlalchemy import create_engine
from eteal.extract import SqlExtractor

engine = create_engine("mssql+pyodbc://sa:password@localhost/mydb?driver=ODBC+Driver+18+for+SQL+Server")
extractor = SqlExtractor(engine)
df = extractor.extract("SELECT * FROM sales WHERE year = :yr", params={"yr": 2024})
```

### Transform

```python
from eteal.transform import drop_duplicates, filter_rows, rename_columns, cast_columns

df = drop_duplicates(df, subset=["order_id"])
df = filter_rows(df, lambda d: d["amount"] > 0)
df = rename_columns(df, {"cust_name": "customer_name"})
df = cast_columns(df, {"amount": float})
```

### Load to SQL

```python
from eteal.load import SqlLoader

loader = SqlLoader("sqlite:///output.db")
loader.load(df, "clean_sales", if_exists="replace")
```

### Load to MinIO

```python
from eteal.load import MinioLoader

loader = MinioLoader("localhost:9000", "minioadmin", "minioadmin")
loader.load_csv(df, bucket_name="etl-output", object_name="sales/2024.csv")
loader.load_parquet(df, bucket_name="etl-output", object_name="sales/2024.parquet")
```

---

## Running tests

### Unit tests (no Docker required)

```bash
pip install "eteal[dev]"
pytest
```

### Integration tests (requires Docker)

```bash
docker compose up -d          # start MSSQL + MinIO
pytest -m integration         # run integration tests only
docker compose down           # stop containers
```

---

## Docker services

| Service | Port | Credentials |
|---------|------|-------------|
| **MSSQL** (SQL Server 2022 Developer) | `1433` | `sa` / `Eteal_Dev_2024!` |
| **MinIO** S3 API | `9000` | `minioadmin` / `minioadmin` |
| **MinIO** web console | `9001` | `minioadmin` / `minioadmin` |

Start them with:

```bash
docker compose up -d
```

---

## License

MIT