# 🎬 TMDB Movie Analytics Data Engineering Pipeline

An enterprise-grade, medallion-architecture (Bronze → Silver → Gold) data engineering pipeline built with **PySpark** and **Delta Lake** on **Databricks**. This pipeline ingests historical and daily incremental movie data from **The Movie Database (TMDB) REST API**, enforcing strict schema contracts, idempotent upserts, robust execution auditing, and parameterized backfills.

---

## 📌 Project Overview

This project builds a scalable entertainment data lakehouse to analyze global movie trends, ratings, production budgets, revenues, and genre performance over time.

- **Data Source:** [TMDB REST API v3](https://developer.themoviedb.org/reference/intro/getting-started)
- **Engine / Compute:** PySpark on Databricks (Community / Student Tier)
- **Storage Layer:** Delta Lake (Bronze & Silver layers)
- **Ingestion Patterns:**
  - **Full Load:** Historical multi-year movie discovery (year-split queries avoiding TMDB's 500-page limit) enriched with granular metadata (`budget`, `revenue`, `runtime`, `keywords`).
  - **Incremental Load:** Daily changed IDs from `/movie/changes` and `/trending/movie/day`, resolved to full movie records for idempotent updates.

---

## 🏗️ Medallion Architecture

```
[ TMDB API ] ──(Raw JSON)──► [ Bronze Layer ] ──(Cleanse / Cast / Dedupe)──► [ Silver Layer ]
                               (Delta Table)                                   (Delta Lake)
                                                                            ├── silver.movies
                                                                            └── silver.movie_genres
```

---

## 📊 Data Models & Schema Enforcement

### 1. Bronze Layer Model (`bronze.raw_movies`)
The Bronze layer acts as an immutable landing zone preserving raw API payloads as Delta tables, augmented with ingestion metadata.

| Column Name | Data Type | Constraint / PK | Description |
| :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | **Primary Key** | Unique TMDB movie identifier |
| `title` | `STRING` | Nullable | Primary movie title |
| `release_date` | `STRING` | Nullable | Unparsed release date string (YYYY-MM-DD) |
| `popularity` | `DOUBLE` | Nullable | TMDB daily popularity metric |
| `vote_average` | `DOUBLE` | Nullable | Weighted average user rating (0-10) |
| `vote_count` | `INTEGER` | Nullable | Number of user votes cast |
| `budget` | `BIGINT` | Nullable | Production budget in USD (0 if unknown) |
| `revenue` | `DOUBLE` | Nullable | Global box office revenue in USD (0 if unknown) |
| `runtime` | `INTEGER` | Nullable | Movie runtime in minutes |
| `genre_ids` | `ARRAY<INT>`| Nullable | Array of raw TMDB numerical genre IDs |
| `original_language` | `STRING` | Nullable | ISO 639-1 language code |
| `overview` | `STRING` | Nullable | Synopsis / summary text |
| `_load_timestamp` | `TIMESTAMP` | **Metadata** | Exact timestamp when record was written to Bronze |
| `_source_file` | `STRING` | **Metadata** | Source file name / parameter path |

---

### 2. Silver Layer Model (`silver.movies` & `silver.movie_genres`)

In the Silver layer, data types are explicitly cast, missing or zero values are standardized, records are deduplicated, and arrays are exploded into relational entities.

#### **Table: `silver.movies`**
*Main movie entity table holding clean, entity-level attributes.*

| Column Name | Data Type | Constraint / PK | Transformation / Casting Rule |
| :--- | :--- | :--- | :--- |
| `movie_id` | `INTEGER` | **Primary Key** | Renamed from `id`; non-null check enforced |
| `title` | `STRING` | Non-Null | Stripped whitespace, default `'Unknown Title'` |
| `release_date` | `DATE` | Nullable | Cast from `STRING` to `DATE` (`yyyy-MM-dd`) |
| `release_year` | `INTEGER` | Derived | Derived via `YEAR(release_date)` |
| `popularity` | `DOUBLE` | Non-Null | Defaulted to `0.0` if null |
| `vote_average` | `DOUBLE` | Non-Null | Defaulted to `0.0` if null |
| `vote_count` | `INTEGER` | Non-Null | Defaulted to `0` if null |
| `budget` | `BIGINT` | Nullable | Cast to `BIGINT`; `0` converted to `NULL` for clean aggregations |
| `revenue` | `BIGINT` | Nullable | Cast to `BIGINT`; `0` converted to `NULL` for clean aggregations |
| `runtime` | `INTEGER` | Nullable | Nullified if `runtime <= 0` |
| `original_language` | `STRING` | Non-Null | Lowercased ISO code |
| `overview` | `STRING` | Nullable | Cleaned synopsis text |
| `_processed_timestamp`| `TIMESTAMP` | **Metadata** | Timestamp when record passed Silver transformation |

#### **Table: `silver.movie_genres` (Bridge Table)**
*Relational bridge resolving the 1-to-many relationship between movies and genres without repeating movie metric rows.*

| Column Name | Data Type | Constraint / PK | Description |
| :--- | :--- | :--- | :--- |
| `movie_id` | `INTEGER` | **FK (Composite PK)** | References `silver.movies.movie_id` |
| `genre_id` | `INTEGER` | **FK (Composite PK)** | Numerical genre identifier (Exploded from `genre_ids` array) |
| `_processed_timestamp`| `TIMESTAMP` | **Metadata** | Processing timestamp |

---

## 🛠️ Key Pipeline Capabilities

### 1. Strict Schema-on-Read
To guarantee pipeline robustness, `inferSchema=True` is disabled across all ingestion paths. Raw JSON inputs are parsed using explicit PySpark `StructType` definitions:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, LongType

tmdb_raw_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("budget", LongType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("original_language", StringType(), True),
    StructField("overview", StringType(), True)
])
```

### 2. Idempotent Upserts (`MERGE INTO`)
To prevent duplicate records during repeated executions or daily incremental updates, Silver layer writes utilize Delta Lake `MERGE INTO` logic keyed on `movie_id`:

```python
from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, "silver.movies")

silver_table.alias("target").merge(
    source=df_silver_updates.alias("source"),
    condition="target.movie_id = source.movie_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### 3. Execution Auditing & Logging
All execution metrics are captured into a dedicated Delta table `audit.pipeline_execution_logs`:

| Column | Description |
| :--- | :--- |
| `execution_id` | Unique UUID for the run batch |
| `layer_processed` | Processing step (e.g., `RAW_TO_BRONZE`, `BRONZE_TO_SILVER`) |
| `batch_parameter` | Path or batch ID processed |
| `start_time` / `end_time` | Precise timestamp range |
| `status` | `SUCCESS` or `FAILED` |
| `records_processed` | Number of inserted or merged rows |
| `error_message` | Error trace if `status == FAILED` |

---

## 🚀 Execution & Backfill Guide

The pipeline uses **Databricks Widgets** for parameterization, allowing seamless execution across standard daily batches and historical backfills without modifying notebook code.

### Option A: Triggering a Standard Incremental Run
Run the notebook or job passing the daily incremental batch parameters:
```python
# Pass parameters dynamically via widgets
dbutils.widgets.text("load_type", "incremental")
dbutils.widgets.text("file_path", "/mnt/datalake/raw/2026-10-03/incremental_load.json")
```

### Option B: Triggering a Historical Backfill Run
To re-process or backfill historical datasets for any historical date range or batch file:
```python
# Set widgets for full backfill
dbutils.widgets.text("load_type", "full")
dbutils.widgets.text("file_path", "/mnt/datalake/raw/historical/full_load.json")
```

---

## 📁 Repository Structure

```text
├── README.md                          # Project documentation and data models
├── scripts/
│   └── tmdb_pull_samples_v2.py        # Python script for year-split & enriched API extraction
├── notebooks/
│   ├── 00_audit_logger.py             # Audit framework and log table creation
│   ├── 01_bronze_ingestion.py         # Explicit-schema raw JSON ingestion to Bronze Delta
│   └── 02_silver_transformation.py   # Cleansing, casting, bridge creation, & Delta MERGE
└── data/
    ├── sample_full_load.json          # Sample enriched full load payload
    └── sample_incremental_load.json   # Sample changed-movie incremental payload
```