# 🎬 TMDB Movie Analytics Data Engineering Pipeline

An enterprise-grade, medallion-architecture (Bronze → Silver → Gold) data engineering pipeline built with **PySpark** and **Delta Lake** on **Databricks**. This pipeline ingests historical and daily incremental movie data from **The Movie Database (TMDB) REST API**, enforcing strict schema contracts, idempotent upserts, robust execution auditing, and parameterized backfills.

---

## 📌 Project Overview

This project builds a scalable entertainment data lakehouse to analyze global movie trends, ratings, production budgets, revenues, and genre performance over time.

- **Data Source:** [TMDB REST API v3](https://developer.themoviedb.org/reference/intro/getting-started)
- **Engine / Compute:** PySpark on Databricks (Community / Student Tier)
- **Storage Layer:** Delta Lake (Bronze, Silver & Gold layers)
- **Ingestion Patterns:**
  - **Full Load:** Historical multi-year movie discovery (year-split queries avoiding TMDB's 500-page limit) enriched with granular metadata (`budget`, `revenue`, `runtime`, `keywords`).
  - **Incremental Load:** Daily changed IDs from `/movie/changes` and `/trending/movie/day`, resolved to full movie records for idempotent updates.

---

## 🏗️ Medallion Architecture

```
[ TMDB API ] ──(Raw JSON)──► [ Bronze Layer ] ──(Parse / Cleanse / Cast / Dedupe)──► [ Silver Layer ] ──(Star Schema Build)──► [ Gold Layer ]
                               workspace.tmdb.bronze_movies                       (Delta Lake)
                                                                                  ├── workspace.tmdb.silver_movies
                                                                                  ├── workspace.tmdb.silver_genre_bridge
                                                                                  └── workspace.tmdb.silver_genres

Gold Star Schema:  workspace.tmdb.dim_movies · workspace.tmdb.dim_date
                   workspace.tmdb.fact_movie_performance
```

All tables live in the **`workspace.tmdb`** Unity Catalog schema. Raw API payloads are staged as JSON files in the **`workspace.tmdb.raw`** UC Volume before Bronze ingestion.

---

## 📊 Data Models & Schema Enforcement

### 1. Bronze Layer Model (`workspace.tmdb.bronze_movies`)
The Bronze layer acts as an immutable landing zone preserving raw API payloads as Delta tables, augmented with ingestion metadata. Each row stores the **full raw JSON** of a single TMDB movie record in the `raw_record` column, enabling schema evolution without re-pulling from the API.

| Column Name | Data Type | Constraint / PK | Description |
| :--- | :--- | :--- | :--- |
| `pulled_at` | `STRING` | **Metadata** | Timestamp when the API pull was performed |
| `source_endpoint` | `STRING` | **Metadata** | TMDB API endpoint URL used |
| `source_collection` | `STRING` | **Metadata** | Origin: `full_load`, `trending`, or `changed` |
| `source_file` | `STRING` | **Metadata** | Source JSON file path in the UC Volume |
| `raw_record` | `STRING` | **Payload** | Full raw JSON string of a single movie record |
| `batch_id` | `STRING` | **Metadata** | Unique batch identifier (auto-timestamped or user-supplied) |
| `load_timestamp` | `TIMESTAMP` | **Metadata** | Exact timestamp when record was written to Bronze |

A quarantine table (`workspace.tmdb.quarantine_movies`) captures records that fail schema validation during Bronze ingestion.

---

### 2. Silver Layer Model

In the Silver layer, raw JSON is parsed with a strict `StructType`, data types are explicitly cast, records are deduplicated (keeping the latest by `load_timestamp`), and the genres array is exploded into two relational tables: a bridge table and a genre lookup.

#### **Table: `workspace.tmdb.silver_movies`**
*Main movie entity table holding clean, entity-level attributes. Populated via idempotent Delta `MERGE` keyed on `movie_id`.*

| Column Name | Data Type | Constraint / PK | Transformation / Casting Rule |
| :--- | :--- | :--- | :--- |
| `movie_id` | `INTEGER` | **Primary Key** | Parsed from `raw_record` JSON `id` field; nulls filtered out |
| `title` | `STRING` | Non-Null | Stripped whitespace; empty/blank titles filtered out |
| `original_title` | `STRING` | Nullable | Original title from TMDB |
| `overview` | `STRING` | Nullable | Synopsis / summary text |
| `release_date` | `DATE` | Nullable | Cast from `STRING` via `try_to_date` (`yyyy-MM-dd`) |
| `release_year` | `INTEGER` | Derived | Derived via `YEAR(release_date)` |
| `popularity` | `DOUBLE` | Nullable | TMDB daily popularity metric |
| `vote_average` | `DOUBLE` | Nullable | Weighted average user rating (0-10) |
| `vote_count` | `LONG` | Nullable | Number of user votes cast |
| `budget` | `LONG` | Nullable | Production budget in USD (from enriched metadata) |
| `revenue` | `LONG` | Nullable | Global box office revenue in USD (from enriched metadata) |
| `runtime` | `INTEGER` | Nullable | Movie runtime in minutes (from enriched metadata) |
| `status` | `STRING` | Nullable | Movie release status (from enriched metadata) |
| `tagline` | `STRING` | Nullable | Movie tagline (from enriched metadata) |
| `adult` | `BOOLEAN` | Nullable | Adult content flag from TMDB |
| `backdrop_path` | `STRING` | Nullable | Backdrop image path on TMDB CDN |
| `poster_path` | `STRING` | Nullable | Poster image path on TMDB CDN |
| `original_language` | `STRING` | Nullable | Uppercased ISO 639-1 language code (`TRIM` + `UPPER`) |
| `source_collection` | `STRING` | **Metadata** | Origin: `full_load`, `trending`, or `changed` |
| `source_file` | `STRING` | **Metadata** | Source JSON file path from Bronze |
| `pulled_at` | `DATE` | **Metadata** | Date the API pull was performed |
| `batch_id` | `STRING` | **Metadata** | Bronze batch identifier |
| `load_timestamp` | `TIMESTAMP` | **Metadata** | Bronze ingestion timestamp |
| `updated_at` | `TIMESTAMP` | **Metadata** | Silver processing timestamp (`current_timestamp()`) |

#### **Table: `workspace.tmdb.silver_genre_bridge`**
*Relational bridge resolving the many-to-many relationship between movies and genres. Populated via idempotent Delta `MERGE` keyed on `(movie_id, genre_id)`.*

| Column Name | Data Type | Constraint / PK | Transformation / Casting Rule |
| :--- | :--- | :--- | :--- |
| `movie_id` | `LONG` | **FK (Composite PK)** | References `silver_movies.movie_id`; exploded from genres array |
| `genre_id` | `LONG` | **FK (Composite PK)** | Genre identifier from exploded genres array |
| `batch_id` | `STRING` | **Metadata** | Bronze batch identifier |
| `load_timestamp` | `TIMESTAMP` | **Metadata** | Bronze ingestion timestamp |
| `updated_at` | `TIMESTAMP` | **Metadata** | Silver processing timestamp (`current_timestamp()`) |

#### **Table: `workspace.tmdb.silver_genres`**
*Genre lookup / dimension table. Populated via idempotent Delta `MERGE` keyed on `genre_id`.*

| Column Name | Data Type | Constraint / PK | Transformation / Casting Rule |
| :--- | :--- | :--- | :--- |
| `genre_id` | `LONG` | **Primary Key** | Genre identifier from exploded genres array |
| `genre_name` | `STRING` | Non-Null | Genre name from TMDB, trimmed; nulls filtered out |
| `batch_id` | `STRING` | **Metadata** | Bronze batch identifier |
| `load_timestamp` | `TIMESTAMP` | **Metadata** | Bronze ingestion timestamp |
| `updated_at` | `TIMESTAMP` | **Metadata** | Silver processing timestamp (`current_timestamp()`) |

#### Entity Relationship

```
silver_movies (1) ──► (M) silver_genre_bridge (M) ◄── (1) silver_genres
   movie_id ─────────────── movie_id
                           genre_id ──────────────── genre_id
```

### 3. Gold Layer Star Schema

The Gold layer serves analytics-ready data as a classic star schema — two dimensions and one fact table. All tables are Delta Lake with **liquid clustering** (`CLUSTER BY`), populated via idempotent `MERGE INTO`, and maintained with `OPTIMIZE`. Built by `gold/gold_transformation` from `workspace.tmdb.silver_movies`; each run is wrapped in try/except and audit-logged to `pipeline_execution_logs` with layer `Silver-to-Gold` (rows_inserted, status, timing).

#### **Table: `workspace.tmdb.dim_movies`** — movie dimension
*Idempotent Delta `MERGE` keyed on `movie_id`; liquid clustered by `movie_id`.*

| Column Name | Data Type | Constraint / PK | Description |
| :--- | :--- | :--- | :--- |
| `movie_id` | `BIGINT` | **Primary Key** | TMDB movie identifier |
| `title` | `STRING` | Non-Null | Display title |
| `original_title` | `STRING` | Nullable | Original-language title |
| `overview` | `STRING` | Nullable | Synopsis |
| `release_date` | `DATE` | Nullable | Release date |
| `status` | `STRING` | Nullable | Release status |
| `tagline` | `STRING` | Nullable | Marketing tagline |

#### **Table: `workspace.tmdb.dim_date`** — date dimension (one row per distinct release date)
*Idempotent Delta `MERGE` keyed on `date_key`; liquid clustered by `date_key`.*

| Column Name | Data Type | Constraint / PK | Description |
| :--- | :--- | :--- | :--- |
| `date_key` | `INT` | **Primary Key** | YYYYMMDD integer (e.g. `20080417`) |
| `release_date` | `DATE` | | Release date |
| `year` | `INT` | | Release year |
| `quarter` | `INT` | | 1–4 |
| `month` | `INT` | | 1–12 |
| `day` | `INT` | | Day of month |
| `day_of_week` | `INT` | | 1 (Sunday) … 7 (Saturday) |

#### **Table: `workspace.tmdb.fact_movie_performance`** — fact table
*Grain: one row per movie with a non-null `release_date`. Idempotent Delta `MERGE` keyed on `(movie_id, date_key)`; liquid clustered by `(movie_id, date_key)`.*

| Column Name | Data Type | Constraint / PK | Description |
| :--- | :--- | :--- | :--- |
| `movie_id` | `BIGINT` | **FK → dim_movies** | Movie identifier |
| `date_key` | `INT` | **FK → dim_date** | Release-date key (YYYYMMDD) |
| `budget` | `BIGINT` | | Production budget (USD) |
| `revenue` | `BIGINT` | | Global box-office revenue (USD) |
| `profit` | `BIGINT` | Derived | `revenue - budget` |
| `roi_percentage` | `DOUBLE` | Derived | `(revenue - budget) / budget * 100`; NULL when `budget <= 0` |
| `popularity` | `DOUBLE` | | TMDB popularity score |
| `vote_average` | `DOUBLE` | | User rating (0–10) |
| `vote_count` | `BIGINT` | | Number of user votes |
| `batch_id` | `STRING` | **Metadata** | Source Silver batch |
| `load_timestamp` | `TIMESTAMP` | **Metadata** | Gold load time |

> **Note:** Movies with a `NULL` `release_date` (27 of 197,871) cannot join `dim_date` and are excluded from the fact table. `OPTIMIZE` runs on `dim_movies` and `fact_movie_performance` after each load.

#### Entity Relationship

```
dim_movies (1) ──► (M) fact_movie_performance (M) ◄── (1) dim_date
    movie_id ────────────── movie_id
                           date_key ──────────────── date_key
```

#### Validation (executed 2026-10-21)
- `dim_movies` primary-key uniqueness: 197,871 rows, **0 duplicates**
- Row counts: dim_movies 197,871 · dim_date 8,142 · fact_movie_performance 197,844
- Star-schema top-10 revenue join verified (Avatar leads: $2.92B revenue, 1133.63% ROI)
- Audit summary: Raw-to-Bronze 2 runs · Bronze-to-Silver 13 runs · Silver-to-Gold 1 run · 0 rows quarantined

---

## 🛠️ Key Pipeline Capabilities

### 1. Strict Schema-on-Read
To guarantee pipeline robustness, `inferSchema=True` is disabled across all ingestion paths. Raw JSON inputs are parsed using explicit PySpark `StructType` definitions:

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType
)

# Schema for parsing the raw_record JSON string in Bronze
tmdb_movie_schema = StructType([
    StructField("id",                LongType(),    True),
    StructField("title",             StringType(),  True),
    StructField("original_title",    StringType(),  True),
    StructField("overview",          StringType(),  True),
    StructField("release_date",      StringType(),  True),
    StructField("popularity",        DoubleType(),  True),
    StructField("vote_average",      DoubleType(),  True),
    StructField("vote_count",        LongType(),    True),
    StructField("adult",             BooleanType(), True),
    StructField("backdrop_path",     StringType(),  True),
    StructField("poster_path",       StringType(),  True),
    StructField("original_language", StringType(),  True),
    StructField("genres", ArrayType(
        StructType([
            StructField("id",   LongType(),   True),
            StructField("name", StringType(), True)
        ])
    ), True)
])
```

### 2. Idempotent Upserts (`MERGE INTO`)
To prevent duplicate records during repeated executions or daily incremental updates, Silver layer writes utilize Delta Lake `MERGE INTO` logic keyed on the primary key of each table:

```python
from delta.tables import DeltaTable

# silver_movies — keyed on movie_id
movies_table = DeltaTable.forName(spark, "workspace.tmdb.silver_movies")
(movies_table.alias("target")
  .merge(silver_movies_df.alias("source"), "target.movie_id = source.movie_id")
  .whenMatchedUpdate(set={"title": "source.title", ...})
  .whenNotMatchedInsert(values={"movie_id": "source.movie_id", ...})
  .execute())

# silver_genre_bridge — keyed on (movie_id, genre_id)
# silver_genres — keyed on genre_id
```

Each `MERGE` explicitly maps every target column to its source equivalent in both `whenMatchedUpdate` and `whenNotMatchedInsert` clauses, ensuring full schema parity.

The Gold layer follows the same MERGE-based upsert pattern (keyed on `movie_id` for `dim_movies`, `date_key` for `dim_date`, and `(movie_id, date_key)` for `fact_movie_performance`) and additionally uses Delta **liquid clustering** (`CLUSTER BY`) plus post-load `OPTIMIZE` on `dim_movies` and `fact_movie_performance`.

### 3. Execution Auditing & Logging
All execution metrics are captured into a dedicated Delta table `workspace.tmdb.pipeline_execution_logs`:

| Column | Description |
| :--- | :--- |
| `log_id` | Unique UUID for the run batch |
| `layer_processed` | Processing step (e.g., `Raw-to-Bronze`, `Bronze-to-Silver`, `Silver-to-Gold`) |
| `load_type` | `all`, `full`, or `incremental` |
| `batch_id` | Batch identifier processed |
| `parameter_processed` | Source path or parameter value |
| `execution_start_time` / `execution_end_time` | Precise timestamp range |
| `status` | `SUCCESS` or `FAILURE` |
| `rows_inserted` / `rows_updated` / `rows_quarantined` | Row-level metrics |
| `error_message` | Error trace if `status == FAILURE` |

---

## 🚀 Execution & Run Instructions

The pipeline uses **Databricks Widgets** for parameterization, allowing seamless execution across standard daily batches and historical backfills without modifying notebook code. Run the notebooks in order: **Bronze first**, then **Silver**, then **Gold** — or run the master orchestration notebook to execute all three stages sequentially.

### Notebook 1: `bronze/bronze_injestion`

| Widget | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `load_type` | Dropdown | `all` | `all` (full + incremental), `full` (historical backfill only), or `incremental` (daily changes only) |
| `source_path` | Text | *(blank)* | Folder or single `.json` file path in the UC Volume (blank = auto-detect from `/Volumes/workspace/tmdb/raw/`) |
| `batch_id` | Text | *(blank)* | Batch identifier (blank = auto-generated timestamp `YYYYMMDD_HHMMSS`) |

### Notebook 2: `silver/silver_injestion`

| Widget | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `batch_id` | Text | *(blank)* | Bronze batch ID to process (blank = process **all** Bronze records) |

---

### Notebook 3: `gold/gold_transformation`

No widgets — always transforms **all** Silver rows into the Gold star schema (idempotent MERGEs make re-runs safe). Run after `silver_injestion`.

### Notebook 4: `00_master_pipeline` (Master Orchestration)

Runs the entire pipeline end-to-end — `bronze/bronze_injestion` → `silver/silver_injestion` → `gold/gold_transformation` — sequentially via `dbutils.notebook.run()`, with per-stage timing and clear failure reporting (aborts on the first failing stage).

| Widget | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `load_type` | Text | `incremental` | `full` or `incremental` (validated); passed to every stage |
| `batch_id` | Text | *(blank)* | Passed to every stage (blank = all batches) |

### Option A: Incremental Load (Daily Run)

Run fresh data from the TMDB trending and changed-movies endpoints through both layers:

1. **Bronze** — Set `load_type` to `incremental` (or `all`), leave `source_path` blank for auto-detect, and leave `batch_id` blank for an auto-generated ID.
2. **Silver** — Set `batch_id` to the Bronze batch ID printed in step 1, or leave blank to process all Bronze records (idempotent MERGE handles duplicates).

### Option B: Historical Backfill

Re-process or load historical multi-year movie discovery data:

1. **Bronze** — Set `load_type` to `full` (or `all`), and optionally set `source_path` to a specific historical JSON file or folder under `/Volumes/workspace/tmdb/raw/full_load/`.
2. **Silver** — Leave `batch_id` blank to process all Bronze records, or specify the Bronze batch ID from step 1 to target only the newly ingested batch.

### Option C: Full Pipeline (Both Incremental + Backfill)

1. **Bronze** — Set `load_type` to `all` to process both full-load and incremental JSON files in a single run.
2. **Silver** — Leave `batch_id` blank to transform everything from Bronze into Silver.

> **Idempotency:** All Silver writes use Delta `MERGE INTO` with upsert logic, so re-running either notebook on the same batch is safe — existing records are updated, new records are inserted, and no duplicates are created.

### Option D: One-Command End-to-End Run (Master Orchestration)

Run `00_master_pipeline` at the project root to execute all three stages sequentially without opening each notebook:

1. Leave the defaults (`load_type` = `incremental`, `batch_id` blank) for a daily-style run, or set `load_type` = `full` for a historical backfill pass.
2. Each stage is executed via `dbutils.notebook.run()` (3600s timeout per stage) with both parameters passed down; the run aborts on the first failing stage and reports which stage failed.

---

## 📁 Repository Structure

```text
TMDB-Data-Engineering-Project/
├── readme.md                              # Project documentation and data models
├── 00_master_pipeline                      # Master orchestration — runs Bronze → Silver → Gold sequentially
├── test_connection                        # Databricks workspace connectivity test notebook
├── .gitignore
├── bronze/
│   └── bronze_injestion                   # Raw JSON → Bronze Delta (schema-validated, audited)
├── silver/
│   └── silver_injestion                    # Bronze → Silver Delta (parse, cast, dedupe, MERGE)
├── gold/
│   └── gold_transformation                 # Silver → Gold star schema (dim_movies, dim_date, fact_movie_performance)
├── log/
│   └── 00_audit_logging                   # Audit framework & pipeline_execution_logs table
├── staging/
│   ├── full_load.json                     # Full-load API payload (historical multi-year)
│   ├── full_load_2005_2014.json           # Historical backfill payload (2005–2014)
│   └── incremental_load.json              # Incremental API payload (trending + changed)
└── data_retrieval_scripts/
    ├── tmdb_pullsamples.py                # Python script for year-split & enriched API extraction
    └── more_data                          # Additional data retrieval notebook
```