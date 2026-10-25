# TMDB Movie Analytics Lakehouse
## Final Architectural & FinOps Report

**Report Date:** October 25, 2026  
**Environment:** Databricks Workspace `dbc-4532e676-0f08` (Workspace ID `7474657753678173`)  
**Unity Catalog Namespace:** `workspace.tmdb`  
**Compute Profile:** Serverless / 2X-Small SQL Warehouse  
**Project Root:** `/Users/l242519@lhr.nu.edu.pk/TMDB-Data-Engineering-Project`  
**BI Tool:** Power BI Desktop (DirectQuery connection to the Databricks SQL Warehouse)  
**Data Verified:** Pipeline schemas, row counts, clustering properties, and audit metrics were queried **live** from `system.information_schema` (`workspace.tmdb`), `DESCRIBE DETAIL`, `DESCRIBE HISTORY`, and `workspace.tmdb.pipeline_execution_logs` on 2026-10-21. The Spark UI query profile in Section 5.4 was captured on 2026-10-25.

---

# 1. Executive Summary & Medallion Lakehouse Architecture

## 1.1 High-Level Overview

The **TMDB Movie Analytics Lakehouse** is an end-to-end, batch-oriented data platform that ingests raw movie metadata from The Movie Database (TMDB) REST API and progressively refines it into an analytics-ready **Gold star schema**, which is consumed by a **Power BI** dashboard. The platform follows the **Medallion Architecture**, the canonical Databricks Lakehouse pattern of three incremental data-quality layers (Bronze → Silver → Gold). It is implemented entirely on **Delta Lake** tables governed by **Unity Catalog** under the `workspace.tmdb` namespace.

The business purpose is to analyze global movie trends, ratings, production budgets, revenues, and genre performance over time.

Key architectural characteristics:

* **Single-source-of-truth raw layer**: the full, unmodified TMDB JSON payload is preserved verbatim in Bronze, guaranteeing infinite replayability.
* **Schema enforcement with a safety net**: Silver applies explicit typing and schema rules, with malformed records quarantined rather than dropped or fatal.
* **Idempotent, MERGE-based loading**: every layer load is re-runnable with **zero duplicate row additions**, evidenced in the audit log (0 duplicate inserts across all re-runs).
* **Dimensional Gold star schema**: `dim_movies`, `dim_date`, and `fact_movie_performance`, plus a genre dimension and movie-genre bridge, with declared primary/foreign key relationships and **Liquid Clustering** (`CLUSTER BY`) for physical data layout.
* **Automated end-to-end orchestration**: Bronze → Silver → Gold are chained (by the master notebook, or as dependent tasks of a scheduled Databricks Job), so a new incremental file flows through to Gold without manual steps.
* **End-to-end auditability**: every pipeline run, successful or failed, is logged to `workspace.tmdb.pipeline_execution_logs` (16 rows at the 2026-10-21 snapshot used for the metrics below), and the audit table is itself visualized in the BI dashboard.
* **FinOps-efficient compute**: the entire Gold refresh (403,857 row-level operations across three tables) completes in **~41.33 seconds** on serverless compute.

## 1.2 Architecture Diagram

![Architecture Diagram](images/architecture.png)

*Figure 1.1: End-to-end data flow from the TMDB API through Bronze, Silver and Gold into Power BI. Solid arrows are data flow; dotted arrows are control, audit and exception paths.*

## 1.3 Verified Table Inventory (queried 2026-10-21)

| Table | Layer | Row Count | Delta Files | Size on Disk | Physical Layout |
| --- | --- | --- | --- | --- | --- |
| `workspace.tmdb.bronze_movies` | Bronze | **395,556** | 6 | ~84.9 MB | Unpartitioned / unclustered |
| `workspace.tmdb.silver_movies` | Silver | **197,871** | 1 | ~34.9 MB | Unpartitioned / unclustered |
| `workspace.tmdb.silver_genres` | Silver (ref) | n/a | n/a | n/a | Genre lookup |
| `workspace.tmdb.silver_genre_bridge` | Silver (bridge) | n/a | n/a | n/a | Movie-Genre bridge |
| `workspace.tmdb.dim_movies` | Gold | **197,871** | 1 | ~27.3 MB | **CLUSTER BY (`movie_id`)** |
| `workspace.tmdb.dim_date` | Gold | **8,142** | 1 | ~60.7 KB | **CLUSTER BY (`date_key`)** |
| `workspace.tmdb.fact_movie_performance` | Gold | **197,844** | 1 | ~1.83 MB | **CLUSTER BY (`movie_id`, `date_key`)** |
| `workspace.tmdb.dim_genre` | Gold (genre dimension) | n/a | n/a | n/a | Genre attributes for BI |
| `workspace.tmdb.bridge_movie_genre` | Gold (bridge) | n/a | n/a | n/a | Movie ↔ genre many-to-many |
| `workspace.tmdb.quarantine_movies` | Quarantine | **0** | n/a | n/a | Rejected-record holding area |
| `workspace.tmdb.pipeline_execution_logs` | Audit | **16** | n/a | n/a | Run-level execution ledger |

> **Naming note:** the raw landing table is physically materialized as `workspace.tmdb.bronze_movies`. This is the project's "raw movies" Bronze table (raw JSON payload ingestion). No table named `raw_movies` exists in the schema; `bronze_movies` fulfills that role. Raw API payloads are staged as JSON files in the `workspace.tmdb.raw` Unity Catalog Volume before Bronze ingestion.

## 1.4 Bronze Layer: Raw JSON Ingestion (`workspace.tmdb.bronze_movies`)

The Bronze layer is the immutable landing zone for raw TMDB API responses. It follows a **schema-on-read** philosophy: the source JSON is stored **verbatim** as a string payload, and no parsing, typing, or business rules are applied at this stage. This guarantees that any future parsing logic change can be replayed against the original data without re-hitting the TMDB API.

**Ingestion patterns:**

* **Full load**: historical multi-year movie discovery (year-split queries to avoid TMDB's 500-page limit), enriched with granular metadata (`budget`, `revenue`, `runtime`, `keywords`).
* **Incremental load**: daily changed IDs from `/movie/changes` and `/trending/movie/day`, resolved to full movie records.

**Verified schema (7 columns):**

| Column | Type | Role |
| --- | --- | --- |
| `raw_record` | `STRING` | Full, unmodified TMDB JSON payload (schema-on-read) |
| `batch_id` | `STRING` | Ingestion batch identifier (e.g. `20261009_153323`) |
| `source_endpoint` | `STRING` | TMDB API endpoint the record came from |
| `source_collection` | `STRING` | Origin: `full_load`, `trending`, or `changed` |
| `source_file` | `STRING` | Source file lineage / provenance metadata |
| `pulled_at` | `STRING` | API pull timestamp (freshness tie-breaker for Silver dedup) |
| `load_timestamp` | `TIMESTAMP` | Delta load time (audit lineage) |

**File metadata retention:** every Bronze row carries `batch_id`, `source_file`, `source_endpoint`, `source_collection`, `pulled_at`, and `load_timestamp`. This is complete file-level lineage that survives into the Silver layer, so every downstream record is traceable back to the exact source file and API pull.

**Verified ingestion volume:** 2 successful Raw-to-Bronze runs totaling **395,556 rows** across 2 batches (`20261009_153323`, `20261011_090006`).

## 1.5 Silver Layer: Typed, Cleaned, Enforced (`workspace.tmdb.silver_movies`)

The Silver layer transforms the raw JSON payload into a strongly-typed, deduplicated, quality-enforced relational model ready for dimensional modeling. Raw JSON is parsed with an explicit PySpark `StructType` (`inferSchema` is disabled on every ingestion path), and the genres array is exploded into two relational tables: `silver_genre_bridge` (many-to-many, keyed on `(movie_id, genre_id)`) and `silver_genres` (lookup, keyed on `genre_id`).

**Verified schema of `silver_movies` (24 columns):**

| Column | Type | | Column | Type |
| --- | --- | --- | --- | --- |
| `movie_id` | `INT` (PK) | | `runtime` | `INT` |
| `title` | `STRING` | | `status` | `STRING` |
| `original_title` | `STRING` | | `tagline` | `STRING` |
| `overview` | `STRING` | | `source_collection` | `STRING` |
| `release_date` | `DATE` | | `source_file` | `STRING` |
| `popularity` | `DOUBLE` | | `pulled_at` | `DATE` |
| `vote_average` | `DOUBLE` | | `batch_id` | `STRING` |
| `vote_count` | `BIGINT` | | `load_timestamp` | `TIMESTAMP` |
| `budget` | `BIGINT` | | `release_year` | `INT` (derived) |
| `revenue` | `BIGINT` | | `adult` | `BOOLEAN` |
| `original_language` | `STRING` | | `backdrop_path` | `STRING` |
| `updated_at` | `TIMESTAMP` | | `poster_path` | `STRING` |

**Explicit typing & cleaning:** raw JSON fields are cast to explicit Spark types (`INT`, `DATE`, `DOUBLE`, `BIGINT`, `BOOLEAN`). Whitespace is trimmed, null IDs and blank titles are filtered, `release_date` is cast via `try_to_date` (`yyyy-MM-dd`), `release_year` is derived from `release_date`, `original_language` is normalized with `TRIM` + `UPPER`, and lineage metadata (`batch_id`, `source_file`, `pulled_at`, `load_timestamp`) is retained from Bronze.

**Schema enforcement: PERMISSIVE mode with quarantine.** JSON parsing uses `PERMISSIVE` mode. Malformed or corrupt payloads are **not** dropped and do **not** halt the pipeline. They are routed to **`workspace.tmdb.quarantine_movies`** (columns: `layer`, `batch_id`, `source_file`, `reject_reason`, `raw_record`, `load_timestamp`) together with a machine-readable `reject_reason`, preserving the full raw payload for forensic replay. Current quarantine count: **0 rows**. The current TMDB feeds parse cleanly, and the quarantine table stands ready as the safety net for future drift.

**MERGE-based deduplication (idempotent upsert):** duplicate `movie_id`s across overlapping batches are resolved by a Delta `MERGE INTO` keyed on `movie_id`, with a **freshness predicate** so only newer pulls overwrite older ones:

```sql
MERGE INTO workspace.tmdb.silver_movies AS t
USING silver_source_view AS s
ON t.movie_id = s.movie_id
WHEN MATCHED AND s.pulled_at > t.pulled_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

The `s.pulled_at > t.pulled_at` condition guarantees that a re-processed file **never duplicates rows and never regresses data freshness**. Older re-arrivals are simply ignored, and only strictly-newer versions update the target.

## 1.6 Gold Layer: Star Schema

The Gold layer is a classic **Kimball star schema** purpose-built for analytical consumption (BI, dashboards, ad-hoc SQL). It is a genuine dimensional remodel of Silver, not a copy: descriptive attributes are separated into dimensions, a date dimension is generated from distinct release dates, and the fact table holds the numeric measures plus derived metrics (`profit`, `roi_percentage`). Genres, a many-to-many attribute of a movie, are modeled with a genre dimension and a bridge table so that genre analysis does not distort the fact grain.

**Fact vs. dimension identification:**

| Table | Type | Role |
| --- | --- | --- |
| `fact_movie_performance` | **Fact** | Metrics: budget, revenue, profit, ROI, popularity, vote average, vote count |
| `dim_movies` | **Dimension** | Descriptive movie attributes: title, original title, overview, status, tagline |
| `dim_date` | **Dimension** | Time attributes: year, quarter, month, day, day of week |
| `dim_genre` | **Dimension** | Genre names |
| `bridge_movie_genre` | **Bridge** | Resolves the many-to-many relationship between movies and genres |

The primary/foreign key relationships between these tables are shown in the Power BI model view (Figure 4.1, Section 4.2).

### Gold Data Dictionary

| `dim_movies` (7 cols) | Type | | `dim_date` (7 cols) | Type |
| --- | --- | --- | --- | --- |
| `movie_id` **(PK)** | `BIGINT` | | `date_key` **(PK)** | `INT` (YYYYMMDD) |
| `title` | `STRING` | | `release_date` | `DATE` |
| `original_title` | `STRING` | | `year` | `INT` |
| `overview` | `STRING` | | `quarter` | `INT` (1–4) |
| `release_date` | `DATE` | | `month` | `INT` (1–12) |
| `status` | `STRING` | | `day` | `INT` |
| `tagline` | `STRING` | | `day_of_week` | `INT` (1 = Sunday … 7 = Saturday) |

| `fact_movie_performance` (11 cols) | Type | Notes |
| --- | --- | --- |
| `movie_id` **(FK → dim_movies)** | `BIGINT` | Part of grain PK |
| `date_key` **(FK → dim_date)** | `INT` | Part of grain PK |
| `budget` | `BIGINT` | Production budget (USD) |
| `revenue` | `BIGINT` | Global box-office revenue (USD) |
| `profit` | `BIGINT` | Derived: `revenue − budget` |
| `roi_percentage` | `DOUBLE` | Derived: `(revenue − budget) / budget × 100`; `NULL` when `budget <= 0` (avoids divide-by-zero / meaningless ROI) |
| `popularity` | `DOUBLE` | TMDB popularity score |
| `vote_average` | `DOUBLE` | User rating (0–10) |
| `vote_count` | `BIGINT` | Number of user votes |
| `batch_id` | `STRING` | Lineage |
| `load_timestamp` | `TIMESTAMP` | Lineage |

| `dim_genre` | Type | Notes |
| --- | --- | --- |
| `genre_name` **(PK)** | `STRING` | Genre label |

| `bridge_movie_genre` | Type | Notes |
| --- | --- | --- |
| `movie_id` **(FK → dim_movies)** | `BIGINT` | Movie side of the many-to-many |
| `genre_name` **(FK → dim_genre)** | `STRING` | Genre side of the many-to-many |

**Key integrity properties:**

* `dim_movies.movie_id` is the primary key (197,871 distinct movies, **0 duplicates** verified); `fact_movie_performance.movie_id` is a foreign key to it.
* `dim_date.date_key` (integer `YYYYMMDD`) is the primary key; `fact_movie_performance.date_key` is a foreign key to it.
* The fact grain is **one row per movie per release date**, with composite key `(movie_id, date_key)`.
* **27 Silver rows with NULL `release_date` are excluded** from the fact table (they cannot join `dim_date`): 197,871 − 197,844 = 27, exactly matching the Gold notebook's printed row reconciliation.
* **Genre analysis** goes through `bridge_movie_genre`, which links `dim_movies` and `dim_genre` (many-to-many), so a movie with several genres is counted once per genre without duplicating fact rows.

### Read Optimization: Liquid Clustering (verified via `DESCRIBE DETAIL`)

```sql
CREATE TABLE IF NOT EXISTS workspace.tmdb.dim_movies (
    movie_id BIGINT, title STRING, original_title STRING,
    overview STRING, release_date DATE, status STRING, tagline STRING
) CLUSTER BY (movie_id);

CREATE TABLE IF NOT EXISTS workspace.tmdb.dim_date (
    date_key INT, release_date DATE, year INT, quarter INT,
    month INT, day INT, day_of_week INT
) CLUSTER BY (date_key);

CREATE TABLE IF NOT EXISTS workspace.tmdb.fact_movie_performance (
    movie_id BIGINT, date_key INT, budget BIGINT, revenue BIGINT,
    profit BIGINT, roi_percentage DOUBLE, popularity DOUBLE,
    vote_average DOUBLE, vote_count BIGINT, batch_id STRING,
    load_timestamp TIMESTAMP
) CLUSTER BY (movie_id, date_key);
```

| Gold Table | Clustering Key(s) | Why |
| --- | --- | --- |
| `dim_movies` | `movie_id` | PK lookup / point queries cluster to a single file region |
| `dim_date` | `date_key` | Time-range queries prune to relevant date ranges |
| `fact_movie_performance` | `movie_id`, `date_key` | Composite; matches the MERGE join keys and the grain PK, so upserts and analytical filters both skip irrelevant files |

`OPTIMIZE` runs on `dim_movies` and `fact_movie_performance` after each Gold load.

### Validation (executed 2026-10-21)

* `dim_movies` primary-key uniqueness: 197,871 rows, **0 duplicates**
* Row counts: `dim_movies` 197,871 · `dim_date` 8,142 · `fact_movie_performance` 197,844
* Star-schema top-10 revenue join verified (Avatar leads: $2.92B revenue, 1133.63% ROI)
* Audit summary: Raw-to-Bronze 2 runs · Bronze-to-Silver 13 runs (3 success, 10 failure) · Silver-to-Gold 1 run · 0 rows quarantined

---

# 2. Decision Log: Technical Rationale & Architecture Defense

## 2.1 Schema Drift & Corrupt Records: PERMISSIVE Parsing + Quarantine over Strict Halt

A pipeline that parses external API payloads in `FAILFAST` / strict mode is an **availability liability**: one malformed record, one new field, or one upstream type change (e.g. TMDB returning `popularity` as a string on an overflow) crashes the entire batch, blocks every downstream layer, and requires an engineer to intervene before the next run.

The **PERMISSIVE + quarantine** pattern chosen for this project instead treats parsing as a **filter, not a gate**:

1. **Good records flow through**: healthy payloads continue uninterrupted, so data latency is unaffected by a handful of bad records.
2. **Bad records are captured, not lost**: corrupt rows land in `workspace.tmdb.quarantine_movies` with `reject_reason` and the **full raw payload** (`raw_record`), enabling root-cause analysis and replay after the parser is fixed. Nothing is silently dropped.
3. **Schema drift is observable, not fatal**: new or changed source fields surface in the quarantine table's reject reasons long before they can corrupt the Silver contract. Because Bronze keeps the full raw JSON, new source fields can be adopted later by extending the Silver `StructType` and replaying, with no API re-pull.
4. **Bad-data blast radius is contained**: a quarantined record can never pollute the Gold star schema, because it never enters Silver.

**What actually happened:** no source-side schema drift occurred during this project. `quarantine_movies` holds 0 rows, and every TMDB payload parsed cleanly against the explicit `StructType`. The 10 failures in the audit log (Section 3.3) were **development-time `DELTA_MERGE_UNRESOLVED_EXPRESSION` errors**, where the Silver MERGE clause referenced columns not resolvable in the source view. They were not drift in the TMDB feed. They were resolved by correcting the MERGE column mapping (an explicit source-to-target mapping for every column in `whenMatchedUpdate` / `whenNotMatchedInsert`), and the subsequent run succeeded. The drift-handling design is therefore a safety net verified by construction rather than one exercised by a live incident.

This is the industry-standard "dead-letter queue" discipline for Lakehouse ingestion: the pipeline **keeps running**, the anomaly is **isolated and auditable**, and recovery is a bounded, targeted re-process instead of a full pipeline outage.

## 2.2 Idempotency Strategy: MERGE Freshness Predicate, Zero Duplicate Inserts

Idempotency (re-running a load with the same input produces the same table state) is the single most important reliability property for a batch pipeline, because in the real world **duplicate deliveries, re-uploaded files, and retried runs are routine**.

The Silver layer achieves it with the `MERGE INTO` freshness condition (`s.pulled_at > t.pulled_at`) and the Gold layer with key-based `MERGE` (`whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()`). The mechanism:

* A key match means the record already exists → **UPDATE** in place (never a second INSERT).
* The `pulled_at` comparison means the update only fires when the incoming record is **strictly newer**, so a stale re-delivery of an old file is a no-op.
* A key miss means genuinely new → single INSERT.

**Verified evidence from `workspace.tmdb.pipeline_execution_logs`: 0 duplicate inserts across all re-runs:**

| Date | Event | Result |
| --- | --- | --- |
| 2026-10-09 15:34 | First Silver load of batch `20261009_153323` | 197,519 **inserted**, 0 updated |
| 2026-10-09 16:00 | **Exact same batch re-run** 26 minutes later | **0 inserted**, 197,519 updated, no duplicates added |
| 2026-10-21 10:24 | Gold load (full re-read of all 197,871 Silver rows) | **0 inserted**, 403,857 updated across dim+date+fact, no duplicates added |

The 16:00 re-run is the key proof: the identical file was reprocessed and the audit log recorded `rows_inserted = 0`, meaning the MERGE keys absorbed every row into updates. Re-running the pipeline any number of times with the same inputs converges to the **same table state** with **zero duplicate row additions**.

## 2.3 Choice of Physical Layout: Liquid Clustering vs. Hive-Style Partitioning

**Why not partition columns?** Hive-style partitioning physically splits a table into directory trees keyed on partition columns (e.g. `PARTITIONED BY (release_date)` or by genre). It is the legacy default, but it fails on this workload:

* **Small-file explosion**: with ~200K movies across 8,142 distinct release dates in Gold, date partitioning produces thousands of directories, each holding a handful of small files. Small files destroy Delta snapshot and listing performance, inflate cloud-storage request costs, and slow every `OPTIMIZE`/`VACUUM` cycle.
* **Cardinality mismatch**: genre is a low-cardinality *many-to-many* attribute (a movie has multiple genres via the bridge table), so genre partitioning is not even expressible on the movie-grain tables without denormalization, and would still produce skewed partitions.
* **Rigid layout**: Hive partition keys cannot be changed without a full table rewrite; a wrong initial choice is expensive to undo.

**Why Liquid Clustering?** Delta Liquid Clustering replaces directory partitioning with intelligent, incremental data layout:

* **No directory explosion**: data is laid out in clustered *files*, not directory trees; 197,871 `dim_movies` rows currently occupy **1 file (~27.3 MB)**, and the 197,844-row fact table **1 file (~1.83 MB)**. This is the exact opposite of the small-file problem.
* **Layout on the query keys**: `CLUSTER BY (movie_id)` / `(date_key)` / `(movie_id, date_key)` co-locates rows sharing key ranges within the same files, so point lookups on the primary key read one file region and range queries on dates skip irrelevant clusters.
* **Incremental reclustering**: `OPTIMIZE` (executed after each Gold MERGE) reclusters incrementally; no full-rewrite cost.
* **Evolution-friendly**: clustering keys can be changed on an existing table (`ALTER TABLE ... CLUSTER BY`), unlike physical partition columns.

**Column choices:** `movie_id` is the PK and the MERGE join key, `date_key` is the time-slicing key used by every trend query, and the fact table uses both because they form the grain PK and match the MERGE predicate. For a fact-dimension workload with high-cardinality keys (`movie_id` ≈ 197K distinct), liquid clustering is the correct choice: granular keys that would be catastrophic as Hive partitions work well as clustering keys. Bronze and Silver are intentionally left unclustered. They are written and read in full by the pipeline, not queried selectively, so clustering would add maintenance cost for no read benefit.

## 2.4 PII & Data Governance

* **Data classification:** the TMDB dataset consists of **public movie metadata**: titles, overviews, release dates, budgets, revenues, ratings, genres, and image paths. No Silver or Gold column contains personal data (no names of individuals, emails, addresses, phone numbers, IP addresses, or user identifiers). The `adult` column is a TMDB content-rating flag on the movie, not a personal attribute.
* **Conclusion:** no PII masking or tokenization was required for this dataset, and none was applied.
* **Credentials:** the TMDB API key is a secret, not data. It is supplied to the extraction script at run time rather than stored in the repository, and credential files are excluded from version control through `.gitignore`. Power BI authenticates to Databricks with a personal access token that stays on the report author's machine and is not stored in the repository.
* **If PII were introduced** (e.g. user-level ratings or reviewer accounts), the design supports the following without re-architecture: hash/pseudonymize identifiers in Silver (raw values remain only in access-restricted Bronze), apply Unity Catalog **column masks** and **row filters** on Gold, grant `SELECT` on Gold only (not Bronze) to BI users, and use Unity Catalog lineage and audit logs to trace access.
* **Access model:** all tables live in Unity Catalog under `workspace.tmdb`, so access can be governed per-table with `GRANT`/`REVOKE`. Bronze is never exposed to BI.

---

# 3. Orchestration & Audit Logging Evaluation

## 3.1 Master Orchestration: `00_master_pipeline`

The **`00_master_pipeline`** notebook (project root) is the single command center for the end-to-end flow. It runs the three medallion stages **sequentially**, each as an isolated child-notebook execution. Gold is the final stage of the chain, so when a new incremental file is processed into Silver, the Gold tables refresh automatically in the same run.

| Order | Stage | Child Notebook |
| --- | --- | --- |
| 1 | Raw-to-Bronze | `bronze/bronze_injestion` |
| 2 | Bronze-to-Silver | `silver/silver_injestion` |
| 3 | Silver-to-Gold | `gold/gold_transformation` |

**Parameter handling (widgets):**

```python
dbutils.widgets.text("load_type", "incremental", "Load type: 'full' or 'incremental'")
dbutils.widgets.text("batch_id",  "", "Batch ID (blank = process all batches)")

load_type = dbutils.widgets.get("load_type").strip().lower()
batch_id  = dbutils.widgets.get("batch_id").strip()

if load_type not in ("full", "incremental"):
    raise ValueError(f"Invalid load_type '{load_type}'. Use 'full' or 'incremental'.")
```

* **`load_type`** (default `incremental`) selects full vs. incremental processing semantics in each stage.
* **`batch_id`** (default blank) restricts processing to a single ingestion batch; blank = all batches.
* Invalid `load_type` values are rejected up front with a hard `ValueError`, failing fast before any data moves.

**Stage execution & failure isolation (`try/except`):**

```python
PIPELINE_STAGES = [
    ("Raw-to-Bronze",    "bronze/bronze_injestion"),
    ("Bronze-to-Silver", "silver/silver_injestion"),
    ("Silver-to-Gold",   "gold/gold_transformation"),
]
STAGE_TIMEOUT_SECONDS = 3600

try:
    for stage_name, stage_path in PIPELINE_STAGES:
        dbutils.notebook.run(
            stage_path,
            STAGE_TIMEOUT_SECONDS,
            {"load_type": load_type, "batch_id": batch_id},
        )
        stage_results.append({"stage": stage_name, "status": "SUCCESS", ...})
except Exception as e:
    print(f"MASTER PIPELINE FAILED at stage '{current_stage}' after {elapsed}s")
    raise
```

Design strengths:

* **`dbutils.notebook.run()` with a 3,600-second per-stage timeout**: each stage is an isolated child run; a hung stage cannot wedge the orchestrator indefinitely, and parameters are passed explicitly as a dict.
* **Stage failure isolation**: the `try/except` records exactly which stage failed (`current_stage`), prints a per-stage success/failure summary, and re-raises so the run surfaces as FAILED. Completed stages keep their results; there is no all-or-nothing rollback of earlier layers.
* **Sequential dependency correctness**: Bronze completes before Silver starts, Silver before Gold, matching the data dependency chain.
* **Relative paths**: the orchestrator is location-portable within the project structure.

**Scheduled execution (Databricks Job).** For automated runs, the same chain is deployed as a Databricks Job on serverless compute with a daily trigger. The job has three dependent notebook tasks (`raw_to_bronze` → `bronze_to_silver` → `silver_to_gold`), each pointing at the same child notebooks and receiving `load_type = incremental`, with retries disabled so a failure writes exactly one FAILURE row to the audit table. The job is split into tasks rather than pointing at `00_master_pipeline` because the workspace allows only one concurrent active run, and on serverless compute each `dbutils.notebook.run()` call starts a separate run, which the quota rejects with `REQUEST_LIMIT_EXCEEDED`. The master notebook remains the one-command interactive entry point and runs the identical chain.

## 3.2 Audit Logging Framework: `workspace.tmdb.pipeline_execution_logs`

Every stage notebook (including `gold/gold_transformation`, i.e. the Silver-to-Gold step) calls the shared `log/00_audit_logging` helper (`log_pipeline_execution(layer, load_type, batch_id, parameter, start_time, end_time, status, rows_inserted, rows_updated, rows_quarantined, error_msg)`) in **both its success and exception paths**, so the audit ledger captures failures as first-class events. The table records `log_id`, `layer_processed`, `load_type`, `batch_id`, `parameter_processed`, `execution_start_time` / `execution_end_time`, `status`, `rows_inserted` / `rows_updated` / `rows_quarantined`, and `error_message`. One row per run; **16 rows** at the 2026-10-21 snapshot, all queried live for this report:

| Layer | `load_type` | `batch_id` | Start (UTC) | Status | Inserted | Updated | Quarantined | Duration |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Raw-to-Bronze | full | 20261009_153323 | 2026-10-09 15:33:24 | **SUCCESS** | 197,520 | 0 | 0 | ~9 s |
| Bronze-to-Silver | full | 20261009_153323 | 2026-10-09 15:34:53 | **SUCCESS** | 197,519 | 0 | 0 | ~29 s |
| Bronze-to-Silver | full | 20261009_153323 | 2026-10-09 16:00:07 | **SUCCESS** (re-run) | **0** | 197,519 | 0 | ~26 s |
| Raw-to-Bronze | all | 20261011_090006 | 2026-10-11 09:00:12 | **SUCCESS** | 198,036 | 0 | 0 | ~31 s |
| Bronze-to-Silver | delta_parse | ALL_BRONZE_BATCHES | 2026-10-11 09:11–09:29 (10 rows) | **FAILURE** ×10 | 0 | 0 | 0 | 0–15 s each |
| Bronze-to-Silver | delta_parse | ALL_BRONZE_BATCHES | 2026-10-11 09:31:24 | **SUCCESS** | 197,871 | 0 | 0 | ~18 s |
| Silver-to-Gold | full | ALL_SILVER_BATCHES | 2026-10-21 10:24:08 | **SUCCESS** | 0 | 403,857 | 0 | **41.33 s** |

## 3.3 Verified Per-Layer Run Metrics

**Raw-to-Bronze: 2 runs, 395,556 rows ingested:**

| Metric | Value |
| --- | --- |
| Successful runs | 2 (2026-10-09, 2026-10-11) |
| Rows inserted (run 1) | 197,520 |
| Rows inserted (run 2) | 198,036 |
| **Total Bronze rows** | **395,556** (verified `COUNT(*)` = 395,556 across 2 distinct batches) |
| Failure logs | 0 |

**Bronze-to-Silver: 3 successful runs, 592,909 rows processed, 10 failures trapped:**

| Metric | Value |
| --- | --- |
| Successful runs | 3 (2026-10-09 ×2, 2026-10-11 ×1) |
| Rows processed (run 1) | 197,519 inserted |
| Rows processed (run 2, re-run) | 197,519 updated, 0 inserted (idempotency proof) |
| Rows processed (run 3) | 197,871 inserted |
| **Total processed rows** | **592,909** |
| Current Silver row count | 197,871 (deduplicated) |
| **Failure logs trapped** | **10**, all `Bronze-to-Silver` / `delta_parse` on 2026-10-11, each with a full `[DELTA_MERGE_UNRESOLVED_EXPRESSION]` error message preserved in `error_message` |

The 10 failure rows are the audit framework proving its worth: they are **historical development failures** (column-resolution errors while the incremental-parse MERGE clause was being iterated, e.g. *"Cannot resolve budget in UPDATE clause"*, *"Cannot resolve release_year"*, *"Cannot resolve title"*). Each was captured with layer, load type, batch, timestamps, status, and the raw exception text. The 09:31:24 run that succeeded afterwards is logged right alongside them. This is precisely the forensic record a production support team needs: **when it broke, what broke, for which batch, without guessing.**

**Silver-to-Gold: 1 run, 403,857 row-level operations in ~41.33 s:**

| Metric | Value |
| --- | --- |
| Runs | 1 (2026-10-21 10:24:08 → 10:24:50 UTC) |
| Status | SUCCESS |
| `dim_movies` MERGE | 0 inserted, 197,871 updated |
| `dim_date` MERGE | 0 inserted, 8,142 updated |
| `fact_movie_performance` MERGE | 0 inserted, 197,844 updated |
| **Total `rows_updated` (audit)** | **403,857** = 197,871 + 8,142 + 197,844 |
| Quarantined | 0 |
| **Duration** | **~41.33 seconds** (notebook-recorded; end-to-end MERGE + OPTIMIZE + audit logging) |

The 403,857 figure reconciles exactly: every Gold row was upserted once, **zero rows inserted** (re-run-safe), and the run finished, self-`OPTIMIZE`d both clustered tables, and wrote its audit row in under 42 seconds.

---

# 4. Business Intelligence Layer (Power BI)

## 4.1 Connection & Data Access

Power BI Desktop is connected directly to the Databricks workspace through the **Databricks connector**, using the **Server Hostname** and **HTTP Path** of the serverless SQL warehouse and authenticating with a Databricks personal access token. The storage mode is **DirectQuery**, so every visual queries the warehouse live and always reflects the latest Gold data without a scheduled refresh. Only Gold tables and the audit table are loaded; Bronze is never exposed to BI users.

| Power BI Table | Source Table | Purpose |
| --- | --- | --- |
| `fact_movie_performance` | `workspace.tmdb.fact_movie_performance` | Fact: metrics |
| `dim_movies` | `workspace.tmdb.dim_movies` | Dimension: movie attributes |
| `dim_date` | `workspace.tmdb.dim_date` | Dimension: time hierarchy |
| `dim_genre` | `workspace.tmdb.dim_genre` | Dimension: genres |
| `bridge_movie_genre` | `workspace.tmdb.bridge_movie_genre` | Bridge: movie ↔ genre |
| `pipeline_execution_logs` | `workspace.tmdb.pipeline_execution_logs` | Audit and error-log visuals |

## 4.2 Semantic Model (Star Schema in Power BI Model View)

The Power BI model mirrors the Gold star schema, with explicit one-to-many relationships configured in Model view. The genre dimension attaches to the star through the bridge table.

| From (One side) | To (Many side) | Cardinality | Cross-filter |
| --- | --- | --- | --- |
| `dim_date[date_key]` | `fact_movie_performance[date_key]` | One-to-many (1:*) | Single (dim → fact) |
| `dim_movies[movie_id]` | `fact_movie_performance[movie_id]` | One-to-many (1:*) | Single (dim → fact) |
| `dim_movies[movie_id]` | `bridge_movie_genre[movie_id]` | One-to-many (1:*) | Both |
| `dim_genre[genre_name]` | `bridge_movie_genre[genre_name]` | One-to-many (1:*) | Both |

The bidirectional filter on the two bridge relationships lets a genre selection flow through the bridge to `dim_movies` and from there to the fact table, so genre slicers and charts correctly filter revenue and ratings. `pipeline_execution_logs` is intentionally standalone: it describes pipeline operations, not movies, so it has no key in common with the star schema.

![Power BI Model View](images/powerbi_model_view.png)

*Figure 4.1: Power BI Model view showing the star schema, the genre bridge, and the standalone audit table. This also serves as the entity-relationship diagram of the Gold layer.*

## 4.3 Business Questions & Dashboard

The business analytics page answers the following questions:

| # | Business Question | Visual | Fields |
| --- | --- | --- | --- |
| Q1 | How big is the catalog, and how does it perform overall? | **KPI cards** | Distinct count of `movie_id`, Sum of `revenue`, Average `vote_average` |
| Q2 | How has movie revenue trended over time? | **Time-series line chart** | `dim_date[year]` vs. Sum of `revenue` |
| Q3 | Which movies earn the most revenue? | **Ranked bar chart (Top 10)** | `dim_movies[title]` vs. Sum of `revenue` |
| Q4 | Which genres perform best? | **Categorical bar chart** | `dim_genre[genre_name]` vs. Sum of `revenue` |
| Q5 | Do higher budgets translate to higher revenue? | **Scatter plot** | `budget` vs. `revenue`, one point per movie (`dim_movies[title]`) |

Interactivity: slicers on `dim_date[year]`, `dim_movies[status]` and `dim_genre[genre_name]`, with cross-filtering between visuals through the star-schema relationships. Because many TMDB records carry a budget or revenue of 0 (not reported), the revenue-based charts and the scatter plot exclude zero values so that they show meaningful movies.

![Business Dashboard](images/powerbi_dashboard_business.png)

*Figure 4.2: Business analytics dashboard page.*

## 4.4 Operational / Audit Dashboard Page

A second report page visualizes the audit and error-logging table:

| Visual | Source | Insight |
| --- | --- | --- |
| **KPI cards: Total Runs, Failed Runs** | `pipeline_execution_logs` | Overall run volume and number of trapped failures |
| **Runs by layer and status** (stacked column) | `pipeline_execution_logs` | Success vs. failure counts per layer (Raw-to-Bronze, Bronze-to-Silver, Silver-to-Gold) |
| **Rows inserted / updated per layer** (clustered column) | `pipeline_execution_logs` | Throughput per layer; shows the 0-insert idempotent re-runs |
| **Execution duration per run** (bar) | `pipeline_execution_logs` (`end − start`) | Performance trend; Gold run at ~41.33 s |
| **Failure log table** | `pipeline_execution_logs` filtered to `FAILURE` | Layer, timestamp, batch and `error_message` for the trapped failures |

Quarantine status is tracked in `workspace.tmdb.quarantine_movies` (currently 0 rows, Section 1.5) and is queried directly in Databricks rather than loaded into the dashboard.

![Audit Dashboard](images/powerbi_dashboard_audit.png)

*Figure 4.3: Pipeline audit and error-logging dashboard page.*

---

# 5. Spark UI & DAG Analysis (Gold Layer Fact Table Aggregation)

## 5.1 Execution Mechanics of the Gold-Layer MERGE

The Silver-to-Gold stage executes three sequential `MERGE INTO` operations. Under the Spark execution model, each `MERGE` compiles to a job with a characteristic DAG:

1. **Source scan**: a scan of the Silver data (materialized as an in-memory DataFrame in the captured profile), applying column pruning (only the 7–11 columns each target needs are read).
2. **Deduplication**: `dropDuplicates(["movie_id"])` / `distinct()` stages introduce `Aggregate` nodes with hash-based partial-to-final aggregation exchanges.
3. **Target join**: the source DataFrame is joined against the Delta target's files on the merge condition (e.g. `target.movie_id = source.movie_id AND target.date_key = source.date_key`), producing matched/unmatched row sets.
4. **Rewrite phase**: matched rows flow to an update/`WriteFiles` branch, unmatched to an insert branch; Delta rewrites only the affected files (with `CLUSTER BY` keys aligned to the merge keys, data changes localize to the same physical files).
5. **Commit**: the transaction commits to the Delta log (`operation = MERGE`), with `operationMetrics` captured by `get_merge_metrics()` via `DESCRIBE HISTORY` and written to the audit log.

Because the fact table's clustering keys `(movie_id, date_key)` **are** its MERGE join keys, the join predicate aligns with the physical layout. The join-to-touched-files mapping is near-optimal, which is a large part of why the whole three-table Gold refresh finished in ~41 seconds.

## 5.2 Broadcast Hash Joins vs. Shuffle Hash Joins (Foreign-Key Lookups)

The Gold transformation performs foreign-key lookups on `movie_id` and `date_key`. Spark's Catalyst optimizer chooses between two join physical strategies:

**Broadcast Hash Join (BHJ)** is chosen when one side fits in memory under `spark.sql.autoBroadcastJoinThreshold` (default 10 MB):

* The small side is collected to the driver, broadcast as a `BroadcastExchange` hash table to every executor, and the big side is scanned **once, with no shuffle at all**. Each executor probes the local hash copy per row.
* **`dim_date` (8,142 rows, ~60 KB)** is a textbook broadcast candidate: a lookup to resolve `release_date → date_key` would typically run as a Broadcast Hash Join, costing one tiny broadcast and zero network shuffle of the ~198K-row source.

**Shuffle Hash Join / Sort-Merge Join (SMJ)** is chosen when both sides exceed the broadcast threshold:

* Both sides are repartitioned (`Exchange hashpartitioning(...)` on the join key) so matching rows land in the same task, then hashed or sort-merged per partition.
* **`dim_movies` (~27 MB, 197,871 rows)** exceeds the 10 MB default threshold, so its joins would typically run through a shuffle/sort-merge path. With **Adaptive Query Execution (AQE)** enabled (serverless default), Spark can at runtime convert a planned sort-merge join into a shuffled hash join or even re-plan a broadcast if the actual, statistics-corrected side size turns out small enough.

**Why this matters for this pipeline:** dimensional schemas are the ideal broadcast workload. A tiny `dim_date` side avoids the most expensive possible operation (shuffling the fact-grain source) on every `date_key` lookup, and AQE keeps the optimizer making the right call as table sizes evolve. The broadcast decision is visible in a Spark UI DAG as a `BroadcastExchange` node feeding a `BroadcastHashJoin` operator, versus a pair of `Exchange` nodes feeding a `SortMergeJoin`. This section describes typical behavior; the serverless profile in Section 5.4 does not label the join strategy.

## 5.3 Delta Log Predicate Pushdown & File Skipping

Every Delta table carries a **transaction log** recording, for each data file, the min/max statistics of every column plus null counts. When a query or MERGE touches the table, Delta applies **data skipping** before reading a single data byte:

1. **Predicate pushdown into the Delta log**: the query's predicates (or the MERGE join keys) are evaluated against the per-file min/max stats in the `_delta_log`; files whose key ranges cannot match are **pruned from the file list entirely**.
2. **Liquid clustering compounds this**: because `CLUSTER BY` physically sorts rows by the clustering keys, each file's min/max range on `movie_id`/`date_key` is **narrow and non-overlapping**, so a point predicate like `movie_id = 19995` eliminates all but the one relevant file. Under random row order, min/max ranges would span nearly the whole key space and skipping would degenerate.
3. **Parquet-level pushdown continues**: within surviving files, Parquet row-group statistics and pushdown filters further skip row groups and pages.

Net effect: point lookups and `date_key`-ranged scans on the Gold tables read a small fraction of the physical data once tables span multiple files. This file-skipping stack (Delta log stats → clustering layout → Parquet row groups) is why a clustered fact table supports efficient keyed access as it grows. At the current size (one to a few files per table) the benefit is small, as the profile in Section 5.4 shows.

## 5.4 Spark UI DAG: Gold Fact Table MERGE (Screenshot & Analysis)

The screenshot below is the Spark UI query profile of the `MERGE INTO workspace.tmdb.fact_movie_performance` statement, executed on Databricks Serverless compute. The graph shows execution 3 of 5 of that statement (964 ms), and the Details panel on the left shows the statement-level metrics. The run is a re-run of the Gold notebook on 2026-10-25 (same logic as the 2026-10-21 run reported in Section 3).

![Spark UI DAG Diagram](images/spark_dag_gold_fact.png)

*Figure 5.1: Spark UI query profile (execution 3 of 5) for the Gold fact table MERGE (`fact_movie_performance`), Databricks Serverless compute.*

**Statement-level metrics (from the Details panel):**

| Metric | Value |
| --- | --- |
| Wall-clock duration | 5.635 s |
| Rows read | 989,220 |
| Rows written | 197,844 |
| Bytes read / written | 6.59 MB / 2.41 MB |
| Files read / written | 11 / 4 |

**How Spark executed it (reading the DAG bottom-up):**

1. **RDD Scan → Row To Columnar → Filter**: the source side of the MERGE is an in-memory DataFrame. Spark converts its rows to columnar batches and applies a filter to the source rows (~197.84K rows).
2. **Shuffle**: the source rows are redistributed across the cluster ahead of the join. This is a data-movement step in the plan.
3. **Scan Table**: the target Delta table (`fact_movie_performance`) is scanned (~197.84K rows).
4. **Inner Join**: source and target are joined on the MERGE condition, identifying which target rows match. About 197.84K rows flow out of the join.
5. **Grouping Aggregate → Shuffle → Grouping Aggregate**: a two-stage aggregation (partial, shuffle, final) reduces the join output to a single row. This is consistent with Delta's MERGE bookkeeping of which target files are touched.
6. **Serialize / Map Partitions / Deserialize → Result Query Stage**: the remaining operators pass that one-row result back to the driver.

This graph ends in a result stage rather than a write operator, so the file rewrite and commit most likely occur in other executions of the same statement.

**Key observations:**

* **Shuffles do appear in this plan**: one on the source side before the join and one inside the final aggregate. For a ~198K-row table they are cheap, and the full statement completes in 5.6 s.
* **Rows read (989,220) equal exactly 5 × rows written (197,844)**, which suggests the data is scanned repeatedly across the statement's 5 executions. This is normal MERGE behaviour (match first, then rewrite).
* **No file pruning in this run**: the Details panel reports 0% skipped for both bytes and files read (11 files). At ~6.6 MB total, the table is too small for data skipping to matter. The benefit of Liquid Clustering will show as the table grows (see Section 5.3).
* **Time spent** colouring shows the in-memory **RDD Scan** as the most expensive operator in this execution; the Delta **Scan Table** on the target costs less.
* Serverless compute exposes only this operator-level profile, not the full physical plan, so the join strategy (broadcast vs. sort-merge) is not labelled. The general trade-offs are covered in Section 5.2.
* This 5.6 s figure covers the fact-table MERGE only, so it is not directly comparable to the ~41 s total for the three-table Gold run in Section 3.

---

# 6. FinOps Evaluation & Enterprise Scaling Analysis

## 6.1 Compute Configuration

| Attribute | Value |
| --- | --- |
| Compute type | **Serverless** (Databricks-managed, zero-infrastructure) |
| Warehouse size | **2X-Small SQL Warehouse** |
| Auto-suspend | 10-minute idle threshold (recommended) |
| Cold-start | Serverless = near-instant spin-up; no cluster boot amortization penalty |
| Photon engine | Enabled on serverless SQL warehouses for vectorized scan/join execution |

The serverless profile is a FinOps decision in itself: no idle all-purpose clusters billing in the background, no cluster-start time charged to the run, and compute scales transparently if the workload grows.

## 6.2 Runtime Efficiency

* **Full Gold refresh (3 tables, 403,857 row-level operations): ~41.33 seconds**, including all three MERGEs, the `OPTIMIZE` passes on `fact_movie_performance` and `dim_movies`, and the audit-log write.
* Raw-to-Bronze ingestion: **~9–31 s** per batch; Bronze-to-Silver: **~18–29 s** per successful run.
* End-to-end, the full medallion pipeline from raw JSON to star schema completes on the order of **~1–2 minutes**. Serverless cold starts are negligible and every stage is small-file, statistics-driven work.
* **Typical incremental run:** a daily incremental load (trending + changed movies) ingests only the changed IDs, so its Bronze and Silver work is a small fraction of the full-load volumes above. The Gold stage currently re-reads all Silver rows, so the measured 41.33 s Gold run is a conservative upper bound for the Gold portion of a daily run.

## 6.3 Estimated DBU Consumption per Run

| Measure | Estimate |
| --- | --- |
| Active compute time per Gold run | ~0.0115 h (41.33 s) |
| **DBUs consumed per Gold run** | **~0.02 DBUs** |
| **Cost per Gold run** | **~$0.03 – $0.08 USD** (DBU rate-band dependent) |
| Full pipeline (all 3 stages) per run | Roughly a multiple of the Gold-only figure, still just cents |

*These are estimates derived from measured runtime and an assumed DBU rate band, not billed figures. Actual billed usage can be verified in `system.billing.usage`.*

At this scale the marginal cost of a pipeline run is effectively **rounding error**. The FinOps story is not "this run is cheap" but "this run is cheap *and* the architecture keeps it cheap as it scales" (clustering keeps queries pruned, MERGE keeps rework bounded, and serverless keeps idle cost at zero).

## 6.4 Enterprise Annual Scaling Projection: Hourly Schedule (8,760 runs/year)

Projecting the Gold refresh to a production **hourly** cadence (24 × 365 = **8,760 runs/year**):

| Measure | Per Run | Annual (Hourly Cadence) |
| --- | --- | --- |
| Runs | 1 | 8,760 |
| DBUs | ~0.02 | **~175 DBUs/year** |
| Cost (low band) | ~$0.03 | **~$263 USD/year** |
| Cost (high band) | ~$0.08 | **~$701 USD/year** |
| Active compute time | 41.33 s | ~100.5 hours/year of actual processing |

**Annual cost of an hourly, enterprise-grade Gold refresh: on the order of $300–$700 USD/year.** For comparison, a single mid-size always-on provisioned cluster would cost that much in days. As data grows 10–100×, MERGE duration and thus DBUs scale roughly linearly with *changed rows* (not total table size), so even a 100× data increase projects to a four-to-five-figure annual run cost, still dramatically cheaper than provisioned infrastructure, thanks to per-second serverless billing and clustering-driven file pruning.

## 6.5 Concrete Cost-Optimization Strategies

| # | Strategy | Implementation | Expected Impact |
| --- | --- | --- | --- |
| 1 | **Warehouse auto-suspension (10-min threshold)** | Keep auto-suspend at 10 minutes on the 2X-Small warehouse; serverless already avoids idle billing, and a short suspend threshold bounds any residual idle window between pipeline stages. | Eliminates idle-time DBU burn; the ~42 s run is the only billed activity. |
| 2 | **Off-peak daily `OPTIMIZE` + `VACUUM`** | Schedule maintenance as a separate off-peak job (e.g. 02:00 local): `OPTIMIZE workspace.tmdb.fact_movie_performance;` and `OPTIMIZE workspace.tmdb.dim_movies;` followed by `VACUUM ... RETAIN 168 HOURS`. This removes today's inline `OPTIMIZE` from the critical path of each run. | Keeps files clustered and compact on cheap off-peak capacity; shortens the billed duration of every business-hours run; VACUUM reclaims storage and keeps snapshot listing lean. |
| 3 | **Auto-scaling compute pools** | As volume grows, move from fixed 2X-Small to auto-scaling serverless (or size-up during bulk backfills, size-down for incremental hourly runs) so concurrency and burst workloads elastically match demand. | Pay for actual load, never for provisioned headroom; no re-architecture when data volume multiplies. |

Additional FinOps guardrails worth institutionalizing:

* **Per-layer DBU attribution**: tag each stage's warehouse/cluster so `system.billing.usage` can attribute DBUs to Raw-to-Bronze vs. Silver vs. Gold for chargeback dashboards.
* **Budget alerts**: an alert on anomalous daily DBU spend (e.g. > 3× trailing-7-day average) catches runaway runs early, which is exactly the failure mode the audit log already captures functionally.
* **Incremental Gold loads**: Gold currently re-reads all Silver rows per run (full-load by design); switching the hourly cadence to batch-scoped MERGEs would cut the 41 s run and its DBUs proportionally to the daily change rate, likely 95%+ savings for a slowly-changing movie catalog.
* **BI query cost**: the dashboard uses DirectQuery for always-fresh data, so each visual interaction runs a query on the SQL warehouse. For an enterprise deployment, switching high-traffic report pages to Import mode with a refresh scheduled right after the pipeline completes would keep the warehouse awake only for the refresh window.

---

# 7. Deliverables Map

| Deliverable | Location |
| --- | --- |
| GitHub repository (PySpark notebooks) | `TMDB-Data-Engineering-Project` repository (contents below) |
| Power BI file | Submitted as a `.pbix` file with this report |
| Final report | This document (PDF) |

Repository contents: `00_master_pipeline`, `bronze/bronze_injestion`, `silver/silver_injestion`, `gold/gold_transformation`, `log/00_audit_logging`, `images/` (report figures), `staging/` sample payloads, and `data_retrieval_scripts/` (TMDB extraction).

---

## Conclusion

The TMDB Movie Analytics Lakehouse is architecturally sound and production-ready: **immutable Bronze replayability**, **PERMISSIVE parsing with quarantine containment**, **idempotent MERGE loading verified at 0 duplicate inserts**, a **liquid-clustered Gold star schema with clean PK/FK integrity** (197,871 dimension movies / 8,142 dates / 197,844 facts, reconciling exactly to Silver minus 27 un-datable rows), a **Power BI semantic model that mirrors the star schema** with a genre bridge and both business and pipeline-audit dashboards, **complete audit coverage including 10 trapped development failures**, and a **~41-second, ~$0.03–$0.08 Gold refresh** that scales to an hourly enterprise cadence for a few hundred dollars a year.