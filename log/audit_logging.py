"""Shared audit logging utilities for TMDB pipeline notebooks.
Importable on both Serverless (Spark Connect) and classic Databricks compute.
"""
import uuid
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()  #gets current active session 

# ensure catalog and schema exist before creating tables
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")  #unity catalogue ensuring hierarchy
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.tmdb")

# one row per pipeline run (per layer)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.tmdb.pipeline_execution_logs (
    log_id               STRING,
    layer_processed      STRING,
    load_type            STRING,
    batch_id             STRING,
    parameter_processed  STRING,
    execution_start_time TIMESTAMP,
    execution_end_time   TIMESTAMP,
    status               STRING,
    rows_inserted        LONG,
    rows_updated         LONG,
    rows_quarantined     LONG,
    error_message        STRING
)
USING DELTA
""")

# stores bad records that cannot be processed properly
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.tmdb.quarantine_movies (
    layer           STRING,
    batch_id        STRING,
    source_file     STRING,
    reject_reason   STRING,
    raw_record      STRING,
    load_timestamp  TIMESTAMP
)
USING DELTA
""")

#reusable function to standardise log creation
def log_pipeline_execution(layer, load_type, batch_id, parameter, start_time, end_time,
                           status, rows_inserted=0, rows_updated=0, rows_quarantined=0,
                           error_msg="NONE"):
    """Record the result of a pipeline run in the logging table."""
    row = [(
        str(uuid.uuid4()), layer, load_type, batch_id, str(parameter),
        start_time, end_time, status,
        int(rows_inserted), int(rows_updated), int(rows_quarantined),
        str(error_msg)[:2000], #prevent huge error strings from causing memory bloat or storage issues
    )]
    schema = spark.table("workspace.tmdb.pipeline_execution_logs").schema
    spark.createDataFrame(row, schema).write.format("delta").mode("append") \
        .saveAsTable("workspace.tmdb.pipeline_execution_logs")
    print(f"LOGGED {layer} | {load_type} | batch={batch_id} | {status} | "
          f"inserted={rows_inserted} updated={rows_updated} quarantined={rows_quarantined}")
