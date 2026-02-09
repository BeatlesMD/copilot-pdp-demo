# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup Delta Tables
# MAGIC
# MAGIC This notebook creates the schema, input/output tables, and the current-state view.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load shared config

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create input table: conversation events
# MAGIC
# MAGIC This is append-only source data for the streaming pipeline.

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {TABLES["conversation_events"]} (
  event_id STRING,
  ts TIMESTAMP,
  conversation_id STRING,
  user_id STRING,
  role STRING,
  content STRING
)
USING DELTA
TBLPROPERTIES (
  delta.appendOnly = true
)
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create output table: profile memory audit
# MAGIC
# MAGIC Every profile emission appends rows to this audit log.

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {TABLES["profile_memory_audit"]} (
  user_id STRING,
  emission_id STRING,
  emission_ts TIMESTAMP,
  `key` STRING,
  kind STRING,
  value STRING,
  confidence DOUBLE,
  action STRING,
  previous_value STRING,
  source_event_ids ARRAY<STRING>
)
USING DELTA
TBLPROPERTIES (
  delta.appendOnly = true
)
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create optional LLM error table
# MAGIC
# MAGIC This keeps malformed responses and parse failures for debugging.

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {TABLES["llm_errors"]} (
  user_id STRING,
  emission_id STRING,
  error_ts TIMESTAMP,
  error_type STRING,
  error_message STRING,
  raw_response STRING
)
USING DELTA
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create current profile view
# MAGIC
# MAGIC The view resolves latest active fact per `user_id` + `key` with deterministic ordering.

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {TABLES["current_view"]} AS
SELECT user_id, emission_id, emission_ts, `key`, kind, value, confidence, action, previous_value, source_event_ids
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id, `key`
      ORDER BY emission_ts DESC, emission_id DESC
    ) AS rn
  FROM {TABLES["profile_memory_audit"]}
  WHERE action != 'deleted'
)
WHERE rn = 1
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify created objects

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
