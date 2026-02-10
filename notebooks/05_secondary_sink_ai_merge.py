# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Secondary Sink: `foreachBatch` + `ai_query` + `MERGE`
# MAGIC
# MAGIC This notebook adds a second streaming sink for demo purposes.
# MAGIC
# MAGIC It demonstrates:
# MAGIC 1. Reading from the first pipeline's audit table as a stream.
# MAGIC 2. Using `foreachBatch` for per-batch business logic.
# MAGIC 3. Enriching rows with Databricks SQL `ai_query`.
# MAGIC 4. Upserting the latest facts into a curated Delta target via `MERGE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load shared config

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pin session context

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime parameters
# MAGIC
# MAGIC These values are printed so you can quickly narrate what the stream is reading,
# MAGIC where it writes, and which endpoint is used for AI enrichment.

# COMMAND ----------

SOURCE_TABLE = TABLES["profile_memory_audit"]
TARGET_TABLE = TABLES["ai_enriched_current"]
SECONDARY_CHECKPOINT = f"{CHECKPOINT_PATH}_secondary_ai_merge"
QUERY_NAME = f"profile_memory_secondary_ai_{CHECKPOINT_SUFFIX}"

print(f"source table      = {SOURCE_TABLE}")
print(f"target table      = {TARGET_TABLE}")
print(f"checkpoint path   = {SECONDARY_CHECKPOINT}")
print(f"trigger mode      = {TRIGGER_MODE}")
print(f"llm endpoint      = {LLM_ENDPOINT}")
print(f"query name        = {QUERY_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions
# MAGIC
# MAGIC We keep helper functions small and explicit so this is easy to walk through live.

# COMMAND ----------

def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _latest_per_user_key(candidate_view: str) -> DataFrame:
    return spark.sql(
        f"""
SELECT
  user_id,
  `key`,
  kind,
  value,
  confidence,
  emission_id,
  emission_ts
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id, `key`
      ORDER BY emission_ts DESC, emission_id DESC
    ) AS rn
  FROM {candidate_view}
)
WHERE rn = 1
"""
    )


def _enrich_with_ai_query(latest_view: str) -> tuple[DataFrame, str]:
    endpoint = _sql_escape(LLM_ENDPOINT)
    try:
        enriched = spark.sql(
            f"""
SELECT
  user_id,
  `key`,
  kind,
  value,
  confidence,
  ai_query(
    '{endpoint}',
    CONCAT(
      'Summarize this profile fact in <= 12 words. ',
      'Key=', `key`, '; Kind=', kind, '; Value=', value
    )
  ) AS ai_summary,
  emission_id AS source_emission_id,
  emission_ts AS source_emission_ts,
  current_timestamp() AS updated_at
FROM {latest_view}
"""
        )
        return enriched, "ai_query"
    except Exception as ai_error:
        print(f"[secondary] ai_query unavailable; using fallback summary. error={ai_error}")
        fallback = spark.sql(
            f"""
SELECT
  user_id,
  `key`,
  kind,
  value,
  confidence,
  CONCAT('fact: ', `key`, ' = ', value) AS ai_summary,
  emission_id AS source_emission_id,
  emission_ts AS source_emission_ts,
  current_timestamp() AS updated_at
FROM {latest_view}
"""
        )
        return fallback, "fallback"


# COMMAND ----------

# MAGIC %md
# MAGIC ## `foreachBatch` logic
# MAGIC
# MAGIC Batch flow:
# MAGIC 1. Filter non-deleted, non-system rows from audit.
# MAGIC 2. Keep latest row per `(user_id, key)` in the micro-batch.
# MAGIC 3. Enrich with `ai_query` (fallback if unavailable).
# MAGIC 4. `MERGE` into curated target table.

# COMMAND ----------

def enrich_and_merge(batch_df: DataFrame, batch_id: int) -> None:
    print(f"[secondary] processing batch_id={batch_id}")
    if batch_df.isEmpty():
        print("[secondary] empty batch; skipping")
        return

    candidate_df = (
        batch_df.filter(F.col("action") != "deleted")
        .filter(~F.col("key").startswith("__"))
        .select("user_id", "key", "kind", "value", "confidence", "emission_id", "emission_ts")
    )
    if candidate_df.isEmpty():
        print("[secondary] no eligible rows after filters; skipping")
        return

    candidate_view = f"secondary_ai_input_{batch_id}"
    latest_view = f"secondary_ai_latest_{batch_id}"
    upsert_view = f"secondary_ai_upserts_{batch_id}"

    candidate_df.createOrReplaceTempView(candidate_view)
    latest_df = _latest_per_user_key(candidate_view)
    latest_df.createOrReplaceTempView(latest_view)

    enriched_df, enrich_mode = _enrich_with_ai_query(latest_view)
    enriched_df.createOrReplaceTempView(upsert_view)

    spark.sql(
        f"""
MERGE INTO {TARGET_TABLE} AS t
USING {upsert_view} AS s
ON t.user_id = s.user_id AND t.`key` = s.`key`
WHEN MATCHED THEN UPDATE SET
  t.kind = s.kind,
  t.value = s.value,
  t.confidence = s.confidence,
  t.ai_summary = s.ai_summary,
  t.source_emission_id = s.source_emission_id,
  t.source_emission_ts = s.source_emission_ts,
  t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
  user_id,
  `key`,
  kind,
  value,
  confidence,
  ai_summary,
  source_emission_id,
  source_emission_ts,
  updated_at
) VALUES (
  s.user_id,
  s.`key`,
  s.kind,
  s.value,
  s.confidence,
  s.ai_summary,
  s.source_emission_id,
  s.source_emission_ts,
  s.updated_at
)
"""
    )
    print(f"[secondary] merged batch_id={batch_id} (enrichment={enrich_mode})")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Start stream

# COMMAND ----------

stream = spark.readStream.table(SOURCE_TABLE)
writer = (
    stream.writeStream.foreachBatch(enrich_and_merge)
    .queryName(QUERY_NAME)
    .option("checkpointLocation", SECONDARY_CHECKPOINT)
)

if TRIGGER_MODE == "availableNow":
    query = writer.trigger(availableNow=True).start()
    finished = query.awaitTermination(10 * 60)
    if not finished:
        query.stop()
        raise RuntimeError("Secondary sink query did not terminate in 10 minutes.")
else:
    query = writer.trigger(processingTime=PROCESSING_TIME_TRIGGER).start()

displayHTML(
    f"""
<b>Secondary sink query started.</b><br/>
Name: {query.name}<br/>
ID: {query.id}<br/>
Status: {query.status}
"""
)

if TRIGGER_MODE != "availableNow":
    print("Streaming in processing-time mode. Stop manually when demo is complete.")
