# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Explore Profile Memory Results
# MAGIC
# MAGIC This notebook is designed for a live demo walkthrough of both sinks:
# MAGIC - Primary sink: profile facts in audit + current view
# MAGIC - Secondary sink: AI-enriched summaries merged into a curated table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load shared config

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pin session context

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current profile for alex_chen
# MAGIC
# MAGIC Talk track: show durable preferences, identity facts, and project facts in the latest view.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT user_id, `key`, kind, value, confidence, emission_ts
FROM {TABLES["current_view"]}
WHERE user_id = 'alex_chen'
ORDER BY `key`
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit trail for sam_patel location
# MAGIC
# MAGIC Talk track: demonstrate supersede behavior (Brooklyn -> Austin) in append-only audit history.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT user_id, `key`, value, previous_value, action, emission_ts, emission_id
FROM {TABLES["profile_memory_audit"]}
WHERE user_id = 'sam_patel' AND `key` = 'location'
ORDER BY emission_ts
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## All current profiles

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT user_id, kind, COUNT(*) AS fact_count
FROM {TABLES["current_view"]}
GROUP BY user_id, kind
ORDER BY user_id, kind
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contradiction and update handling
# MAGIC
# MAGIC Talk track: review how language preference evolves over time for the same key.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT user_id, `key`, value, previous_value, action, emission_ts
FROM {TABLES["profile_memory_audit"]}
WHERE user_id = 'alex_chen' AND `key` = 'preferred_language'
ORDER BY emission_ts
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Emission timeline
# MAGIC
# MAGIC Talk track: each emission ID is a profile snapshot event for one user.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT user_id, emission_id, MIN(emission_ts) AS emission_ts, COUNT(*) AS facts_emitted
FROM {TABLES["profile_memory_audit"]}
GROUP BY user_id, emission_id
ORDER BY emission_ts, user_id
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming state metrics (if available)
# MAGIC
# MAGIC Optional: in Databricks Streaming UI, open query details for RocksDB state metrics.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secondary sink output (AI summaries)
# MAGIC
# MAGIC Talk track: this table is produced by notebook `05_secondary_sink_ai_merge.py`
# MAGIC using `foreachBatch` + `ai_query` + `MERGE`.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT user_id, `key`, value, ai_summary, confidence, source_emission_ts, updated_at
FROM {TABLES["ai_enriched_current"]}
ORDER BY updated_at DESC, user_id, `key`
"""
    )
)
