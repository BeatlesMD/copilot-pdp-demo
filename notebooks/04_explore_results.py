# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Explore Profile Memory Results
# MAGIC
# MAGIC This notebook provides demo-friendly queries to explain how memory is built and updated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load shared config

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current profile for alex_chen
# MAGIC
# MAGIC Look for durable preferences, identity facts, and project facts in the latest view.

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
# MAGIC This should show location supersede from Brooklyn to Austin across emissions.

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
# MAGIC Review how language preference evolved for alex_chen over time.

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
# MAGIC Each emission represents a profile snapshot for one user.

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
# MAGIC In Databricks, use query details in the Streaming UI for RocksDB state size metrics.

# COMMAND ----------

display(spark.sql("SHOW TABLES"))
