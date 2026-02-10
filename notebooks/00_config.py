# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuration
# MAGIC
# MAGIC This notebook defines shared parameters and constants for the profile memory demo.
# MAGIC Run this first, then `%run` it from other notebooks.

# COMMAND ----------

from typing import Dict

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC
# MAGIC Widgets let the same demo run across catalogs/schemas/endpoints without code edits.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "memory_demo", "Schema")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-1-70b-instruct", "LLM Endpoint")
dbutils.widgets.text("trigger_mode", "availableNow", "Trigger Mode (availableNow|processingTime)")
dbutils.widgets.text("checkpoint_suffix", "dev", "Checkpoint Suffix")
dbutils.widgets.dropdown("run_stream", "true", ["true", "false"], "Run Stream")
dbutils.widgets.dropdown("clear_checkpoint", "true", ["true", "false"], "Clear Checkpoint")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint").strip()
TRIGGER_MODE = dbutils.widgets.get("trigger_mode").strip()
CHECKPOINT_SUFFIX = dbutils.widgets.get("checkpoint_suffix").strip() or "dev"
RUN_STREAM = dbutils.widgets.get("run_stream").strip().lower() == "true"
CLEAR_CHECKPOINT = dbutils.widgets.get("clear_checkpoint").strip().lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared constants
# MAGIC
# MAGIC These values control buffering, emission thresholds, and prompt safety limits.

# COMMAND ----------

CHECKPOINT_PATH = f"/tmp/checkpoints/profile_memory/{CATALOG}_{SCHEMA}_{CHECKPOINT_SUFFIX}"
EMISSION_THRESHOLD = 10
TIMER_TTL_MS = 600_000
MESSAGE_BUFFER_SIZE = 20
MAX_CONTENT_CHARS = 2000
CONFIDENCE_THRESHOLD = 0.6
MAX_FACTS_PER_EMISSION = 30

PROCESSING_TIME_TRIGGER = "30 seconds"


def fq(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


TABLES: Dict[str, str] = {
    "conversation_events": fq("conversation_events"),
    "profile_memory_audit": fq("profile_memory_audit"),
    "current_view": fq("v_profile_memory_current"),
    "ai_enriched_current": fq("profile_memory_ai_current"),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Active configuration
# MAGIC
# MAGIC Confirm these values before running setup and pipeline notebooks.

# COMMAND ----------

print("CATALOG:", CATALOG)
print("SCHEMA:", SCHEMA)
print("LLM_ENDPOINT:", LLM_ENDPOINT)
print("TRIGGER_MODE:", TRIGGER_MODE)
print("CHECKPOINT_SUFFIX:", CHECKPOINT_SUFFIX)
print("RUN_STREAM:", RUN_STREAM)
print("CLEAR_CHECKPOINT:", CLEAR_CHECKPOINT)
print("CHECKPOINT_PATH:", CHECKPOINT_PATH)
print("TABLES:", TABLES)
