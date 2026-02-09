# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Profile Memory Pipeline
# MAGIC
# MAGIC This notebook runs the stateful streaming pipeline using `transformWithState` and RocksDB state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load shared config

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional
from uuid import uuid4

from pyspark.sql import Row, functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

try:
    from pyspark.sql.streaming.state import StatefulProcessor, StatefulProcessorHandle
except Exception as exc:
    raise ImportError(
        "This demo requires Spark 4.x transformWithState APIs (DBR 16.x or newer)."
    ) from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt and extraction helpers
# MAGIC
# MAGIC The extractor tries Model Serving first and falls back to a deterministic heuristic for demos.

# COMMAND ----------

KEY_RE = re.compile(r"[^a-z0-9_]+")
WHITESPACE_RE = re.compile(r"\s+")
ALLOWED_KINDS = {"preference", "identity", "project", "constraint"}
ALLOWED_ACTIONS = {"new", "updated", "unchanged", "deleted"}


def normalize_key(value: str) -> str:
    raw = (value or "").strip().lower().replace(" ", "_")
    raw = KEY_RE.sub("_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    return raw[:64]


def clip_text(value: str, max_chars: int = MAX_CONTENT_CHARS) -> str:
    text = WHITESPACE_RE.sub(" ", (value or "").strip())
    return text[:max_chars]


def get_workspace_host() -> str:
    host = os.environ.get("DATABRICKS_HOST")
    if host:
        return host.replace("https://", "").strip("/")
    return spark.conf.get("spark.databricks.workspaceUrl", "")


def get_llm_client() -> Optional["OpenAI"]:
    if OpenAI is None:
        return None

    host = get_workspace_host()
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        return None

    return OpenAI(
        base_url=f"https://{host}/serving-endpoints",
        api_key=token,
        max_retries=3,
        timeout=60.0,
    )


def system_prompt() -> str:
    return (
        "You extract durable user profile facts from chat history. "
        "Compare incoming messages with current facts and return strict JSON only.\n"
        "Schema:\n"
        "{"
        '"facts":[{"key":"snake_case","kind":"preference|identity|project|constraint",'
        '"value":"short value","confidence":0.0,"action":"new|updated|unchanged|deleted"}]'
        "}\n"
        "Rules: durable facts only, no ephemeral details, max 30 facts, keep values short."
    )


def build_user_prompt(messages: List[Dict[str, str]], current_facts: Dict[str, Dict[str, str]]) -> str:
    transcript_lines = [
        f'- {m.get("role", "user")}: {clip_text(m.get("content", ""))}' for m in messages
    ]
    return (
        "Current profile facts JSON:\n"
        f"{json.dumps(current_facts, ensure_ascii=True)}\n\n"
        "Recent transcript:\n"
        + "\n".join(transcript_lines)
    )


def try_parse_response(raw: str) -> Dict[str, List[Dict[str, object]]]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Response must be a JSON object.")
    facts = parsed.get("facts", [])
    if not isinstance(facts, list):
        raise ValueError("'facts' must be an array.")
    return parsed


def normalize_fact(fact: Dict[str, object]) -> Optional[Dict[str, object]]:
    key = normalize_key(str(fact.get("key", "")))
    kind = str(fact.get("kind", "")).strip().lower()
    action = str(fact.get("action", "")).strip().lower()
    value = clip_text(str(fact.get("value", "")), max_chars=100)

    if not key or kind not in ALLOWED_KINDS or action not in ALLOWED_ACTIONS:
        return None

    confidence = fact.get("confidence", 0.0)
    try:
        confidence = float(confidence)
    except Exception:
        confidence = 0.0
    confidence = min(1.0, max(0.0, confidence))

    return {
        "key": key,
        "kind": kind,
        "value": value,
        "confidence": confidence,
        "action": action,
    }


def heuristic_extract(messages: List[Dict[str, str]], current_facts: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    updates: Dict[str, Dict[str, object]] = {}
    joined = " ".join([m.get("content", "").lower() for m in messages if m.get("role") == "user"])

    checks = [
        ("preferred_language", "preference", "Python", "prefer python"),
        ("preferred_language", "preference", "Python and Rust", "python daily, now together with rust"),
        ("location", "identity", "San Francisco", "san francisco"),
        ("location", "identity", "Austin", "relocated to austin"),
        ("location", "identity", "Brooklyn", "live in brooklyn"),
        ("diet", "identity", "Vegan", "i am vegan"),
        ("response_style", "preference", "Concise bullet points", "concise bullet-point responses"),
    ]
    for key, kind, value, needle in checks:
        if needle in joined:
            existing = current_facts.get(key)
            action = "new" if existing is None else ("updated" if existing.get("value") != value else "unchanged")
            updates[key] = {
                "key": key,
                "kind": kind,
                "value": value,
                "confidence": 0.75,
                "action": action,
            }

    return {"facts": list(updates.values())}


def call_llm(client: Optional["OpenAI"], messages: List[Dict[str, str]], current_facts: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    if client is None:
        return heuristic_extract(messages, current_facts)

    user_prompt = build_user_prompt(messages, current_facts)
    response = client.chat.completions.create(
        model=LLM_ENDPOINT,
        messages=[
            {"role": "system", "content": system_prompt()},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
    )
    raw = (response.choices[0].message.content or "").strip()
    try:
        return try_parse_response(raw)
    except Exception:
        repair = client.chat.completions.create(
            model=LLM_ENDPOINT,
            messages=[
                {"role": "system", "content": "Return valid JSON only. Keep same semantic meaning."},
                {"role": "user", "content": raw},
            ],
            temperature=0.0,
        )
        repaired = (repair.choices[0].message.content or "").strip()
        return try_parse_response(repaired)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stateful processor
# MAGIC
# MAGIC Per-user state includes message buffer, profile facts map, and count since last emission.

# COMMAND ----------

message_struct = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("conversation_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("role", StringType(), True),
        StructField("content", StringType(), True),
    ]
)

fact_struct = StructType(
    [
        StructField("key", StringType(), False),
        StructField("kind", StringType(), False),
        StructField("value", StringType(), False),
        StructField("confidence", DoubleType(), False),
    ]
)

output_schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("emission_id", StringType(), False),
        StructField("emission_ts", TimestampType(), False),
        StructField("key", StringType(), False),
        StructField("kind", StringType(), False),
        StructField("value", StringType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("action", StringType(), False),
        StructField("previous_value", StringType(), True),
        StructField("source_event_ids", ArrayType(StringType()), True),
    ]
)


class ProfileMemoryProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.message_buffer = handle.getListState("message_buffer", message_struct)
        self.profile_facts = handle.getMapState("profile_facts", StringType(), fact_struct)
        self.new_msg_count = handle.getValueState("new_msg_count", IntegerType())
        self.llm_client = get_llm_client()

    def handleInputRows(self, key, rows, timer_values) -> Iterator[Row]:
        user_id = key[0]
        new_rows = list(rows)
        if not new_rows:
            return

        self.message_buffer.appendList(new_rows)

        all_msgs = list(self.message_buffer.get())
        if len(all_msgs) > MESSAGE_BUFFER_SIZE:
            self.message_buffer.clear()
            self.message_buffer.appendList(all_msgs[-MESSAGE_BUFFER_SIZE:])

        current_count = self.new_msg_count.getOption() or 0
        user_turns = sum(1 for r in new_rows if r["role"] == "user")
        current_count += user_turns
        self.new_msg_count.update(current_count)

        timer_values.register_processing_time_timer(
            timer_values.get_current_processing_time_in_ms() + TIMER_TTL_MS
        )

        if current_count >= EMISSION_THRESHOLD:
            yield from self._emit_profile(user_id)

    def handleExpiredTimer(self, key, timer_values, expired_timer_info) -> Iterator[Row]:
        user_id = key[0]
        current_count = self.new_msg_count.getOption() or 0
        if current_count > 0:
            yield from self._emit_profile(user_id)
        timer_values.register_processing_time_timer(
            timer_values.get_current_processing_time_in_ms() + TIMER_TTL_MS
        )

    def _emit_profile(self, user_id: str) -> Iterator[Row]:
        messages = [r.asDict(recursive=True) for r in list(self.message_buffer.get())]
        source_event_ids = [m["event_id"] for m in messages[-10:]]

        prior_facts = {k: v.asDict(recursive=True) for k, v in self.profile_facts.iterator()}
        payload = call_llm(self.llm_client, messages, prior_facts)

        emission_id = str(uuid4())
        emission_ts = datetime.now(timezone.utc).replace(tzinfo=None)

        action_by_key: Dict[str, str] = {}
        deleted_rows: List[Row] = []

        for item in payload.get("facts", [])[:MAX_FACTS_PER_EMISSION]:
            normalized = normalize_fact(item if isinstance(item, dict) else {})
            if normalized is None:
                continue

            key = normalized["key"]
            prior = prior_facts.get(key)
            prior_value = None if prior is None else prior.get("value")
            action = normalized["action"]

            if action == "deleted":
                if prior is not None:
                    self.profile_facts.remove(key)
                    deleted_rows.append(
                        Row(
                            user_id=user_id,
                            emission_id=emission_id,
                            emission_ts=emission_ts,
                            key=key,
                            kind=normalized["kind"],
                            value=prior_value or "",
                            confidence=normalized["confidence"],
                            action="deleted",
                            previous_value=prior_value,
                            source_event_ids=source_event_ids,
                        )
                    )
                continue

            if normalized["confidence"] < CONFIDENCE_THRESHOLD:
                continue

            self.profile_facts.update(
                key,
                Row(
                    key=key,
                    kind=normalized["kind"],
                    value=normalized["value"],
                    confidence=float(normalized["confidence"]),
                ),
            )

            if prior is None:
                action_by_key[key] = "new"
            elif prior.get("value") != normalized["value"]:
                action_by_key[key] = "updated"
            else:
                action_by_key[key] = "unchanged"

        for row in deleted_rows:
            yield row

        current = {k: v.asDict(recursive=True) for k, v in self.profile_facts.iterator()}
        for key, fact in current.items():
            yield Row(
                user_id=user_id,
                emission_id=emission_id,
                emission_ts=emission_ts,
                key=key,
                kind=fact["kind"],
                value=fact["value"],
                confidence=float(fact["confidence"]),
                action=action_by_key.get(key, "unchanged"),
                previous_value=None if prior_facts.get(key) is None else prior_facts[key].get("value"),
                source_event_ids=source_event_ids,
            )

        self.new_msg_count.update(0)

    def close(self) -> None:
        return

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream wiring
# MAGIC
# MAGIC This section configures RocksDB state store and executes one-shot or continuous trigger mode.

# COMMAND ----------

spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
)

source_df = spark.readStream.table(TABLES["conversation_events"])

stream_builder = (
    source_df.groupBy("user_id")
    .transformWithState(
        statefulProcessor=ProfileMemoryProcessor(),
        outputStructType=output_schema,
        outputMode="append",
        timeMode="ProcessingTime",
    )
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
)

if TRIGGER_MODE == "availableNow":
    stream_builder = stream_builder.trigger(availableNow=True)
else:
    stream_builder = stream_builder.trigger(processingTime=PROCESSING_TIME_TRIGGER)

query = stream_builder.toTable(TABLES["profile_memory_audit"])
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to look at next
# MAGIC
# MAGIC Run notebook `04_explore_results` to inspect current profile state and audit history.
