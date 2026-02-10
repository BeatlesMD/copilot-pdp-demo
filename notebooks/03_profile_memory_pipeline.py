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
import re
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Tuple
from uuid import uuid4

import pandas as pd
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
    from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact normalization helpers
# MAGIC
# MAGIC These helpers validate keys, actions, and confidence before facts enter state.


def system_prompt() -> str:
    return (
        "You extract durable profile memory facts for one user. "
        "Return strict JSON only with no extra prose.\n\n"
        "Output schema:\n"
        "{\n"
        '  "facts": [\n'
        "    {\n"
        '      "key": "snake_case_key",\n'
        '      "kind": "preference|identity|project|constraint",\n'
        '      "value": "short string under 100 chars",\n'
        '      "confidence": 0.0,\n'
        '      "action": "new|updated|unchanged|deleted",\n'
        '      "reason": "short justification"\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        "Rules:\n"
        "- Durable facts only; ignore ephemeral details.\n"
        "- Keep keys canonical snake_case.\n"
        "- Include unchanged facts when still valid.\n"
        "- If a fact contradicts existing facts, use action=updated.\n"
        "- Use action=deleted when a prior fact is no longer valid.\n"
        "- Max 30 facts."
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Heuristic extractor
# MAGIC
# MAGIC This fallback path keeps the demo functional even when an LLM is unavailable.


def heuristic_extract(messages: List[Dict[str, str]], current_facts: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    updates: Dict[str, Dict[str, object]] = {}
    joined = " ".join([m.get("content", "").lower() for m in messages if m.get("role") == "user"])

    checks = [
        ("job_role", "identity", "Data engineer", "data engineer"),
        ("job_role", "identity", "Product manager", "product manager"),
        ("job_role", "identity", "Frontend engineer", "frontend engineer"),
        ("company_domain", "identity", "Fintech", "fintech"),
        ("preferred_language", "preference", "Python", "prefer python"),
        ("preferred_language", "preference", "Python and Rust", "python daily, now together with rust"),
        ("project_migration_status", "project", "In progress", "migrating legacy hive tables to delta lake"),
        ("project_migration_status", "project", "Complete", "migration project is now complete"),
        ("orchestration_tool", "project", "Airflow", "uses airflow for orchestration"),
        ("interest_topic", "preference", "Kafka Connect", "kafka connect"),
        ("location", "identity", "San Francisco", "san francisco"),
        ("location", "identity", "Brooklyn", "live in brooklyn"),
        ("location", "identity", "Austin", "relocated to austin"),
        ("travel_plan", "project", "Japan in April", "trip to japan in april"),
        ("response_style", "preference", "Concise bullet points", "concise bullet-point responses"),
        ("pet_name", "identity", "Beau", "dog named beau"),
        ("interest_topic", "preference", "Real estate investing", "real estate investing"),
        ("side_project", "project", "Wedding planning app", "wedding planning app"),
        ("tech_stack", "project", "React Native", "react native"),
        ("diet", "identity", "Vegan", "i am vegan"),
        ("travel_seat_preference", "preference", "Window seat", "prefer window seats"),
        ("life_event", "identity", "Getting married in October", "getting married in october"),
        ("partner_name", "identity", "Riley", "partner's name is riley"),
        ("interest_topic", "preference", "Accessibility best practices", "accessibility best practices"),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM call wrapper
# MAGIC
# MAGIC Currently configured to fall back to heuristics for worker stability.


def call_llm(
    client: Optional[object], messages: List[Dict[str, str]], current_facts: Dict[str, Dict[str, object]]
) -> Tuple[Dict[str, object], Optional[str]]:
    if client is None:
        return heuristic_extract(messages, current_facts), "llm_unavailable_heuristic_fallback"

    user_prompt = build_user_prompt(messages, current_facts)
    try:
        response = client.chat.completions.create(
            model=LLM_ENDPOINT,
            messages=[
                {"role": "system", "content": system_prompt()},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
        )
        raw = (response.choices[0].message.content or "").strip()
        return try_parse_response(raw), None
    except Exception:
        try:
            repair = client.chat.completions.create(
                model=LLM_ENDPOINT,
                messages=[
                    {"role": "system", "content": "Return valid JSON only. Keep same semantic meaning."},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
            )
            repaired = (repair.choices[0].message.content or "").strip()
            return try_parse_response(repaired), "llm_primary_failed_repair_succeeded"
        except Exception:
            return heuristic_extract(messages, current_facts), "llm_failed_heuristic_fallback"

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

value_string_state_schema = StructType([StructField("value", StringType(), True)])
value_int_state_schema = StructType([StructField("value", IntegerType(), True)])


class ProfileMemoryProcessor(StatefulProcessor):
    @staticmethod
    def _value_get(state):
        raw = None
        for name in ["getOption", "get_option", "get"]:
            fn = getattr(state, name, None)
            if fn:
                raw = fn()
                break
        if raw is None:
            return None
        # ValueState in PySpark is typically a Row with one field.
        if hasattr(raw, "asDict"):
            d = raw.asDict(recursive=True)
            return d.get("value")
        if isinstance(raw, dict):
            return raw.get("value")
        if isinstance(raw, (tuple, list)) and len(raw) > 0:
            return raw[0]
        return None

    @staticmethod
    def _value_update(state, value):
        state.update((value,))

    @staticmethod
    def _load_json(raw, default):
        if raw is None or raw == "":
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.init_error = None
        try:
            # Use StructType payloads for ValueState compatibility.
            self.buffer_json_state = handle.getValueState("message_buffer_json", value_string_state_schema)
            self.facts_json_state = handle.getValueState("profile_facts_json", value_string_state_schema)
            self.new_msg_count = handle.getValueState("new_msg_count", value_int_state_schema)
        except Exception as exc:
            self.init_error = f"state_init_error: {exc}"

        # Keep processor workers network-free for runtime stability.
        self.llm_client = None

    def _error_df(self, user_id: str, message: str, source_event_ids: Optional[List[str]] = None) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "user_id": user_id,
                    "emission_id": str(uuid4()),
                    "emission_ts": datetime.now(timezone.utc).replace(tzinfo=None),
                    "key": "__processor_error__",
                    "kind": "constraint",
                    "value": clip_text(message, max_chars=500),
                    "confidence": 1.0,
                    "action": "deleted",
                    "previous_value": None,
                    "source_event_ids": source_event_ids or [],
                }
            ]
        )

    def _read_buffer(self) -> List[Dict[str, object]]:
        return self._load_json(self._value_get(self.buffer_json_state), [])

    def _write_buffer(self, buffer_rows: List[Dict[str, object]]) -> None:
        self._value_update(self.buffer_json_state, json.dumps(buffer_rows, ensure_ascii=True))

    def _read_facts(self) -> Dict[str, Dict[str, object]]:
        return self._load_json(self._value_get(self.facts_json_state), {})

    def _write_facts(self, facts: Dict[str, Dict[str, object]]) -> None:
        self._value_update(self.facts_json_state, json.dumps(facts, ensure_ascii=True))

    def handleInputRows(self, key, rows, timer_values) -> Iterator[pd.DataFrame]:
        user_id = key[0]
        new_rows: List[Dict[str, object]] = []
        try:
            if self.init_error is not None:
                yield self._error_df(user_id, self.init_error)
                return

            for pdf in rows:
                if pdf is None or pdf.empty:
                    continue
                for _, series in pdf.iterrows():
                    d = series.to_dict()
                    ts = d.get("ts")
                    if hasattr(ts, "isoformat"):
                        d["ts"] = ts.isoformat()
                    new_rows.append(d)
            if not new_rows:
                return

            buffer_rows = self._read_buffer()
            buffer_rows.extend(new_rows)
            if len(buffer_rows) > MESSAGE_BUFFER_SIZE:
                buffer_rows = buffer_rows[-MESSAGE_BUFFER_SIZE:]
            self._write_buffer(buffer_rows)

            current_count = self._value_get(self.new_msg_count) or 0
            user_turns = sum(1 for r in new_rows if r.get("role") == "user")
            self._value_update(self.new_msg_count, current_count + user_turns)

            if (current_count + user_turns) >= EMISSION_THRESHOLD:
                yield from self._emit_profile(user_id, buffer_rows)
        except Exception as exc:
            source_event_ids = [r.get("event_id") for r in new_rows[-10:] if r.get("event_id")]
            yield self._error_df(user_id, f"handleInputRows error: {exc}", source_event_ids)

    def handleExpiredTimer(self, key, timer_values, expired_timer_info) -> Iterator[pd.DataFrame]:
        # Timer-driven emission is disabled for availableNow demo runs.
        # Keep this as a generator to satisfy API expectations in PROCESS_TIMER/COMPLETE modes.
        yield from ()

    def _emit_profile(self, user_id: str, messages: List[Dict[str, object]]) -> Iterator[pd.DataFrame]:
        source_event_ids = [m["event_id"] for m in messages[-10:] if m.get("event_id")]
        prior_facts = self._read_facts()
        payload, llm_status = call_llm(self.llm_client, messages, prior_facts)

        emission_id = str(uuid4())
        emission_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        action_by_key: Dict[str, str] = {}
        deleted_rows: List[Dict[str, object]] = []
        current_facts = dict(prior_facts)

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
                    current_facts.pop(key, None)
                    deleted_rows.append(
                        {
                            "user_id": user_id,
                            "emission_id": emission_id,
                            "emission_ts": emission_ts,
                            "key": key,
                            "kind": normalized["kind"],
                            "value": prior_value or "",
                            "confidence": normalized["confidence"],
                            "action": "deleted",
                            "previous_value": prior_value,
                            "source_event_ids": source_event_ids,
                        }
                    )
                continue

            if normalized["confidence"] < CONFIDENCE_THRESHOLD:
                continue

            current_facts[key] = {
                "key": key,
                "kind": normalized["kind"],
                "value": normalized["value"],
                "confidence": float(normalized["confidence"]),
            }

            if prior is None:
                action_by_key[key] = "new"
            elif prior.get("value") != normalized["value"]:
                action_by_key[key] = "updated"
            else:
                action_by_key[key] = "unchanged"

        self._write_facts(current_facts)

        if deleted_rows:
            yield pd.DataFrame(deleted_rows)

        if llm_status is not None:
            yield pd.DataFrame(
                [
                    {
                        "user_id": user_id,
                        "emission_id": emission_id,
                        "emission_ts": emission_ts,
                        "key": "__llm_status__",
                        "kind": "constraint",
                        "value": llm_status,
                        "confidence": 1.0,
                        "action": "deleted",
                        "previous_value": None,
                        "source_event_ids": source_event_ids,
                    }
                ]
            )

        snapshot_rows: List[Dict[str, object]] = []
        for key, fact in current_facts.items():
            action = action_by_key.get(key, "unchanged")
            snapshot_rows.append(
                {
                    "user_id": user_id,
                    "emission_id": emission_id,
                    "emission_ts": emission_ts,
                    "key": key,
                    "kind": fact["kind"],
                    "value": fact["value"],
                    "confidence": float(fact["confidence"]),
                    "action": action,
                    "previous_value": (
                        prior_facts[key].get("value")
                        if action == "updated" and prior_facts.get(key) is not None
                        else None
                    ),
                    "source_event_ids": source_event_ids,
                }
            )
        if snapshot_rows:
            yield pd.DataFrame(snapshot_rows)

        self._value_update(self.new_msg_count, 0)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint handling
# MAGIC
# MAGIC For iterative debugging, you can keep checkpoint state by setting `clear_checkpoint=false`.

# COMMAND ----------

if CLEAR_CHECKPOINT:
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print("Checkpoint cleared:", CHECKPOINT_PATH)
else:
    print("Checkpoint retained:", CHECKPOINT_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build streaming query
# MAGIC
# MAGIC This cell only defines the query builder so you can inspect settings before starting it.

# COMMAND ----------

source_df = spark.readStream.table(TABLES["conversation_events"])

stream_builder = (
    source_df.groupBy("user_id")
    .transformWithStateInPandas(
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start stream (optional)
# MAGIC
# MAGIC Set widget `run_stream=false` when you want to debug notebook cells without launching streaming execution.

# COMMAND ----------

if RUN_STREAM:
    query = stream_builder.toTable(TABLES["profile_memory_audit"])
    query.awaitTermination()
else:
    print("RUN_STREAM is false; query was built but not started.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to look at next
# MAGIC
# MAGIC Run notebook `04_explore_results` to inspect current profile state and audit history.
