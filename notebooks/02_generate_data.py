# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate Synthetic Conversation Data
# MAGIC
# MAGIC This notebook loads deterministic, hand-crafted conversations for three users.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load shared config

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime, timedelta
from uuid import NAMESPACE_DNS, uuid5

from pyspark.sql import Row, functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build deterministic events
# MAGIC
# MAGIC Event IDs are deterministic so reruns only append truly new records.

# COMMAND ----------

base_ts = datetime(2026, 1, 10, 9, 0, 0)


def event_id_for(ts: datetime, conversation_id: str, user_id: str, role: str, content: str) -> str:
    seed = f"{ts.isoformat()}|{conversation_id}|{user_id}|{role}|{content}"
    return str(uuid5(NAMESPACE_DNS, seed))


def mk(ts_offset_hours: int, conversation_id: str, user_id: str, role: str, content: str) -> Row:
    ts = base_ts + timedelta(hours=ts_offset_hours)
    return Row(
        event_id=event_id_for(ts, conversation_id, user_id, role, content),
        ts=ts,
        conversation_id=conversation_id,
        user_id=user_id,
        role=role,
        content=content,
    )


events = [
    # alex_chen conv 1 (8 user turns + assistant turns)
    mk(0, "alex_conv_1", "alex_chen", "user", "I am a data engineer at a fintech company."),
    mk(1, "alex_conv_1", "alex_chen", "assistant", "Nice. What stack do you use most?"),
    mk(2, "alex_conv_1", "alex_chen", "user", "I prefer Python for data work."),
    mk(3, "alex_conv_1", "alex_chen", "assistant", "Python is great for ETL."),
    mk(4, "alex_conv_1", "alex_chen", "user", "I am migrating legacy Hive tables to Delta Lake."),
    mk(5, "alex_conv_1", "alex_chen", "assistant", "That migration is a common path."),
    mk(6, "alex_conv_1", "alex_chen", "user", "My team uses Airflow for orchestration."),
    mk(7, "alex_conv_1", "alex_chen", "assistant", "Airflow and Delta usually pair well."),
    mk(8, "alex_conv_1", "alex_chen", "user", "I prefer dark mode IDEs."),
    mk(9, "alex_conv_1", "alex_chen", "assistant", "Dark mode noted."),
    mk(10, "alex_conv_1", "alex_chen", "user", "I am based in San Francisco."),
    mk(11, "alex_conv_1", "alex_chen", "assistant", "Thanks for sharing location."),
    mk(12, "alex_conv_1", "alex_chen", "user", "Our platform roadmap is heavy on reliability."),
    mk(13, "alex_conv_1", "alex_chen", "assistant", "Reliability focus makes sense."),
    mk(14, "alex_conv_1", "alex_chen", "user", "I also care about low-latency data access."),
    mk(15, "alex_conv_1", "alex_chen", "assistant", "Noted on latency priorities."),
    # alex_chen conv 2 (4 user turns)
    mk(72, "alex_conv_2", "alex_chen", "user", "I have been learning Rust for performance critical ETL."),
    mk(73, "alex_conv_2", "alex_chen", "assistant", "Rust can help with performance."),
    mk(74, "alex_conv_2", "alex_chen", "user", "The migration project is now complete."),
    mk(75, "alex_conv_2", "alex_chen", "assistant", "Great milestone."),
    mk(76, "alex_conv_2", "alex_chen", "user", "Can we discuss Kafka Connect options?"),
    mk(77, "alex_conv_2", "alex_chen", "assistant", "Sure, happy to help."),
    mk(78, "alex_conv_2", "alex_chen", "user", "I still use Python daily, now together with Rust."),
    mk(79, "alex_conv_2", "alex_chen", "assistant", "Good combined toolset."),
    # sam_patel conv 1 (7 user turns)
    mk(24, "sam_conv_1", "sam_patel", "user", "I live in Brooklyn."),
    mk(25, "sam_conv_1", "sam_patel", "assistant", "Brooklyn noted."),
    mk(26, "sam_conv_1", "sam_patel", "user", "I work as a product manager in fintech."),
    mk(27, "sam_conv_1", "sam_patel", "assistant", "Thanks for the context."),
    mk(28, "sam_conv_1", "sam_patel", "user", "I am planning a trip to Japan in April."),
    mk(29, "sam_conv_1", "sam_patel", "assistant", "That sounds exciting."),
    mk(30, "sam_conv_1", "sam_patel", "user", "I prefer concise bullet-point responses."),
    mk(31, "sam_conv_1", "sam_patel", "assistant", "I can keep answers concise."),
    mk(32, "sam_conv_1", "sam_patel", "user", "I have a dog named Beau."),
    mk(33, "sam_conv_1", "sam_patel", "assistant", "Beau is a great dog name."),
    mk(34, "sam_conv_1", "sam_patel", "user", "I usually decide quickly when choices are clear."),
    mk(35, "sam_conv_1", "sam_patel", "assistant", "Quick decisions noted."),
    mk(36, "sam_conv_1", "sam_patel", "user", "I value practical examples over theory."),
    mk(37, "sam_conv_1", "sam_patel", "assistant", "I can prioritize practical examples."),
    # sam_patel conv 2 (4 user turns)
    mk(96, "sam_conv_2", "sam_patel", "user", "I just relocated to Austin for my partner's job."),
    mk(97, "sam_conv_2", "sam_patel", "assistant", "Congrats on the move."),
    mk(98, "sam_conv_2", "sam_patel", "user", "The Japan trip was great."),
    mk(99, "sam_conv_2", "sam_patel", "assistant", "Glad it went well."),
    mk(100, "sam_conv_2", "sam_patel", "user", "Now I am interested in real estate investing."),
    mk(101, "sam_conv_2", "sam_patel", "assistant", "Interesting next focus."),
    mk(102, "sam_conv_2", "sam_patel", "user", "I also want neighborhood-level market data."),
    mk(103, "sam_conv_2", "sam_patel", "assistant", "We can explore data sources."),
    # jordan_lee conv 1 (10 user turns)
    mk(48, "jordan_conv_1", "jordan_lee", "user", "I am building a wedding planning app as a side project."),
    mk(49, "jordan_conv_1", "jordan_lee", "assistant", "Great project idea."),
    mk(50, "jordan_conv_1", "jordan_lee", "user", "It is built with React Native."),
    mk(51, "jordan_conv_1", "jordan_lee", "assistant", "React Native is a good fit."),
    mk(52, "jordan_conv_1", "jordan_lee", "user", "I am vegan."),
    mk(53, "jordan_conv_1", "jordan_lee", "assistant", "Thanks for sharing."),
    mk(54, "jordan_conv_1", "jordan_lee", "user", "I prefer window seats when traveling."),
    mk(55, "jordan_conv_1", "jordan_lee", "assistant", "Window seat preference noted."),
    mk(56, "jordan_conv_1", "jordan_lee", "user", "I work as a frontend engineer."),
    mk(57, "jordan_conv_1", "jordan_lee", "assistant", "Frontend background noted."),
    mk(58, "jordan_conv_1", "jordan_lee", "user", "I am getting married in October."),
    mk(59, "jordan_conv_1", "jordan_lee", "assistant", "Congratulations."),
    mk(60, "jordan_conv_1", "jordan_lee", "user", "My partner's name is Riley."),
    mk(61, "jordan_conv_1", "jordan_lee", "assistant", "Nice to meet Riley."),
    mk(62, "jordan_conv_1", "jordan_lee", "user", "I care a lot about accessibility best practices."),
    mk(63, "jordan_conv_1", "jordan_lee", "assistant", "Accessibility is essential."),
    mk(64, "jordan_conv_1", "jordan_lee", "user", "I prefer checklists for planning."),
    mk(65, "jordan_conv_1", "jordan_lee", "assistant", "Checklists can help structure work."),
    mk(66, "jordan_conv_1", "jordan_lee", "user", "I want reminders for milestones."),
    mk(67, "jordan_conv_1", "jordan_lee", "assistant", "Milestone reminders noted."),
]

events_df = spark.createDataFrame(events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write append-only input data idempotently
# MAGIC
# MAGIC We left-anti join on `event_id` so reruns avoid duplicating existing rows.

# COMMAND ----------

target = TABLES["conversation_events"]
existing = spark.table(target).select("event_id")
to_insert = events_df.alias("n").join(existing.alias("e"), on="event_id", how="left_anti")

to_insert.write.mode("append").format("delta").saveAsTable(target)

print("Rows prepared:", events_df.count())
print("Rows inserted:", to_insert.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick sanity check

# COMMAND ----------

display(
    spark.table(target)
    .groupBy("user_id", "role")
    .agg(F.count("*").alias("n"))
    .orderBy("user_id", "role")
)
