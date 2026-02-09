# copilot-pdp-demo

Profile Memory demo using Spark `transformWithState`, RocksDB-backed state, Model Serving extraction, and append-only Delta audit output.

## Project layout

- `notebooks/00_config.py` shared widgets and constants
- `notebooks/01_setup_tables.py` schema + input/output tables + current-state view
- `notebooks/02_generate_data.py` deterministic synthetic conversation events
- `notebooks/03_profile_memory_pipeline.py` stateful streaming pipeline
- `notebooks/04_explore_results.py` demo queries and validation views
- `databricks.yml` Databricks Asset Bundle root config
- `resources/profile_memory_job.yml` multi-task job definition
- `scripts/run_demo.sh` one-command bundle validate/deploy/run

## Architecture

1. `conversation_events` receives append-only chat events.
2. `transformWithState` keeps per-user state:
   - `ListState`: recent message buffer
   - `MapState`: profile facts
   - `ValueState`: new user-message counter
   - processing-time timer for TTL-based emission
3. On threshold or timer, the pipeline calls a Model Serving endpoint and updates profile facts.
4. Emissions append to `profile_memory_audit`.
5. `v_profile_memory_current` resolves latest active fact per `user_id` + `key`.

## Prerequisites

- Databricks workspace with Unity Catalog access
- Databricks CLI installed (`databricks -v`)
- Auth configured:
  - `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, or
  - `DATABRICKS_CONFIG_PROFILE`
- Existing cluster ID for the job tasks (set as bundle variable)
- A serving endpoint (default: `databricks-meta-llama-3-1-70b-instruct`)

## Run in Databricks notebooks (manual)

Run notebooks in this order:

1. `notebooks/00_config.py`
2. `notebooks/01_setup_tables.py`
3. `notebooks/02_generate_data.py`
4. `notebooks/03_profile_memory_pipeline.py`
5. `notebooks/04_explore_results.py`

Default trigger mode is `availableNow`. For live streaming style runs, set widget `trigger_mode` to `processingTime`.

## Run from Cursor agent with Databricks Bundle

1. Copy environment template:
   - `cp .env.example .env`
2. Edit `.env`:
   - set `DATABRICKS_HOST`
   - set `DATABRICKS_TOKEN` (or `DATABRICKS_CONFIG_PROFILE`)
   - set `BUNDLE_VAR_cluster_id`
3. Execute:
   - `./scripts/run_demo.sh`

You can also choose target and job key:

- `./scripts/run_demo.sh dev profile-memory-demo`

## Direct CLI alternative

If you do not want the wrapper script:

- `databricks bundle validate -t dev`
- `databricks bundle deploy -t dev`
- `databricks bundle run profile-memory-demo -t dev`

## Expected outputs

- Table: `<catalog>.<schema>.conversation_events`
- Table: `<catalog>.<schema>.profile_memory_audit`
- View: `<catalog>.<schema>.v_profile_memory_current`

In `04_explore_results.py`, you should see:

- current profile facts for `alex_chen`
- location evolution for `sam_patel` (Brooklyn -> Austin)
- emission timeline by user and emission ID

## Rerun behavior and idempotency

- Synthetic events use deterministic `event_id`.
- Data generation inserts only new events (`left_anti` by `event_id`).
- Current-state view uses deterministic ordering:
  - `ORDER BY emission_ts DESC, emission_id DESC`
- Checkpoint path includes catalog/schema/suffix:
  - `/tmp/checkpoints/profile_memory/<catalog>_<schema>_<checkpoint_suffix>`

## Known caveats

- Processing-time timers are less meaningful in `availableNow` mode; the message threshold is the primary trigger in one-shot demos.
- If `openai` package or endpoint auth is unavailable, the pipeline falls back to a deterministic heuristic extractor.
- LLM fallback status is recorded in audit as `key='__llm_status__'` with `action='deleted'` so it does not appear in current-state view.
- Bundles will fail deployment if `BUNDLE_VAR_cluster_id` is not set to a valid cluster.

## Troubleshooting

- `bundle validate` fails on auth:
  - verify `DATABRICKS_HOST` and token/profile
- Job task fails on endpoint call:
  - confirm endpoint exists and your cluster has permission
- No output in audit table:
  - confirm input data exists and threshold reached
- State store issues:
  - verify DBR runtime supports `transformWithState` and RocksDB provider class