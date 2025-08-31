# Documentation

## Overview

This service consumes calculation requests from Kafka, looks up an entity row from a relational database (reflected “Walrus” schema), performs a lightweight computation, persists results, and emits a completion event via an outbox dispatcher. A small HTTP health endpoint reports outbox backlog.

Core modules:
- `app/consumer.py`: Kafka consumer + request handling and compute flow.
- `app/dispatcher.py`: Outbox-to-Kafka dispatcher with delivery marking.
- `app/db.py`: Engine/session setup, reflected schema access, and native tables.
- `app/health.py`: `GET /healthz` returns status and `outbox_unsent`.
- `app/config.py`: Configuration via environment variables.
- `main.py`: Wires consumer, dispatcher, and health server.

## Message Contracts

- Request (Kafka `TOPIC_REQUEST`): bytes payload with pipe-delimited fields
  - Format: `event_id|entity_type|entity_id`
  - Example: `evt-123|electric_sensor|1`

- Completion (Kafka `TOPIC_COMPLETED`): JSON object published by dispatcher
  - Fields: `event_id`, `correlation_id`, `entity_type`, `entity_id`, `result_id`, `status`, `duration_ms`, `completed_at`
  - On failures an additional reason may be included, e.g. `{ "reason": "entity_not_found" }`.

## Data Model

- Reflected tables: Loaded dynamically from the target database (see `sql/schema_definition.sql` for an example schema containing `electric_sensor`, sensors, rooms, etc.). Lookups are performed by primary key `id` via `fetch_entity_row`.
- Native tables managed by this service:
  - `calc_results(id, entity_type, entity_id, status, payload, created_at)`
  - `outbox_events(id, topic, key, payload, sent, created_at)`
  - `inbox_events(event_id)` — used for idempotent consumption.

## Computation

The current implementation is a stub that sums numeric-looking fields in the entity row and records the source columns. See `mock_compute` in `app/consumer.py`. Replace with domain logic as needed.

## Configuration

Environment variables (defaults in parentheses):
- `KAFKA_BOOTSTRAP` (`localhost:9092`): Kafka bootstrap servers.
- `KAFKA_GROUP_ID` (`ms2-calculator`): Consumer group.
- `TOPIC_REQUEST` (`calc.request`): Topic for inbound requests.
- `TOPIC_COMPLETED` (`calc.completed`): Topic for completion events.
- `DB_URL` (`postgresql+psycopg2://walrus:walrus@localhost:55432/walrus`): Database URL.
- `OUTBOX_BATCH` (`100`): Max events per dispatch pass.
- `OUTBOX_POLL_MS` (`1000`): Dispatcher poll interval in milliseconds.
- `ALLOWED_ENTITY_TABLES` (empty): Optional comma-separated allowlist of table names; if set, only these can be queried.

## Running Locally

Option A — Docker Compose (recommended):
1) Create the shared network used by the compose files:
   `docker network create walrus-net`
2) Start Postgres:
   `docker compose -f docker-compose.db.yml up -d`
   - If you want the host to access Postgres, uncomment the `ports` mapping in `docker-compose.db.yml`.
3) Configure the app (create `.env`):
   - `DB_URL=postgresql+psycopg2://walrus:walrus@walrus-db:5432/walrus`
   - `KAFKA_BOOTSTRAP=host.docker.internal:9092` (if Kafka runs on your host)
4) Build and run the service:
   `docker compose up --build`

Option B — Local Python (dev/test):
- Install requirements: `pip install -r requirements.txt`
- Ensure `DB_URL` points to a reachable DB (SQLite or Postgres) and `KAFKA_BOOTSTRAP` points to a Kafka cluster, then run: `python main.py`

## Health Endpoint

- `GET /healthz` → `{ "status": "ok", "outbox_unsent": <int> }` on success.
- Returns `{ "status": "error", "detail": "..." }` with HTTP 500 on failure.

## Logging

- Logs are line-delimited JSON via `app/logging_setup.py` using `log_json(...)` for key events.

# Test Suite Overview

This repository includes a focused, end‑to‑end test suite that validates the service’s core flow using a local SQLite database and a fake Kafka producer. The tests emphasize idempotent consumption, persistence, outbox publishing/dispatching, and operational behavior for error and health scenarios.

## Fixtures (tests/conftest.py)

- `/_set_test_env` (session, autouse): Sets `DB_URL` to a local SQLite file and clears `ALLOWED_ENTITY_TABLES` so all reflected entities are allowed by default. Cleans up the test DB file after the test session.
- `/fresh_db`: Creates/drops native service tables (`calc_results`, `outbox_events`, `inbox_events`) for a clean state per test.
- `/walrus_tables`: Creates a minimal reflected table `electric_sensor` with numeric columns (`power`, `voltage`, `energy`, `curent`).
- `/sample_row`: Inserts a single row into `electric_sensor` (`id=1`) for use by tests.
- `/fake_producer`: Monkeypatches the outbox dispatcher’s Kafka producer to capture produced messages and simulate a successful delivery that marks outbox events as sent.

## Core Test Cases (tests/test_ms2.py)

- `test_happy_path`
  - Sends a request message in the new pipe‑delimited format: `event_id|entity_type|entity_id`.
  - Verifies the consumer commits once, persists a `CalcResult` with status `SUCCESS`, enqueues an outbox event, and after a short dispatcher run, marks it as sent.
  - Asserts a `calc.completed` event was produced with `correlation_id == event_id`.

- `test_idempotency`
  - Sends the exact same message twice (same `event_id`).
  - Ensures only one `CalcResult` exists due to `InboxEvent`‑based deduplication.

- `test_entity_not_found`
  - Requests an entity that does not exist.
  - Persists a `CalcResult` with status `FAILED` and enqueues an outbox event whose payload includes `status: FAILED`.

## Additional Test Cases (tests/test_ms2_extra.py)

- `test_malformed_message_commits_and_no_side_effects`
  - Sends an invalid message with fewer than three parts.
  - Confirms the consumer commits without any DB side effects (no `CalcResult`, no `OutboxEvent`, no `InboxEvent`).

- `test_allowed_entity_tables_filter_blocks`
  - Monkeypatches `ALLOWED_ENTITY_TABLES` to exclude the target table.
  - A valid request is blocked by the filter; the service records a `FAILED` `CalcResult` and still enqueues a completion outbox event.

- `test_delivery_error_keeps_outbox_unsent`
  - Produces a valid outbox event, then replaces the producer with one that invokes the delivery callback with an error.
  - Verifies the outbox event remains `sent = False`.

- `test_health_reports_unsent_count`
  - Starts the health server and issues `GET /healthz`.
  - Asserts HTTP 200 and that `outbox_unsent` is an integer ≥ 1 when an outbox event is pending.

## Notes

- The consumer parses messages by splitting on the `|` character; the tests reflect this format.
- The real Kafka and Postgres stacks are not required for the tests; SQLite and a fake producer are used.
- The dispatcher’s delivery callback marks outbox events as sent only on success; error paths keep events unsent.

## Running the Tests

```
pytest -q
```

Ensure dependencies from `requirements.txt` are installed in your environment.
# Test Suite Overview
