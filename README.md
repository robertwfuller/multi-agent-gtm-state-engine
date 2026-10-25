# Multi-Agent GTM State Engine

![CI Test Suite](https://github.com/robertwfuller/multi-agent-gtm-state-engine/actions/workflows/ci.yml/badge.svg)

A PostgreSQL-backed state transition coordinator for multi-agent workflows using transaction-level advisory locks (`pg_try_advisory_xact_lock`) (Canonical Catalog: **Asset #19**).

## Implemented Capabilities
* **Transaction-Level Advisory Locking (`GtmStateEngine.transaction_lock`):** Hashes scenario lock keys into signed 32-bit integers and acquires non-blocking transaction locks via `pg_try_advisory_xact_lock`.
* **Conditional State Advancement (`GtmStateEngine.advance_state`):** Validates `current_state` and `next_state` against `VALID_STATES` (`idle`, `observing`, `interacting`, `quiescent`, `assert`) and returns `True` only when `UPDATE funnel_runs ... WHERE id = %s AND state = %s` modifies a row (`rowcount == 1`).

## Proposed Enhancements (Roadmap)
* Explicit transition-graph edge enforcement (`current_state -> next_state`).
* Idempotency key deduplication and consecutive-failure quarantine circuit breakers.

## Quickstart & Verification
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -v
```
To run the live PostgreSQL advisory-lock and state-persistence integration test locally, set `TEST_PG_DSN`:
```bash
TEST_PG_DSN="postgresql://test_user:test_password@localhost:5432/harness_test" pytest -v
```
