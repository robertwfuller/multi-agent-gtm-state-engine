#!/usr/bin/env python3
"""
tests/test_engine.py
Scope: Unit, zero-row regression, and real PostgreSQL advisory-lock + state persistence tests for GtmStateEngine.
"""

import os
import uuid
from pathlib import Path
import pytest
import psycopg2
from unittest.mock import patch
import multi_agent_gtm_state_engine as engine_mod
from multi_agent_gtm_state_engine import GtmStateEngine, VALID_STATES


class MockCursor:
    def __init__(self, should_lock_succeed=True, rowcount=1, raise_on_update=None):
        self.should_lock_succeed = should_lock_succeed
        self.rowcount = rowcount
        self.raise_on_update = raise_on_update
        self.executed_queries = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed_queries.append((sql, params))
        if "UPDATE funnel_runs" in sql and self.raise_on_update:
            raise self.raise_on_update

    def fetchone(self):
        return (self.should_lock_succeed,)

    def close(self):
        self.closed = True


class MockConnection:
    def __init__(self, should_lock_succeed=True, rowcount=1, raise_on_update=None):
        self.cursor_obj = MockCursor(should_lock_succeed, rowcount, raise_on_update)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_canonical_import_path():
    src_path = Path(engine_mod.__file__).resolve()
    assert "src" in src_path.parts, f"Expected import from src/, got {src_path}"


@pytest.mark.parametrize("current_state,next_state", [
    ("idle", "observing"),
    ("observing", "interacting"),
    ("interacting", "quiescent"),
    ("quiescent", "assert"),
])
@patch("psycopg2.connect")
def test_advance_state_succeeds_when_one_row_updated(mock_connect, current_state, next_state):
    mock_conn = MockConnection(should_lock_succeed=True, rowcount=1)
    mock_connect.return_value = mock_conn

    engine = GtmStateEngine(dsn="postgresql://mock_user:mock_pass@localhost:5432/mock_db")
    run_id = str(uuid.uuid4())
    assert engine.advance_state(run_id, "SCEN-001", current_state, next_state) is True
    assert mock_conn.committed is True
    assert mock_conn.rolled_back is False
    assert mock_conn.closed is True
    assert mock_conn.cursor_obj.closed is True


@patch("psycopg2.connect")
def test_advance_state_returns_false_when_zero_rows_updated(mock_connect):
    """R-03 Regression: Missing run_id or stale current_state affects 0 rows and MUST return False."""
    mock_conn = MockConnection(should_lock_succeed=True, rowcount=0)
    mock_connect.return_value = mock_conn

    engine = GtmStateEngine(dsn="postgresql://mock_user:mock_pass@localhost:5432/mock_db")
    run_id = str(uuid.uuid4())
    assert engine.advance_state(run_id, "SCEN-ZERO-ROWS", "idle", "observing") is False
    assert mock_conn.committed is True
    assert mock_conn.rolled_back is False
    assert mock_conn.closed is True


@pytest.mark.parametrize("bad_current,bad_next", [
    ("unknown_state", "observing"),
    ("idle", "unknown_state"),
    ("", "observing"),
    ("idle", ""),
])
def test_advance_state_rejects_invalid_states(bad_current, bad_next):
    engine = GtmStateEngine()
    with pytest.raises(ValueError):
        engine.advance_state(str(uuid.uuid4()), "SCEN-INVALID", bad_current, bad_next)


@patch("psycopg2.connect")
def test_advisory_lock_collision_rolls_back_and_returns_false(mock_connect):
    mock_conn = MockConnection(should_lock_succeed=False, rowcount=0)
    mock_connect.return_value = mock_conn

    engine = GtmStateEngine()
    assert engine.advance_state(str(uuid.uuid4()), "SCEN-COLLISION", "idle", "observing") is False
    assert mock_conn.rolled_back is True
    assert mock_conn.committed is False
    assert mock_conn.closed is True


@patch("psycopg2.connect")
def test_database_error_rolls_back_and_returns_false(mock_connect):
    mock_conn = MockConnection(
        should_lock_succeed=True,
        rowcount=0,
        raise_on_update=psycopg2.OperationalError("simulated connection drop"),
    )
    mock_connect.return_value = mock_conn

    engine = GtmStateEngine()
    assert engine.advance_state(str(uuid.uuid4()), "SCEN-DB-ERR", "idle", "observing") is False
    assert mock_conn.rolled_back is True
    assert mock_conn.committed is False
    assert mock_conn.closed is True


@pytest.mark.skipif(not os.environ.get("TEST_PG_DSN"), reason="TEST_PG_DSN not set for live PostgreSQL integration")
def test_postgres_integration_state_persistence_and_advisory_lock_contention():
    """
    Real PostgreSQL integration test exercising:
      1. Valid transition persists state change in funnel_runs (returns True)
      2. Stale current_state or missing run_id does not modify row and returns False
      3. Advisory lock contention across two independent connections blocks second caller (returns False)
      4. Releasing the first transaction's lock allows the subsequent transition to succeed (returns True)
    """
    dsn = os.environ["TEST_PG_DSN"]
    run_id = f"run_{uuid.uuid4().hex[:10]}"
    scenario_id = f"scen_{uuid.uuid4().hex[:8]}"

    admin_conn = psycopg2.connect(dsn)
    admin_conn.autocommit = True
    try:
        with admin_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS funnel_runs (
                    id VARCHAR(128) PRIMARY KEY,
                    state VARCHAR(32) NOT NULL,
                    locked_at TIMESTAMPTZ
                );
            """)
            cur.execute("INSERT INTO funnel_runs (id, state) VALUES (%s, 'idle');", (run_id,))

        engine = GtmStateEngine(dsn=dsn)

        # 1. Missing run_id -> 0 rows updated -> False
        assert engine.advance_state("nonexistent_run", scenario_id, "idle", "observing") is False

        # 2. Stale current_state -> 0 rows updated -> False
        assert engine.advance_state(run_id, scenario_id, "observing", "interacting") is False

        # 3. Valid transition idle -> observing -> True & persisted
        assert engine.advance_state(run_id, scenario_id, "idle", "observing") is True
        with admin_conn.cursor() as cur:
            cur.execute("SELECT state FROM funnel_runs WHERE id = %s;", (run_id,))
            assert cur.fetchone()[0] == "observing"

        # 4. Advisory lock contention across independent connections:
        # Hold the advisory lock inside engine.transaction_lock in connection 1 while connection 2 attempts advance_state
        with engine.transaction_lock(f"funnel:{scenario_id}"):
            assert engine.advance_state(run_id, scenario_id, "observing", "interacting") is False

        # 5. Once the context manager exits and releases the advisory lock, transition succeeds
        assert engine.advance_state(run_id, scenario_id, "observing", "interacting") is True
        with admin_conn.cursor() as cur:
            cur.execute("SELECT state FROM funnel_runs WHERE id = %s;", (run_id,))
            assert cur.fetchone()[0] == "interacting"
    finally:
        with admin_conn.cursor() as cur:
            cur.execute("DELETE FROM funnel_runs WHERE id = %s;", (run_id,))
        admin_conn.close()
