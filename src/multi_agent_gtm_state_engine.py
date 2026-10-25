#!/usr/bin/env python3
"""
multi_agent_gtm_state_engine.py
Asset #19 (Portfolio Repo): Multi-Agent GTM Workflows State Orchestrator.
Scope: Uses PostgreSQL Transaction Advisory Locks and rowcount verification for state transitions.
"""

import os
import hashlib
import psycopg2
from contextlib import contextmanager

VALID_STATES = ('idle', 'observing', 'interacting', 'quiescent', 'assert')


class GtmStateEngine:
    def __init__(self, dsn: str = None):
        self.dsn = dsn or os.environ.get("PG_DSN", "postgresql://user:pass@localhost:5432/harness_test")

    @contextmanager
    def transaction_lock(self, lock_id_str: str):
        """
        Uses pg_try_advisory_xact_lock to secure a transaction-level lock.
        Automatically releases at the end of the transaction block.
        """
        lock_hash = int(hashlib.md5(lock_id_str.encode("utf-8")).hexdigest()[:8], 16) - (1 << 31)
        conn = psycopg2.connect(self.dsn)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT pg_try_advisory_xact_lock(%s);", (lock_hash,))
            row = cursor.fetchone()
            acquired = bool(row and row[0])
            if not acquired:
                raise BlockingIOError(f"Advisory lock collision on hash: {lock_hash} ({lock_id_str})")
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def advance_state(self, run_id: str, scenario_id: str, current_state: str, next_state: str) -> bool:
        """
        Transitions a run safely from current_state to next_state.
        Returns True if and only if the advisory lock was acquired AND exactly 1 row was updated.
        """
        if current_state not in VALID_STATES:
            raise ValueError(f"Invalid current GTM State: {current_state}")
        if next_state not in VALID_STATES:
            raise ValueError(f"Invalid next GTM State: {next_state}")

        lock_key = f"funnel:{scenario_id}"
        try:
            with self.transaction_lock(lock_key) as cursor:
                sql = """
                UPDATE funnel_runs
                SET state = %s,
                    locked_at = CURRENT_TIMESTAMP
                WHERE id = %s AND state = %s;
                """
                cursor.execute(sql, (next_state, run_id, current_state))
                return int(getattr(cursor, "rowcount", 0)) == 1
        except (BlockingIOError, psycopg2.Error) as err:
            print(f"[!] Transition blocked: {err}")
            return False
