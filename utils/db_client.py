"""
db_client.py
────────────
PostgreSQL helpers for verifying data written by the DataReceiver API.

Key addition: get_bulk_snapshot(run_id) — fetches ALL records for a test run
in a single query. Used by the two-phase architecture so the whole DB
verification phase costs one wait + three SELECTs total.
"""
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

import config


@contextmanager
def _conn():
    conn = psycopg2.connect(**config.DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()


def _fetch(sql: str, params: tuple = ()) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def _execute(sql: str, params: tuple = ()) -> int:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


# ── Individual lookups (kept for backwards compatibility) ──────────────────────

def get_client_users_data(client_user_id: str) -> list[dict]:
    return _fetch(
        "SELECT * FROM profiles.client_users_data WHERE LOWER(client_user_id) = LOWER(%s)",
        (client_user_id,),
    )


def get_client_user_mapping(client_user_id: str) -> list[dict]:
    return _fetch(
        "SELECT * FROM profiles.client_user_mapping WHERE LOWER(client_user_id) = LOWER(%s)",
        (client_user_id,),
    )


def get_raw_signals() -> list[dict]:
    return _fetch("SELECT * FROM profiles.raw_signals")


def get_user_properties(client_user_id: str) -> list[dict]:
    """Joins through client_user_mapping because user_properties has no client_user_id column."""
    return _fetch(
        """
        SELECT up.*
        FROM   profiles.user_properties up
        JOIN   profiles.client_user_mapping cum ON cum.da_user_id = up.da_user_id
        WHERE  LOWER(cum.client_user_id) = LOWER(%s)
        """,
        (client_user_id,),
    )


# ── Bulk snapshot — the core of the two-phase approach ────────────────────────

def get_bulk_snapshot(run_id: str) -> dict:
    """
    Fetch ALL records created by a test run in three queries.
    Returns a dict with three sub-dicts, each keyed by lowercase client_user_id.

        {
          "user_properties":    { "at_user_xxxx_...": [row, ...] },
          "client_users_data":  { "at_user_xxxx_...": [row, ...] },
          "client_user_mapping":{ "at_user_xxxx_...": [row, ...] },
        }

    Phase-2 DB tests do instant dict lookups against this — no DB calls,
    no polling, no waiting.
    """
    prefix = f"at_user_{run_id.lower()}%"

    # ── 1. user_properties (via mapping join) ──────────────────────────────────
    up_rows = _fetch(
        """
        SELECT up.*, cum.client_user_id AS _client_user_id
        FROM   profiles.user_properties up
        JOIN   profiles.client_user_mapping cum ON cum.da_user_id = up.da_user_id
        WHERE  LOWER(cum.client_user_id) LIKE %s
        ORDER  BY up.id
        """,
        (prefix,),
    )

    # ── 2. client_users_data ───────────────────────────────────────────────────
    cud_rows = _fetch(
        """
        SELECT * FROM profiles.client_users_data
        WHERE  LOWER(client_user_id) LIKE %s
        ORDER  BY id
        """,
        (prefix,),
    )

    # ── 3. client_user_mapping ─────────────────────────────────────────────────
    cum_rows = _fetch(
        """
        SELECT * FROM profiles.client_user_mapping
        WHERE  LOWER(client_user_id) LIKE %s
        ORDER  BY id
        """,
        (prefix,),
    )

    # ── Build lookup dicts keyed by lowercase client_user_id ──────────────────
    def group_by_user(rows: list[dict], id_field: str = "client_user_id") -> dict:
        result: dict = {}
        for row in rows:
            key = row[id_field].lower()
            result.setdefault(key, []).append(row)
        return result

    return {
        "user_properties":    group_by_user(up_rows,  "_client_user_id"),
        "client_users_data":  group_by_user(cud_rows, "client_user_id"),
        "client_user_mapping": group_by_user(cum_rows, "client_user_id"),
    }


# ── Legacy pollers (still used if someone calls them directly) ─────────────────

def wait_for_signal_in_db(client_user_id: str, max_wait: int = None) -> list[dict]:
    max_wait = max_wait or config.DB_PROPAGATION_DELAY
    deadline = time.time() + max_wait
    while time.time() < deadline:
        rows = get_client_users_data(client_user_id)
        if rows:
            return rows
        time.sleep(1)
    return []


def wait_for_user_property_in_db(client_user_id: str, max_wait: int = None) -> list[dict]:
    max_wait = max_wait or config.DB_PROPAGATION_DELAY
    deadline = time.time() + max_wait
    while time.time() < deadline:
        rows = get_user_properties(client_user_id)
        if rows:
            return rows
        time.sleep(1)
    return []


# ── Run-level record helpers (for cleanup) ─────────────────────────────────────

def get_test_records_by_run(run_id: str) -> dict:
    prefix = f"at_user_{run_id.lower()}%"
    signals = _fetch(
        "SELECT id, client_user_id, signal_name FROM profiles.client_users_data "
        "WHERE LOWER(client_user_id) LIKE %s ORDER BY id",
        (prefix,),
    )
    mapping = _fetch(
        "SELECT id, client_user_id, da_user_id FROM profiles.client_user_mapping "
        "WHERE LOWER(client_user_id) LIKE %s ORDER BY id",
        (prefix,),
    )
    props = _fetch(
        """
        SELECT up.id, up.da_user_id, up.property_name, up.property_value
        FROM   profiles.user_properties up
        JOIN   profiles.client_user_mapping cum ON cum.da_user_id = up.da_user_id
        WHERE  LOWER(cum.client_user_id) LIKE %s ORDER BY up.id
        """,
        (prefix,),
    )
    return {
        "client_users_data":   signals,
        "client_user_mapping": mapping,
        "user_properties":     props,
    }


def delete_test_records_by_run(run_id: str) -> dict:
    prefix = f"at_user_{run_id.lower()}%"
    props = _execute(
        "DELETE FROM profiles.user_properties WHERE da_user_id IN "
        "(SELECT da_user_id FROM profiles.client_user_mapping WHERE LOWER(client_user_id) LIKE %s)",
        (prefix,),
    )
    signals = _execute(
        "DELETE FROM profiles.client_users_data WHERE LOWER(client_user_id) LIKE %s",
        (prefix,),
    )
    mapping = _execute(
        "DELETE FROM profiles.client_user_mapping WHERE LOWER(client_user_id) LIKE %s",
        (prefix,),
    )
    return {
        "user_properties":     props,
        "client_users_data":   signals,
        "client_user_mapping": mapping,
    }