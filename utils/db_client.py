"""
db_client.py
────────────
PostgreSQL helpers for verifying data written by the DataReceiver API.

All public functions return plain dicts / lists so tests stay readable
without knowing psycopg2 internals.
"""
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

import config


@contextmanager
def _conn():
    """Yield a short-lived connection, then close it."""
    conn = psycopg2.connect(**config.DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()


def _fetch(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return rows as a list of dicts."""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def _execute(sql: str, params: tuple = ()) -> int:
    """Execute a non-SELECT statement and return rowcount."""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


# ── Signals ───────────────────────────────────────────────────────────────────

def get_client_users_data(client_user_id: str) -> list[dict]:
    """Return all rows from profiles.client_users_data for a given client_user_id.
    Case-insensitive: the API inconsistently lowercases IDs on some paths.
    """
    return _fetch(
        "SELECT * FROM profiles.client_users_data WHERE LOWER(client_user_id) = LOWER(%s)",
        (client_user_id,),
    )


def get_client_user_mapping(client_user_id: str) -> list[dict]:
    """Return mapping rows for a client_user_id.
    Case-insensitive: the API lowercases client_user_id when writing to this table.
    """
    return _fetch(
        "SELECT * FROM profiles.client_user_mapping WHERE LOWER(client_user_id) = LOWER(%s)",
        (client_user_id,),
    )


def get_raw_signals() -> list[dict]:
    """Return all rows from profiles.raw_signals (signal metadata/stats store)."""
    return _fetch("SELECT * FROM profiles.raw_signals")


# ── UserProperties ─────────────────────────────────────────────────────────────

def get_user_properties(client_user_id: str) -> list[dict]:
    """Return all user_properties rows for a given client_user_id.
    Joins through client_user_mapping because user_properties has no client_user_id column —
    it uses da_user_id (UUID) as its FK. Case-insensitive on the mapping join.
    """
    return _fetch(
        """
        SELECT up.*
        FROM   profiles.user_properties up
        JOIN   profiles.client_user_mapping cum
               ON cum.da_user_id = up.da_user_id
        WHERE  LOWER(cum.client_user_id) = LOWER(%s)
        """,
        (client_user_id,),
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def wait_for_signal_in_db(client_user_id: str, max_wait: int = None) -> list[dict]:
    """
    Poll profiles.client_users_data until rows appear or max_wait seconds elapse.
    Returns the rows found (empty list on timeout).
    """
    max_wait = max_wait or config.DB_PROPAGATION_DELAY
    deadline = time.time() + max_wait
    while time.time() < deadline:
        rows = get_client_users_data(client_user_id)
        if rows:
            return rows
        time.sleep(1)
    return []


def wait_for_user_mapping_in_db(client_user_id: str, max_wait: int = None) -> list[dict]:
    """
    Poll profiles.client_user_mapping until rows appear or max_wait seconds elapse.
    The API lowercases client_user_id when writing to this table, so querying
    with LOWER() on both sides (already done in get_client_user_mapping) handles
    case mismatches correctly. The real reason to poll is that the DB write is async.
    Returns the rows found (empty list on timeout).
    """
    max_wait = max_wait or config.DB_PROPAGATION_DELAY
    deadline = time.time() + max_wait
    while time.time() < deadline:
        rows = get_client_user_mapping(client_user_id)
        if rows:
            return rows
        time.sleep(1)
    return []


def wait_for_user_property_in_db(client_user_id: str, max_wait: int = None) -> list[dict]:
    """
    Poll profiles.user_properties (via mapping join) until rows appear or max_wait seconds elapse.
    Returns the rows found (empty list on timeout).
    """
    max_wait = max_wait or config.DB_PROPAGATION_DELAY
    deadline = time.time() + max_wait
    while time.time() < deadline:
        rows = get_user_properties(client_user_id)
        if rows:
            return rows
        time.sleep(1)
    return []


# ── Cleanup helpers ────────────────────────────────────────────────────────────

def get_test_records_by_run(run_id: str) -> dict:
    """
    Return all DB records created by a specific test run, grouped by table.
    Uses the AT_USER_<run_id> / AT_SIG_<run_id> / AT_PROP_<run_id> naming convention.

    Returns a dict:
        {
            "client_users_data":  [{"id": ..., "client_user_id": ..., "signal_name": ...}, ...],
            "client_user_mapping": [{"id": ..., "client_user_id": ...}, ...],
            "user_properties":    [{"id": ..., "da_user_id": ...}, ...],
        }
    """
    prefix = f"at_user_{run_id.lower()}%"

    signals = _fetch(
        """
        SELECT id, client_user_id, signal_name
        FROM   profiles.client_users_data
        WHERE  LOWER(client_user_id) LIKE %s
        ORDER  BY id
        """,
        (prefix,),
    )

    mapping = _fetch(
        """
        SELECT id, client_user_id, da_user_id
        FROM   profiles.client_user_mapping
        WHERE  LOWER(client_user_id) LIKE %s
        ORDER  BY id
        """,
        (prefix,),
    )

    # user_properties has no client_user_id — join through mapping
    props = _fetch(
        """
        SELECT up.id, up.da_user_id, up.property_name, up.property_value
        FROM   profiles.user_properties up
        JOIN   profiles.client_user_mapping cum ON cum.da_user_id = up.da_user_id
        WHERE  LOWER(cum.client_user_id) LIKE %s
        ORDER  BY up.id
        """,
        (prefix,),
    )

    return {
        "client_users_data":   signals,
        "client_user_mapping": mapping,
        "user_properties":     props,
    }


def delete_test_records_by_run(run_id: str) -> dict:
    """
    Delete all records created by a specific test run from all three tables.
    Returns a dict with the row counts deleted per table.

    Deletion order matters: user_properties → client_users_data → client_user_mapping
    (mapping rows may be FK-referenced by user_properties).
    """
    prefix = f"at_user_{run_id.lower()}%"

    # 1. user_properties (joined delete via mapping)
    props_deleted = _execute(
        """
        DELETE FROM profiles.user_properties
        WHERE da_user_id IN (
            SELECT da_user_id
            FROM   profiles.client_user_mapping
            WHERE  LOWER(client_user_id) LIKE %s
        )
        """,
        (prefix,),
    )

    # 2. client_users_data
    signals_deleted = _execute(
        "DELETE FROM profiles.client_users_data WHERE LOWER(client_user_id) LIKE %s",
        (prefix,),
    )

    # 3. client_user_mapping (last, as it may be referenced)
    mapping_deleted = _execute(
        "DELETE FROM profiles.client_user_mapping WHERE LOWER(client_user_id) LIKE %s",
        (prefix,),
    )

    return {
        "user_properties":     props_deleted,
        "client_users_data":   signals_deleted,
        "client_user_mapping": mapping_deleted,
    }