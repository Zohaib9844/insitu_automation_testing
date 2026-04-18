"""
conftest.py
───────────
Two-phase DB verification design
─────────────────────────────────
Phase 1  (test_phase1_api.py)  – all API calls fire, responses asserted,
          each test writes what it submitted into `submissions` dict.

Phase 2  (test_phase2_db.py)   – `db_snapshot` session fixture fires ONCE:
          sleeps DB_PROPAGATION_DELAY once, runs ONE bulk SQL query for the
          whole run, returns a lookup dict. Every DB assertion is an instant
          dict lookup — no per-test polling, no per-test DB round trips.

Wait time:  1 × DB_PROPAGATION_DELAY   (was N_tests × delay)
DB queries: 3 bulk SELECTs per session  (was N_tests SELECTs)
"""

import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import config
from utils import db_client

_session_run_id: str | None = None


# ── Session run ID ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def run_id() -> str:
    global _session_run_id
    _session_run_id = uuid.uuid4().hex[:8].upper()
    return _session_run_id


# ── Per-test unique identifiers ────────────────────────────────────────────────

@pytest.fixture
def unique_user_id(run_id) -> str:
    return f"AT_USER_{run_id}_{int(time.time())}"


@pytest.fixture
def unique_signal_name(run_id) -> str:
    return f"AT_SIG_{run_id}_{int(time.time())}"


@pytest.fixture
def unique_property_name(run_id) -> str:
    return f"AT_PROP_{run_id}_{int(time.time())}"


# ── Submissions registry ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def submissions() -> dict:
    """
    Session-level dict. Phase-1 API tests WRITE into this.
    Phase-2 DB tests READ from this.

    Schema per entry:
        submissions["76"] = {
            "user_ids":      ["AT_USER_XXXX_..."],   # all user IDs submitted
            "property_name": "AT_PROP_XXXX_...",     # UP tests
            "signal_name":   "AT_SIG_XXXX_...",      # signal tests
            "api_status":    200,                    # HTTP status received
            "extra":         {},                     # anything row-specific
        }
    """
    return {}


# ── Bulk DB snapshot (runs ONCE for the whole session) ────────────────────────

@pytest.fixture(scope="session")
def db_snapshot(run_id: str) -> dict:
    """
    Called the first time any Phase-2 DB test needs it — and only then.
    Waits once, bulk-queries once, caches forever for the session.

    Returns:
        {
          "user_properties":    { "at_user_xxxx_...": [row_dict, ...] },
          "client_users_data":  { "at_user_xxxx_...": [row_dict, ...] },
          "client_user_mapping":{ "at_user_xxxx_...": [row_dict, ...] },
        }

    Usage in DB tests:
        rows = db_snapshot["user_properties"].get(self.client_user_id.lower(), [])
        assert rows, f"Not found: {self.client_user_id}"
    """
    print(f"\n[db_snapshot] Waiting {config.DB_PROPAGATION_DELAY}s for async writes …")
    time.sleep(config.DB_PROPAGATION_DELAY)
    print(f"[db_snapshot] Bulk-querying run {run_id} …")
    snapshot = db_client.get_bulk_snapshot(run_id)
    total = sum(
        sum(len(v) for v in table.values())
        for table in snapshot.values()
    )
    print(f"[db_snapshot] Done — {total} rows cached. All DB tests are now instant.\n")
    return snapshot


# ── Markers ────────────────────────────────────────────────────────────────────

def pytest_configure(config_obj):
    config_obj.addinivalue_line("markers", "happy_path: Happy-path / smoke tests")
    config_obj.addinivalue_line("markers", "regression: Regression tests (rows 76-119)")
    config_obj.addinivalue_line("markers", "signals:    Tests for the signals schema")
    config_obj.addinivalue_line("markers", "userprops:  Tests for the userproperties schema")
    config_obj.addinivalue_line("markers", "api:        Phase-1 API-only tests")
    config_obj.addinivalue_line("markers", "db:         Phase-2 DB-only tests")


# ── CLI helpers ────────────────────────────────────────────────────────────────

def pytest_addoption(parser):
    parser.addoption(
        "--cleanup-run", metavar="RUN_ID", default=None,
        help="Delete all DB records for a previous run and exit.",
    )


def pytest_sessionstart(session):
    rid = session.config.getoption("--cleanup-run", default=None)
    if not rid:
        return
    print(f"\n[cleanup] Run: {rid.upper()}")
    try:
        records = db_client.get_test_records_by_run(rid)
        total = sum(len(v) for v in records.values())
        if total == 0:
            pytest.exit("Nothing to clean up.", returncode=0)
        deleted = db_client.delete_test_records_by_run(rid)
        for table, count in deleted.items():
            print(f"   Deleted {count} from profiles.{table}")
    except Exception as exc:
        print(f"[cleanup] Error: {exc}")
        pytest.exit("Cleanup failed.", returncode=1)
    pytest.exit("Cleanup complete.", returncode=0)


def pytest_sessionfinish(session, exitstatus):
    if _session_run_id is None:
        return
    try:
        records = db_client.get_test_records_by_run(_session_run_id)
    except Exception as exc:
        print(f"\n[cleanup] DB unavailable: {exc}")
        return

    total = sum(len(v) for v in records.values())
    print("\n" + "═" * 70)
    print(f"  RUN {_session_run_id}  —  {total} record(s) in DB")
    print("═" * 70)

    if total == 0:
        print("  Nothing to clean up.\n")
        return

    for table, rows in records.items():
        if not rows:
            continue
        print(f"\n  ┌─ profiles.{table} ({len(rows)} row(s))")
        for row in rows:
            if table == "client_users_data":
                print(f"  │  id={row['id']}  user={row['client_user_id']}  signal={row['signal_name']}")
            elif table == "client_user_mapping":
                print(f"  │  id={row['id']}  user={row['client_user_id']}")
            elif table == "user_properties":
                print(f"  │  id={row['id']}  prop={row['property_name']}={row.get('property_value', '')}")
        print(f"  └{'─' * 60}")

    if not sys.stdin.isatty():
        print(f"\n[cleanup] CI mode — run:  pytest --cleanup-run {_session_run_id}\n")
        return

    print()
    try:
        answer = input(
            f"  Delete all {total} record(s) for run {_session_run_id}? [y/N] "
        ).strip().lower()
    except (EOFError, KeyboardInterrupt):
        answer = "n"

    if answer == "y":
        deleted = db_client.delete_test_records_by_run(_session_run_id)
        for table, count in deleted.items():
            print(f"  ✔ {count} row(s) from profiles.{table}")
    else:
        print(f"  Kept. Clean later: pytest --cleanup-run {_session_run_id}")
    print()