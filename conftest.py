"""
conftest.py
───────────
Shared pytest fixtures and hooks for the InSitu QA suite.
"""
import sys
import time
import uuid
from pathlib import Path

import pytest

# Make project root importable so `import config` / `import utils.*` work
sys.path.insert(0, str(Path(__file__).parent))

from utils import db_client

# ── Module-level run_id storage (needed by session-finish hook) ────────────────
# Fixtures can't be accessed inside pytest hooks directly, so we cache it here.
_session_run_id: str | None = None


# ── Unique test-run ID ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def run_id() -> str:
    """A short unique ID that ties all DB records created in this test run together."""
    global _session_run_id
    _session_run_id = uuid.uuid4().hex[:8].upper()
    return _session_run_id


# ── Unique per-test identifiers ────────────────────────────────────────────────

@pytest.fixture
def unique_user_id(run_id) -> str:
    """e.g. AT_USER_A3F21B04_1712345678"""
    return f"AT_USER_{run_id}_{int(time.time())}"


@pytest.fixture
def unique_signal_name(run_id) -> str:
    return f"AT_SIG_{run_id}_{int(time.time())}"


@pytest.fixture
def unique_property_name(run_id) -> str:
    return f"AT_PROP_{run_id}_{int(time.time())}"


# ── Pytest hooks ───────────────────────────────────────────────────────────────

def pytest_configure(config):
    """Register custom markers so pytest doesn't warn about unknown ones."""
    config.addinivalue_line("markers", "happy_path: Happy-path / smoke tests")
    config.addinivalue_line("markers", "signals:    Tests for the signals schema")
    config.addinivalue_line("markers", "userprops:  Tests for the userproperties schema")
    config.addinivalue_line("markers", "db:         Tests that verify DB state")
    config.addinivalue_line("markers", "regression: Regression / negative / boundary tests")
    config.addinivalue_line("markers", "skip_reason: Tests skipped with a documented reason")


def pytest_sessionfinish(session, exitstatus):
    """
    After all tests complete, show the DB records created this run and
    interactively ask whether to delete them.

    Requires pytest to be run with -s (no output capture) so that
    input() can read from the terminal. In CI environments where stdin
    is not a tty, the prompt is skipped automatically.
    """
    if _session_run_id is None:
        # No tests actually ran (e.g. collection-only mode)
        return

    # ── Fetch records ──────────────────────────────────────────────────────────
    try:
        records = db_client.get_test_records_by_run(_session_run_id)
    except Exception as exc:
        print(f"\n[cleanup] Could not query DB for run {_session_run_id}: {exc}")
        return

    total = sum(len(v) for v in records.values())

    print("\n" + "═" * 70)
    print(f"  TEST RUN: {_session_run_id}  —  {total} record(s) created in DB")
    print("═" * 70)

    if total == 0:
        print("  Nothing to clean up.\n")
        return

    # ── Print summary table ────────────────────────────────────────────────────
    for table, rows in records.items():
        if not rows:
            continue
        print(f"\n  ┌─ profiles.{table} ({len(rows)} row(s))")
        for row in rows:
            if table == "client_users_data":
                print(f"  │  id={row['id']}  user={row['client_user_id']}  signal={row['signal_name']}")
            elif table == "client_user_mapping":
                print(f"  │  id={row['id']}  user={row['client_user_id']}  da_user_id={row['da_user_id']}")
            elif table == "user_properties":
                print(f"  │  id={row['id']}  da_user_id={row['da_user_id']}  prop={row['property_name']}={row['property_value']}")
        print(f"  └{'─' * 60}")

    # ── Prompt ────────────────────────────────────────────────────────────────
    # Skip prompt when stdin is not a real terminal (CI pipelines, piped input).
    if not sys.stdin.isatty():
        print("\n[cleanup] Non-interactive environment detected — skipping cleanup prompt.")
        print(f"[cleanup] To clean up manually, run:  pytest --cleanup-run {_session_run_id}\n")
        return

    print()
    try:
        answer = input(f"  Delete all {total} record(s) for run {_session_run_id}? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\n[cleanup] Prompt interrupted — records kept.")
        return

    if answer == "y":
        try:
            deleted = db_client.delete_test_records_by_run(_session_run_id)
            print(f"\n  ✔ Deleted:")
            for table, count in deleted.items():
                print(f"     {count} row(s) from profiles.{table}")
        except Exception as exc:
            print(f"\n  ✘ Cleanup failed: {exc}")
    else:
        print(f"\n  Records kept. Run ID: {_session_run_id}")
        print(f"  To clean up later, run:  pytest --cleanup-run {_session_run_id}")

    print()


# ── Optional --cleanup-run CLI flag ───────────────────────────────────────────

def pytest_addoption(parser):
    """Add --cleanup-run <RUN_ID> option to delete records from a previous run."""
    parser.addoption(
        "--cleanup-run",
        metavar="RUN_ID",
        default=None,
        help="Delete all DB records for a previous test run and exit. "
             "Example: pytest --cleanup-run A3F21B04",
    )


def pytest_sessionstart(session):
    """If --cleanup-run was passed, delete those records and exit before any tests run."""
    run_id_to_clean = session.config.getoption("--cleanup-run", default=None)
    if not run_id_to_clean:
        return

    print(f"\n[cleanup] Looking up records for run: {run_id_to_clean.upper()}")
    try:
        records = db_client.get_test_records_by_run(run_id_to_clean)
        total = sum(len(v) for v in records.values())

        if total == 0:
            print(f"[cleanup] No records found for run {run_id_to_clean.upper()}.")
            pytest.exit("Nothing to clean up.", returncode=0)

        print(f"[cleanup] Found {total} record(s):")
        for table, rows in records.items():
            if rows:
                ids = [str(r["id"]) for r in rows]
                print(f"   profiles.{table}: ids {', '.join(ids)}")

        deleted = db_client.delete_test_records_by_run(run_id_to_clean)
        print("[cleanup] Deleted:")
        for table, count in deleted.items():
            print(f"   {count} row(s) from profiles.{table}")

    except Exception as exc:
        print(f"[cleanup] Error: {exc}")
        pytest.exit("Cleanup failed.", returncode=1)

    pytest.exit("Cleanup complete.", returncode=0)  