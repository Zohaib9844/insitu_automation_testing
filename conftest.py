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
import re
from html import escape
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import config
from utils import db_client

_session_run_id: str | None = None
_report_meta: dict[str, dict] = {}
_coverage_summary: dict[str, list[int]] = {"present": [], "missing": [], "extra": []}
_case_results: dict[str, dict[str, list[bool]]] = {}


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

def pytest_configure(config):
    config.addinivalue_line("markers", "happy_path: Happy-path / smoke tests")
    config.addinivalue_line("markers", "regression: Regression tests (rows 76-119)")
    config.addinivalue_line("markers", "signals:    Tests for the signals schema")
    config.addinivalue_line("markers", "userprops:  Tests for the userproperties schema")
    config.addinivalue_line("markers", "api:        Phase-1 API-only tests")
    config.addinivalue_line("markers", "db:         Phase-2 DB-only tests")


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


# ── Coverage + HTML report enrichment ──────────────────────────────────────────

def _extract_row_number(nodeid: str) -> int | None:
    m = re.search(r"row(\d+)", nodeid.lower())
    return int(m.group(1)) if m else None


def _layer(item: pytest.Item) -> str:
    if item.get_closest_marker("db"):
        return "DB check"
    if item.get_closest_marker("api"):
        return "API check"
    if "test_phase2_db.py" in item.nodeid:
        return "DB check"
    if "test_phase1_api.py" in item.nodeid:
        return "API check"
    return "API+DB check"


def _description(item: pytest.Item) -> str:
    doc = (getattr(item.function, "__doc__", "") or "").strip()
    if doc:
        return " ".join(doc.split())
    class_doc = (getattr(item.cls, "__doc__", "") or "").strip()
    if class_doc:
        return " ".join(class_doc.split())
    return item.name


def _case_key(item: pytest.Item) -> str:
    row = _extract_row_number(item.nodeid)
    if row is not None:
        return f"row{row}"
    class_doc = (getattr(item.cls, "__doc__", "") or "").strip()
    tc = re.search(r"\bTC-[A-Z]+-\d+\b", class_doc, flags=re.IGNORECASE)
    if tc:
        return tc.group(0).upper()
    return item.nodeid


def _final_verdict(item: pytest.Item) -> str:
    """
    DB is the source of truth whenever a case has DB checks.
    API-only cases fall back to API outcomes.
    """
    case = _case_key(item)
    outcomes = _case_results.get(case, {})
    db = outcomes.get("db", [])
    api = outcomes.get("api", [])

    if db:
        return "FAIL" if any(not ok for ok in db) else "PASS"
    if api:
        return "FAIL" if any(not ok for ok in api) else "PASS"
    return "N/A"


def _final_verdict_for_case(case: str) -> str:
    outcomes = _case_results.get(case, {})
    db = outcomes.get("db", [])
    api = outcomes.get("api", [])
    if db:
        return "FAIL" if any(not ok for ok in db) else "PASS"
    if api:
        return "FAIL" if any(not ok for ok in api) else "PASS"
    return "N/A"


def _extract_test_data(item: pytest.Item) -> str:
    inst = getattr(item, "instance", None)
    if not inst:
        return "N/A"

    response = getattr(inst, "response", None)
    request = getattr(response, "request", None)
    body = getattr(request, "body", None)
    if body is not None:
        if isinstance(body, bytes):
            body = body.decode("utf-8", errors="replace")
        compact = " ".join(str(body).split())
        return compact[:500] + ("…" if len(compact) > 500 else "")

    keys = [
        "client_user_id", "user_id_1", "user_id_2",
        "property_name", "signal_name",
    ]
    values = [f"{k}={getattr(inst, k)}" for k in keys if hasattr(inst, k)]
    return ", ".join(values) if values else "N/A"


def _sql_query(item: pytest.Item) -> str:
    inst = getattr(item, "instance", None)
    user_hint = None
    if inst:
        user_hint = getattr(inst, "client_user_id", None) or getattr(inst, "user_id_1", None)

    if item.get_closest_marker("signals"):
        if item.get_closest_marker("db"):
            uid = user_hint or "<client_user_id>"
            return (
                f"SELECT * FROM profiles.client_users_data WHERE LOWER(client_user_id)=LOWER('{uid}'); "
                f"SELECT * FROM profiles.client_user_mapping WHERE LOWER(client_user_id)=LOWER('{uid}');"
            )
        return "N/A (API assertion)"

    if item.get_closest_marker("userprops"):
        if item.get_closest_marker("db"):
            uid = user_hint or "<client_user_id>"
            return (
                "SELECT up.* FROM profiles.user_properties up "
                "JOIN profiles.client_user_mapping cum ON cum.da_user_id = up.da_user_id "
                f"WHERE LOWER(cum.client_user_id)=LOWER('{uid}');"
            )
        return "N/A (API assertion)"

    return "N/A"


def pytest_collection_finish(session: pytest.Session):
    present = sorted({
        row
        for item in session.items
        for row in [_extract_row_number(item.nodeid)]
        if row is not None
    })
    expected = set(range(76, 120))
    missing = sorted(expected - set(present))
    extra = sorted(set(present) - expected)
    _coverage_summary["present"] = present
    _coverage_summary["missing"] = missing
    _coverage_summary["extra"] = extra


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    outcome = yield
    report = outcome.get_result()
    if report.when != "call":
        return

    if report.failed:
        reason = f"{_layer(item)} failed"
    elif report.passed:
        reason = f"{_layer(item)} passed"
    else:
        reason = f"{_layer(item)} skipped"

    case = _case_key(item)
    bucket = "db" if _layer(item) == "DB check" else "api"
    passed = bool(report.passed)
    _case_results.setdefault(case, {"api": [], "db": []})[bucket].append(passed)

    _report_meta[item.nodeid] = {
        "case_key": case,
        "description": _description(item),
        "test_data": _extract_test_data(item),
        "layer": _layer(item),
        "reason": reason,
        "final_verdict": _final_verdict(item),
        "sql": _sql_query(item),
    }


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_header(cells):
    cells.insert(2, "<th>Layer</th>")
    cells.insert(3, "<th>Final Verdict</th>")
    cells.insert(4, "<th>Description</th>")
    cells.insert(5, "<th>Test Data</th>")
    cells.insert(6, "<th>Reason</th>")
    cells.insert(7, "<th>SQL Query</th>")


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_row(report, cells):
    meta = _report_meta.get(report.nodeid, {})
    case = meta.get("case_key")
    verdict = _final_verdict_for_case(case) if case else meta.get("final_verdict", "N/A")
    cells.insert(2, f"<td>{escape(meta.get('layer', 'N/A'))}</td>")
    cells.insert(3, f"<td>{escape(verdict)}</td>")
    cells.insert(4, f"<td>{escape(meta.get('description', 'N/A'))}</td>")
    cells.insert(5, f"<td>{escape(meta.get('test_data', 'N/A'))}</td>")
    cells.insert(6, f"<td>{escape(meta.get('reason', 'N/A'))}</td>")
    cells.insert(7, f"<td>{escape(meta.get('sql', 'N/A'))}</td>")


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_summary(prefix, summary, postfix):
    present = _coverage_summary.get("present", [])
    missing = _coverage_summary.get("missing", [])
    extra = _coverage_summary.get("extra", [])

    prefix.extend([
        "<h3>Excel Coverage Audit (Rows 76–119)</h3>",
        f"<p>Rows present in collected suite: {len(present)}</p>",
        "<p>Missing rows: "
        + (", ".join(map(str, missing)) if missing else "None")
        + "</p>",
        "<p>Out-of-range rows found: "
        + (", ".join(map(str, extra)) if extra else "None")
        + "</p>",
    ])
