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

# ── Excel title mapping ──────────────────────────────────────────────────────
# Maps the code's case key (e.g. "row76") to the exact title from the Excel sheet.
# Code row number = Excel row number - 1 (consistent offset throughout the suite).
TC_TITLES: dict[str, str] = {
    # Happy path — signals
    "TC-SIG-01": "TC-SIG-01: Verify signals CSV ingestion (happy path)",
    "TC-SIG-04": "TC-SIG-04: Verify signals JSON single ingestion (happy path)",
    "TC-SIG-06": "TC-SIG-06: Verify signals JSON array ingestion (happy path)",
    # Happy path — user properties
    "TC-UP-01":  "TC-UP-01: Verify userprops CSV ingestion (happy path)",
    "TC-UP-03":  "TC-UP-03: Verify userprops JSON single ingestion (happy path)",
    "TC-UP-05":  "TC-UP-05: Verify userprops JSON array ingestion (happy path)",
    # Regression rows 76–119 (code row = Excel row - 1)
    "row76":  "Verify that data receiver API with userproperties schema and csv format is ingesting data in all the user_properties table",
    "row77":  "Verify that data receiver API with userproperties schema and csv format is ingesting bsent=false and modified date for all the newly added properties",
    "row78":  "Verify that data receiver API with userproperties schema and csv format is ingesting bsent=false and modified date for all the updated properties with all datatypes",
    "row79":  "Verify that data receiver API with userproperties schema in csv format is giving error for the row with more than datatype values but insert other valid rows",
    "row80":  "Verify that data receiver API with userproperties schema and csv format is trimming spaces from all the fields while ingesting data in user_properties",
    "row81":  "Verify that data receiver API with userproperties schema in csv format is throwing error in case of missing ClientUserId column",
    "row82":  "Verify that data receiver API with userproperties schema in csv format is throwing error in case of missing PropertyName column",
    "row83":  "Verify that data receiver API with userproperties schema in csv format is throwing error in case of missing property_value column",
    "row84":  "Verify that data receiver API with userproperties schema in csv format is not entering data for the row in which ClientUserId column is missing and enter remaining data",
    "row85":  "Verify that data receiver API with userproperties schema in csv format is not entering data for the row in which ClientUserId column is empty or null and enter remaining data",
    "row86":  "Verify that data receiver API with userproperties schema in csv format is not entering data for the row in which PropertyName column is missing and enter remaining data",
    "row87":  "Verify that data receiver API with userproperties schema in csv format is not entering data for the row in which PropertyName column is empty or null and enter remaining data",
    "row88":  "Verify that data receiver API with userproperties schema in csv format is not entering data for the row in which property_value column is missing and enter remaining data",
    "row89":  "Verify that data receiver API with userproperties schema in csv format is entering data for the row in which property_value column is missing, empty or null",
    "row90":  "Verify that data receiver API with userproperties schema and in csv format is ignoring the columns which are not usable and not entering the required columns data",
    # row91 intentionally absent — no Excel mapping (Excel 92 = 'file size >1GB, not tested')
    "row92":  "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value Text column",
    "row93":  "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value is Text but given in any other property_value Type Column",
    "row94":  "Verify that data receiver API with userproperties schema and csv format is not inserting duplicate data property_value Text for in user_properties table",
    "row95":  "Verify that data receiver API with userproperties schema and csv format is ignoring case while verifying duplication for property_value Text in user_properties",
    "row96":  "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_int column",
    "row97":  "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_int but given in any other property_value Type Column",
    "row98":  "Verify that data receiver API with userproperties schema and csv format is not inserting duplicate data property_value_int for in user_properties table",
    "row99":  "Verify that data receiver API with userproperties schema and csv format is ignoring case while verifying duplication for property_value_int in user_properties",
    "row100": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_double column",
    "row101": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_double but given in any other property_value Type Column",
    "row102": "Verify that data receiver API with userproperties schema and csv format is not inserting duplicate data property_value_double for in user_properties table",
    "row103": "Verify that data receiver API with userproperties schema and csv format is ignoring case while verifying duplication for property_value_double in user_properties",
    "row104": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_date column",
    "row105": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_date but given in any other property_value Type Column",
    "row106": "Verify that data receiver API with userproperties schema and csv format is not inserting duplicate data property_value_date for in user_properties table",
    "row107": "Verify that data receiver API with userproperties schema and csv format is ignoring case while verifying duplication for property_value_date in user_properties",
    "row108": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_currency column",
    "row109": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_currency but given in any other property_value Type Column",
    "row110": "Verify that data receiver API with userproperties schema and csv format is not inserting duplicate data property_value_currency for in user_properties table",
    "row111": "Verify that data receiver API with userproperties schema and csv format is ignoring case while verifying duplication for property_value_currency in user_properties",
    "row112": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_bool column",
    "row113": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_bool but given in any other property_value Type Column",
    "row114": "Verify that data receiver API with userproperties schema and csv format is not inserting duplicate data property_value_bool for in user_properties table",
    "row115": "Verify that data receiver API with userproperties schema and csv format is ignoring case while verifying duplication for property_value_bool in user_properties",
    "row116": "Verify that data receiver API with userproperties schema and csv is ingesting data correctly in user_properties by filling property_value_json column",
    "row117": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_json column with json array",
    "row118": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_json but given in any other property_value Type Column",
    "row119": "Verify that data receiver API with userproperties schema and csv format is not inserting duplicate data property_value_json for in user_properties table",
    # JSON format rows 120–152 (code row = Excel row - 1)
    # row133 intentionally absent — Excel row 134 = 'file size >1GB, not tested'
    "row120": "Verify that data receiver API with userproperties schema and json format is ingesting data in all the user_properties table",
    "row121": "Verify that data receiver API with userproperties schema and json format is ingesting bsent=false and modified date for all the newly added properties",
    "row122": "Verify that data receiver API with userproperties schema and json format is ingesting bsent=false and modified date for all the updated properties with all datatypes",
    "row123": "Verify that data receiver API with userproperties schema and json format is trimming spaces from all the fields while ingesting data in user_properties",
    "row124": "Verify that data receiver API with userproperties schema in json format is throwing error in case of missing ClientUserId column",
    "row125": "Verify that data receiver API with userproperties schema in json format is throwing error in case of missing PropertyName column",
    "row126": "Verify that data receiver API with userproperties schema in json format is throwing error in case of missing property_value column",
    "row127": "Verify that data receiver API with userproperties schema in json format is not entering data for the row in which ClientUserId column is missing and enter remaining data",
    "row128": "Verify that data receiver API with userproperties schema in json format is not entering data for the row in which ClientUserId column is empty or null and enter remaining rows data in which valid ClientUserId",
    "row129": "Verify that data receiver API with userproperties schema in json format is not entering data for the row in which PropertyName column is missing and enter remaining data",
    "row130": "Verify that data receiver API with userproperties schema in json format is not entering data for the row in which PropertyName column is  empty or null and enter remaining data",
    "row131": "Verify that data receiver API with userproperties schema in json format is entering data for the ClientUserId which is not present in client_user_mapping",
    "row132": "Verify that data receiver API with userproperties schema in json format is not entering data for the row in which property_value column is missing and enter remaining data",
    "row134": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value Text column",
    "row135": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value is Text but given in any other  property_value Type Column",
    "row136": "Verify that data receiver API with userproperties schema and json format is not inserting duplicate data property_value Text in user_properties table",
    "row137": "Verify that data receiver API with userproperties schema and json format is ignoring case while verifying duplication for property_value Text in user_properties",
    "row138": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_double column",
    "row139": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing data property_value_double but given  in any other  property_value Type Column",
    "row140": "Verify that data receiver API with userproperties schema and json format is not inserting duplicate data property_value_double for  in user_properties table",
    "row141": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_date column",
    "row142": "Verify that data receiver API with userproperties schema is ingesting data in user_properties where existing property_value_date but given  in any other  property_value Type Column",
    "row143": "Verify that data receiver API with userproperties schema and json format is not inserting duplicate data property_value_date for  in user_properties table",
    "row144": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_currency column",
    "row145": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_currency but given  in any other property_value Type Column",
    "row146": "Verify that data receiver API with userproperties schema and json format is not inserting duplicate data property_value_currency for  in user_properties table",
    "row147": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_bool column",
    "row148": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_bool but given  in any other  property_value Type Column",
    "row149": "Verify that data receiver API with userproperties schema and json format is not inserting duplicate data property_value_bool for  in user_properties table",
    "row150": "Verify that data receiver API with userproperties schema is ingesting data correctly in user_properties by filling property_value_json column",
    "row151": "Verify that data receiver API with userproperties schema is not ingesting data in user_properties where existing property_value_json but given  in any other  property_value Type Column",
    "row152": "Verify that data receiver API with userproperties schema and json format is not inserting duplicate data property_value_json for in user_properties table",
}

_session_run_id: str | None = None
_report_meta: dict[str, dict] = {}
_coverage_summary: dict[str, list[int]] = {"present": [], "missing": [], "extra": []}
_case_results: dict[str, dict[str, list[dict[str, str | bool]]]] = {}
_case_db_identifiers: dict[str, dict[str, list[str]]] = {}


# ── Session run ID ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def run_id() -> str:
    global _session_run_id
    _session_run_id = uuid.uuid4().hex[:8].upper()
    return _session_run_id


# ── Per-test unique identifiers ────────────────────────────────────────────────

_user_counter = 0

@pytest.fixture
def unique_user_id(run_id) -> str:
    global _user_counter
    _user_counter += 1
    return f"AT_USER_{run_id}_{_user_counter:04d}"

@pytest.fixture
def unique_signal_name(run_id) -> str:
    return f"AT_SIG_{run_id}_{int(time.time())}"


# conftest.py — fix unique_property_name to avoid same-second collisions
_prop_counter = 0

@pytest.fixture
def unique_property_name(run_id) -> str:
    global _prop_counter
    _prop_counter += 1
    return f"AT_PROP_{run_id}_{_prop_counter:04d}"


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
    total = 0
    for key, table in snapshot.items():
        if isinstance(table, list):          # raw_signals — flat list
            total += len(table)
        else:                                # all others — dict of lists
            total += sum(len(v) for v in table.values())
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
    """Return the exact Excel title for the test case, falling back to the docstring."""
    case = _case_key(item)
    if case in TC_TITLES:
        return TC_TITLES[case]
    # Fallback: use the class or function docstring
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


def _bucket_status(case: str, bucket: str) -> str:
    entries = _case_results.get(case, {}).get(bucket, [])
    if not entries:
        return "N/A"
    if any(not e.get("passed") for e in entries):
        return "FAIL"
    return "PASS"


def _bucket_reason(case: str, bucket: str) -> str:
    entries = _case_results.get(case, {}).get(bucket, [])
    if not entries:
        return "N/A"
    failed = [str(e.get("reason", "")).strip() for e in entries if not bool(e.get("passed"))]
    if failed:
        return failed[0]
    passed_count = sum(1 for e in entries if bool(e.get("passed")))
    return f"All {bucket.upper()} checks passed ({passed_count})"


def _final_verdict_for_case(case: str) -> str:
    db = _bucket_status(case, "db")
    api = _bucket_status(case, "api")
    # Any failure in any bucket = case fails
    if "FAIL" in (db, api):
        return "FAIL"
    if "PASS" in (db, api):
        return "PASS"
    return "N/A"


def _failure_diagnosis(case: str) -> str:
    """
    Returns a single plain-English verdict for non-technical readers:
    - What was checked (API status + DB presence)
    - If failed: exactly what went wrong in simple terms
    """
    api_status = _bucket_status(case, "api")
    db_status  = _bucket_status(case, "db")
    api_reason = _bucket_reason(case, "api")
    db_reason  = _bucket_reason(case, "db")

    # ── Both passed ──────────────────────────────────────────────────────
    if api_status == "PASS" and db_status == "PASS":
        return "API returned expected status ✓  |  Database record verified ✓"
    if api_status == "PASS" and db_status == "N/A":
        return "API returned expected status ✓  (no DB check for this case)"
    if db_status == "PASS" and api_status == "N/A":
        return "Database record verified ✓  (no API check for this case)"

    # ── Failures — translate to plain English ────────────────────────────
    if api_status == "FAIL" and db_status in ("PASS", "N/A"):
        return f"FAILED — API did not return the expected response. Detail: {api_reason}"

    if db_status == "FAIL" and api_status == "PASS":
        reason_lower = db_reason.lower()
        if "not in user_properties" in reason_lower or "no user_properties" in reason_lower:
            return "FAILED — API accepted the request but data was NOT saved to the database."
        if "should not be inserted" in reason_lower or "should not be in" in reason_lower:
            return "FAILED — Data that should have been rejected was saved to the database."
        if "space trimming failed" in reason_lower or "padded" in reason_lower:
            return "FAILED — Record was saved with leading/trailing spaces instead of being trimmed."
        if "duplicate" in reason_lower:
            return "FAILED — Duplicate rows were stored in the database (deduplication is not working)."
        if "text wins" in reason_lower or "preserved" in reason_lower:
            return f"FAILED — Wrong data type column was populated in the database. Detail: {db_reason}"
        if "assert []" in db_reason:
            return "FAILED — API accepted the request but the record was NOT saved to the database."
        if "assert not [" in db_reason:
            return "FAILED — A record that should have been rejected was found in the database."
        if ".get" in db_reason:
            return "FAILED — Record is in the database but the expected field value was not populated (wrong data type column)."
        return "FAILED — Database check failed (unexpected assertion result)."

    if api_status == "FAIL" and db_status == "FAIL":
        return (
            f"FAILED — Both API and DB checks failed. "
            f"API: {api_reason}  |  DB: {db_reason}"
        )

    if db_status == "FAIL" and api_status == "N/A":
            if "assert []" in db_reason:
                return "FAILED — API accepted the request but the record was NOT saved to the database."
            if "assert not [" in db_reason:
                return "FAILED — A record that should have been rejected was found in the database."
            if ".get" in db_reason:
                return "FAILED — Record is in the database but the expected field value was not populated (wrong data type column)."
            return "FAILED — Database check failed (unexpected assertion result)."

    return "N/A"

def _case_order(item: pytest.Item) -> str:
    row = _extract_row_number(item.nodeid)
    if row is not None:
        return f"{row:03d}"
    class_doc = (getattr(item.cls, "__doc__", "") or "").strip().upper()
    tc = re.search(r"\bTC-[A-Z]+-(\d+)\b", class_doc)
    if tc:
        return f"{int(tc.group(1)):03d}"
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


# ── Rows covered by each suite ────────────────────────────────────────────────
_SIGNAL_ROWS    = set(range(5, 75)) - {18, 22, 31, 42, 50, 70}   # practical skips; 70 = >1 GB manual
_UP_ROWS        = set(range(76, 153))

def pytest_collection_finish(session: pytest.Session):
    present = sorted({
        row
        for item in session.items
        for row in [_extract_row_number(item.nodeid)]
        if row is not None
    })
    present_set = set(present)

    # Work out expected based on which test files were collected
    collected_files = {item.fspath.basename for item in session.items}
    expected: set[int] = set()
    if any("signal" in f for f in collected_files):
        expected |= _SIGNAL_ROWS
    if any("phase1_api" in f or "phase2_db" in f for f in collected_files):
        expected |= _UP_ROWS

    missing = sorted(expected - present_set)
    extra   = sorted(present_set - expected)
    _coverage_summary["present"] = present
    _coverage_summary["missing"] = missing
    _coverage_summary["extra"]   = extra

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    outcome = yield
    report = outcome.get_result()
    if report.when != "call":
        return

    layer = _layer(item)
    if report.failed:
        default_reason = f"{layer} failed"
        longrepr = str(getattr(report, "longreprtext", "") or getattr(report, "longrepr", "") or "")
        reason = longrepr.strip().splitlines()[-1] if longrepr.strip() else default_reason
    elif report.passed:
        reason = f"{layer} passed"
    else:
        reason = f"{layer} skipped"

    case = _case_key(item)
    bucket = "db" if layer == "DB check" else "api"
    passed = bool(report.passed)
    _case_results.setdefault(case, {"api": [], "db": []})[bucket].append({
        "passed": passed,
        "reason": reason,
    })

    _report_meta[item.nodeid] = {
        "case_key":    case,
        "case_order":  _case_order(item),
        "description": _description(item),
        "test_data":   _extract_test_data(item),
        "layer":       layer,
        "reason":      reason,
        "sql":         _sql_query(item),
        # ← removed: api_check, db_check, final_verdict, diagnosis
        #    these are always recomputed live in pytest_html_results_table_row
    }
    # ── Auto-collect DB identifiers from test instance ─────────────────────
    if layer == "DB check":
        inst = getattr(item, "instance", None)
        if inst:
            id_entry = _case_db_identifiers.setdefault(
                case, {"should_have": [], "should_not_have": []}
            )
            for attr_name in vars(inst):
                val = getattr(inst, attr_name, None)
                if not isinstance(val, str):
                    continue
                if not val.upper().startswith("AT_USER_"):
                    continue
                val_upper = val.upper()
                bucket_key = (
                    "should_not_have"
                    if ("_BAD" in val_upper or "_INVALID" in val_upper)
                    else "should_have"
                )
                if val not in id_entry[bucket_key]:
                    id_entry[bucket_key].append(val)


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_header(cells):
    cells[:] = []

@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_row(report, cells):
    cells[:] = []


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_summary(prefix, summary, postfix):
    # Iterate over ALL row* keys in TC_TITLES — not just ones that ran.
    # Rows with no results show as NOT AUTOMATED.
    all_tc_rows = sorted(
        [k for k in TC_TITLES if re.match(r'^row\d+$', k)],
        key=lambda c: int(c[3:])
    )

    passed_cases  = [c for c in all_tc_rows if _final_verdict_for_case(c) == "PASS"]
    failed_cases  = [c for c in all_tc_rows if _final_verdict_for_case(c) == "FAIL"]
    na_cases      = [c for c in all_tc_rows if _final_verdict_for_case(c) == "N/A"]

    rows_html = []
    for case in all_tc_rows:
        title   = TC_TITLES.get(case, case)
        verdict = _final_verdict_for_case(case)
        diag    = _failure_diagnosis(case)

        if verdict == "PASS":
            color, label = "green", "PASS"
        elif verdict == "FAIL":
            color, label = "red", "FAIL"
        else:
            color, label = "gray", "NOT AUTOMATED"
            diag = "No test exists for this case yet."

        # DB identifier columns
        ids = _case_db_identifiers.get(case, {})
        should_have     = ", ".join(ids.get("should_have", []))     or "—"
        should_not_have = ", ".join(ids.get("should_not_have", [])) or "—"

        rows_html.append(
            f"<tr>"
            f"<td style='padding:4px 8px'>{escape(title)}</td>"
            f"<td style='padding:4px 12px;color:{color};font-weight:bold'>{escape(label)}</td>"
            f"<td style='padding:4px 8px'>{escape(diag)}</td>"
            f"<td style='padding:4px 8px;font-family:monospace;font-size:11px'>{escape(should_have)}</td>"
            f"<td style='padding:4px 8px;font-family:monospace;font-size:11px'>{escape(should_not_have)}</td>"
            f"</tr>"
        )

    summary_table = (
        "<h2>Test Results Summary</h2>"
        f"<p>"
        f"<b>Total Excel rows:</b> {len(all_tc_rows)} &nbsp;|&nbsp; "
        f"<b style='color:green'>PASSED: {len(passed_cases)}</b> &nbsp;|&nbsp; "
        f"<b style='color:red'>FAILED: {len(failed_cases)}</b> &nbsp;|&nbsp; "
        f"<b style='color:gray'>NOT AUTOMATED: {len(na_cases)}</b>"
        f"</p>"
        "<table border='1' cellpadding='0' cellspacing='0' "
        "style='border-collapse:collapse;width:100%;font-size:12px'>"
        "<thead><tr style='background:#f0f0f0'>"
        "<th style='padding:6px 8px;text-align:left;min-width:350px'>Test Case Title (from Excel)</th>"
        "<th style='padding:6px 12px;min-width:120px'>Result</th>"
        "<th style='padding:6px 8px;text-align:left'>Outcome / Failure Reason</th>"
        "<th style='padding:6px 8px;text-align:left;min-width:160px'>Should Have Record</th>"
        "<th style='padding:6px 8px;text-align:left;min-width:160px'>Shouldn&#39;t Have Record</th>"
        "</tr></thead>"
        "<tbody>" + "".join(rows_html) + "</tbody>"
        "</table>"
    )

    prefix.extend([summary_table])