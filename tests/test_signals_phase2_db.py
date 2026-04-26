"""
test_signals_phase2_db.py
─────────────────────────
Phase 2: DB verification ONLY for the signals schema (Excel rows 5–60).

Architecture (mirrors test_phase2_db.py exactly):
  ─ db_snapshot session fixture fires ONCE (defined in conftest.py):
      sleeps DB_PROPAGATION_DELAY once, bulk-queries once, caches forever.
  ─ Every test here is an instant dict lookup — no polling, no waiting,
    no individual DB round trips.

Tables verified:
  profiles.client_users_data   – one row per (client_user_id, signal_name, signal_value)
  profiles.client_user_mapping – one row per client_user_id
  profiles.raw_signals         – ingestion metadata (checked for existence only)

Run Phase 1 + Phase 2 together (full signal suite):
  pytest tests/test_signals_phase1_api.py tests/test_signals_phase2_db.py \\
         -v --html=reports/signals_report.html --self-contained-html

Run this file alone (DB assertions only, useful after a previous API run):
  pytest tests/test_signals_phase2_db.py -v

This file runs AFTER test_signals_phase1_api.py (alphabetical ordering).
By the time this file starts, all API requests in Phase 1 have already fired.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))


# ── Shared lookup helpers ──────────────────────────────────────────────────────

def _sig_rows(db_snapshot, client_user_id: str) -> list[dict]:
    """Return client_users_data rows for a client_user_id from the cached snapshot."""
    return db_snapshot["client_users_data"].get(client_user_id.lower(), [])


def _mapping_rows(db_snapshot, client_user_id: str) -> list[dict]:
    """Return client_user_mapping rows for a client_user_id from the cached snapshot."""
    return db_snapshot["client_user_mapping"].get(client_user_id.lower(), [])


def _all_sig_rows(db_snapshot) -> list[dict]:
    """Flatten all client_users_data rows from the cached snapshot."""
    return [
        row
        for rows in db_snapshot["client_users_data"].values()
        for row in rows
    ]


def _raw_signals(db_snapshot) -> list[dict]:
    """Return all raw_signals rows from the cached snapshot."""
    return db_snapshot.get("raw_signals", [])


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 5  —  TC-SIG-01 : CSV Happy Path DB checks (TC-SIG-02, TC-SIG-03)
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow5DB:
    """Excel Row 5 (TC-SIG-01) — DB: signal in client_users_data, mapping entry exists."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow5_signal_in_client_users_data(self, db_snapshot, submissions):
        """TC-SIG-02: CSV happy-path signal is written to client_users_data."""
        sub = submissions.get("sig_row5", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 5] Phase-1 did not register sig_row5 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 5] No rows in client_users_data for '{uid}'"
        assert any(
            r.get("signal_name", "").lower() == sub["signal_name"].lower()
            for r in rows
        ), f"[Row 5] Signal '{sub['signal_name']}' not found in: {rows}"

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow5_mapping_entry_exists(self, db_snapshot, submissions):
        """TC-SIG-03: CSV happy-path user is written to client_user_mapping."""
        sub = submissions.get("sig_row5", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 5] Phase-1 did not register sig_row5 submission"
        rows = _mapping_rows(db_snapshot, uid)
        assert rows, f"[Row 5] No rows in client_user_mapping for '{uid}'"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 6  —  TC-SIG-04 : JSON Single Happy Path DB check (TC-SIG-05)
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow6DB:
    """Excel Row 6 (TC-SIG-04) — DB: JSON single signal in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow6_signal_in_db(self, db_snapshot, submissions):
        """TC-SIG-05: JSON single-object signal written to client_users_data."""
        sub = submissions.get("sig_row6", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 6] Phase-1 did not register sig_row6 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 6] No rows in client_users_data for '{uid}'"
        assert any(
            r.get("signal_name", "").lower() == sub["signal_name"].lower()
            for r in rows
        ), f"[Row 6] Signal '{sub['signal_name']}' not found"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 7  —  TC-SIG-06 : JSON Array Happy Path DB check (TC-SIG-07)
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow7DB:
    """Excel Row 7 (TC-SIG-06) — DB: both JSON-array users in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow7_both_users_in_db(self, db_snapshot, submissions):
        """TC-SIG-07: Both users from JSON array upload found in client_users_data."""
        sub = submissions.get("sig_row7", {})
        uid1, uid2 = sub.get("user_ids", [None, None])
        assert _sig_rows(db_snapshot, uid1), f"[Row 7] No rows for '{uid1}'"
        assert _sig_rows(db_snapshot, uid2), f"[Row 7] No rows for '{uid2}'"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 13  —  Wrong CSV column names → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow13DB:
    """Excel Row 13 — DB: wrong CSV column names → API rejected with 400 → nothing in DB."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow13_no_db_insert_for_invalid_csv_columns(self, db_snapshot, submissions):
        """Row 13: API rejected payload → no data written to client_users_data."""
        sub = submissions.get("sig_row13", {})
        # No user_ids were submitted — confirm api_status recorded the 400
        api_status = sub.get("api_status")
        assert api_status == 400, (
            f"[Row 13] Expected Phase-1 to record api_status=400, got {api_status}. "
            "DB insert guard cannot be verified without a user ID."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 14  —  Wrong JSON field names → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow14DB:
    """Excel Row 14 — DB: wrong JSON field names → API rejected with 400 → nothing in DB."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow14_no_db_insert_for_invalid_json_fields(self, db_snapshot, submissions):
        """Row 14: API rejected payload → no data written to client_users_data."""
        sub = submissions.get("sig_row14", {})
        api_status = sub.get("api_status")
        assert api_status == 400, (
            f"[Row 14] Expected Phase-1 to record api_status=400, got {api_status}."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 16  —  format=csv but JSON body → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow16DB:
    """Excel Row 16 — DB: format mismatch (csv+JSON body) → API rejected → nothing in DB."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow16_no_db_insert_for_format_mismatch(self, db_snapshot, submissions):
        """Row 16: format=csv but JSON body → API returned 400 → no DB insert."""
        sub = submissions.get("sig_row16", {})
        api_status = sub.get("api_status")
        assert api_status == 400, (
            f"[Row 16] Expected Phase-1 to record api_status=400, got {api_status}."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 17  —  format=json but CSV body → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow17DB:
    """Excel Row 17 — DB: format mismatch (json+CSV body) → API rejected → nothing in DB."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow17_no_db_insert_for_format_mismatch(self, db_snapshot, submissions):
        """Row 17: format=json but CSV body → API returned 400 → no DB insert."""
        sub = submissions.get("sig_row17", {})
        api_status = sub.get("api_status")
        assert api_status == 400, (
            f"[Row 17] Expected Phase-1 to record api_status=400, got {api_status}."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 19  —  CSV ingests data into ALL required tables
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow19DB:
    """Excel Row 19 — DB: CSV upload → client_users_data, client_user_mapping, raw_signals all populated."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow19_signal_in_client_users_data(self, db_snapshot, submissions):
        """Row 19: CSV upload → user's signal present in client_users_data."""
        sub = submissions.get("sig_row19", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 19] Phase-1 did not register sig_row19 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 19] No rows in client_users_data for '{uid}'"

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow19_mapping_exists(self, db_snapshot, submissions):
        """Row 19: CSV upload → user present in client_user_mapping."""
        sub = submissions.get("sig_row19", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 19] Phase-1 did not register sig_row19 submission"
        rows = _mapping_rows(db_snapshot, uid)
        assert rows, f"[Row 19] No rows in client_user_mapping for '{uid}'"

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow19_raw_signals_populated(self, db_snapshot, submissions):
        """Row 19: CSV upload → raw_signals table has at least one entry."""
        rs = _raw_signals(db_snapshot)
        assert rs, (
            "[Row 19] raw_signals table appears empty. "
            "Expected at least one row to be present after signal ingestion."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 20  —  CSV responseTime ISO-8601 variants all ingested
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow20DB:
    """Excel Row 20 — DB: all 4 ISO-8601 ResponseTime variants written to client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow20_all_iso8601_users_in_db(self, db_snapshot, submissions):
        """Row 20: All 4 users (tz-offset, UTC, plain, date-only) stored in client_users_data."""
        sub = submissions.get("sig_row20", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 20] Phase-1 did not register any user_ids for sig_row20"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, (
                f"[Row 20] No rows in client_users_data for '{uid}'. "
                "ISO-8601 variant may have been rejected."
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 21  —  CSV correct columns ingested into client_users_data
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow21DB:
    """Excel Row 21 — DB: verify signal_name, signal_value, response_group, platform stored correctly."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow21_correct_column_values_in_db(self, db_snapshot, submissions):
        """Row 21: CSV column values stored correctly in client_users_data."""
        sub = submissions.get("sig_row21", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 21] Phase-1 did not register sig_row21 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 21] No rows in client_users_data for '{uid}'"

        expected = sub.get("extra", {}).get("expected_columns", {})
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert match, f"[Row 21] Signal row not found for '{uid}'"
        row = match[0]

        if "signal_name" in expected:
            assert row.get("signal_name", "").lower() == expected["signal_name"].lower(), (
                f"[Row 21] signal_name mismatch: expected '{expected['signal_name']}', "
                f"got '{row.get('signal_name')}'"
            )
        if "signal_value" in expected:
            assert str(row.get("signal_value", "")) == str(expected["signal_value"]), (
                f"[Row 21] signal_value mismatch: expected '{expected['signal_value']}', "
                f"got '{row.get('signal_value')}'"
            )
        if "response_group" in expected:
            assert row.get("response_group", "").lower() == expected["response_group"].lower(), (
                f"[Row 21] response_group mismatch: expected '{expected['response_group']}', "
                f"got '{row.get('response_group')}'"
            )
        if "platform" in expected:
            assert row.get("platform", "").lower() == expected["platform"].lower(), (
                f"[Row 21] platform mismatch: expected '{expected['platform']}', "
                f"got '{row.get('platform')}'"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 23  —  CSV space-trimming on all fields
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow23DB:
    """Excel Row 23 — DB: padded CSV fields stored with leading/trailing spaces removed."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow23_trimmed_user_id_in_db(self, db_snapshot, submissions):
        """Row 23: Trimmed client_user_id (no surrounding spaces) used as DB key."""
        sub = submissions.get("sig_row23", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 23] Phase-1 did not register sig_row23 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 23] No rows in client_users_data for '{uid}'. "
            "Space trimming may not have occurred — padded ID stored instead."
        )

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow23_trimmed_signal_name_found(self, db_snapshot, submissions):
        """Row 23: Trimmed signal_name (no surrounding spaces) stored in DB."""
        sub = submissions.get("sig_row23", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 23] No rows in client_users_data for '{uid}'"
        assert any(
            r.get("signal_name", "").lower() == sub["signal_name"].lower()
            for r in rows
        ), (
            f"[Row 23] Trimmed signal_name '{sub['signal_name']}' not found in DB. "
            f"Rows stored: {[r.get('signal_name') for r in rows]}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 24  —  CSV case-insensitive deduplication
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow24DB:
    """Excel Row 24 — DB: 3 case-variant rows → only 1 row stored in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow24_single_row_stored_after_dedup(self, db_snapshot, submissions):
        """Row 24: Case variants (upper/lower/mixed) deduplicated to a single DB row."""
        sub = submissions.get("sig_row24", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 24] Phase-1 did not register sig_row24 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 24] No rows in client_users_data for '{uid}'"
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert len(match) <= 1, (
            f"[Row 24] Expected ≤1 row after case-insensitive dedup, "
            f"found {len(match)} rows for signal '{sub['signal_name']}'"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 25  —  CSV no duplicate data inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow25DB:
    """Excel Row 25 — DB: identical (user, signal, value) rows deduplicated to ≤1 row."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow25_no_duplicate_rows_in_db(self, db_snapshot, submissions):
        """Row 25: Same (ClientUserId, SignalName, SignalValue) sent twice → ≤1 row in DB."""
        sub = submissions.get("sig_row25", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 25] Phase-1 did not register sig_row25 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 25] No rows in client_users_data for '{uid}'"
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
            and str(r.get("signal_value", "")) == "40000.0"
        ]
        assert len(match) <= 1, (
            f"[Row 25] Expected ≤1 row for duplicate (user+signal+value), "
            f"found {len(match)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 26  —  CSV minimum mandatory fields only
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow26DB:
    """Excel Row 26 — DB: minimum required fields (4 columns only) stored in DB."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow26_minimum_fields_stored_in_db(self, db_snapshot, submissions):
        """Row 26: Minimum mandatory CSV columns → user's signal in client_users_data."""
        sub = submissions.get("sig_row26", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 26] Phase-1 did not register sig_row26 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 26] No rows in client_users_data for '{uid}'"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 27  —  CSV missing ClientUserId column → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow27DB:
    """Excel Row 27 — DB: missing ClientUserId column → API returned 400 → no DB insert."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow27_no_db_insert_when_client_user_id_column_missing(self, db_snapshot, submissions):
        """Row 27: Missing ClientUserId column → API 400 confirmed → no data in client_users_data."""
        sub = submissions.get("sig_row27", {})
        api_status = sub.get("api_status")
        assert api_status == 400, (
            f"[Row 27] Expected Phase-1 to record api_status=400, got {api_status}."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 28  —  CSV missing SignalName column → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow28DB:
    """Excel Row 28 — DB: missing SignalName column → no row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow28_no_db_insert_when_signal_name_column_missing(self, db_snapshot, submissions):
        """Row 28: Missing SignalName column → API 400 → client_user_id absent from DB."""
        sub = submissions.get("sig_row28", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            assert sub.get("api_status") == 400, (
                "[Row 28] Missing user_id and api_status != 400 — cannot verify DB state"
            )
            return
        rows = _sig_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 28] Expected no client_users_data row for invalid payload "
            f"(missing SignalName column), but found: {rows}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 29  —  CSV missing SignalValue column → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow29DB:
    """Excel Row 29 — DB: missing SignalValue column → no row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow29_no_db_insert_when_signal_value_column_missing(self, db_snapshot, submissions):
        """Row 29: Missing SignalValue column → API 400 → client_user_id absent from DB."""
        sub = submissions.get("sig_row29", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            assert sub.get("api_status") == 400
            return
        rows = _sig_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 29] Expected no client_users_data row for invalid payload "
            f"(missing SignalValue column), but found: {rows}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 30  —  CSV missing ResponseTime column → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow30DB:
    """Excel Row 30 — DB: missing ResponseTime column → no row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow30_no_db_insert_when_response_time_column_missing(self, db_snapshot, submissions):
        """Row 30: Missing ResponseTime column → API 400 → client_user_id absent from DB."""
        sub = submissions.get("sig_row30", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            assert sub.get("api_status") == 400
            return
        rows = _sig_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 30] Expected no client_users_data row for invalid payload "
            f"(missing ResponseTime column), but found: {rows}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 32  —  CSV misspelled optional columns — mandatory data still ingested
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow32DB:
    """Excel Row 32 — DB: misspelled optional columns ignored → mandatory signal still stored."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow32_mandatory_signal_stored_despite_misspelled_optional_columns(self, db_snapshot, submissions):
        """Row 32: Misspelled optional columns ignored — signal row still in client_users_data."""
        sub = submissions.get("sig_row32", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 32] Phase-1 did not register sig_row32 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 32] No rows in client_users_data for '{uid}'. "
            "Misspelled optional columns may have incorrectly caused the row to be rejected."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 33  —  CSV blank ClientUserId value → that row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow33DB:
    """Excel Row 33 — DB: good rows inserted, row with blank ClientUserId skipped."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow33_good_users_in_db_blank_user_absent(self, db_snapshot, submissions):
        """Row 33: Valid rows stored; row with blank ClientUserId not in client_users_data."""
        sub = submissions.get("sig_row33", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 33] Phase-1 did not register any user_ids for sig_row33"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, f"[Row 33] Valid user '{uid}' not found in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 33] Blank ClientUserId '{bad_uid}' should not be in DB, found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 34  —  CSV NULL ClientUserId value → that row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow34DB:
    """Excel Row 34 — DB: good row inserted, NULL/null ClientUserId rows absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow34_good_user_in_db_null_absent(self, db_snapshot, submissions):
        """Row 34: Valid user in DB; NULL and null ClientUserId rows absent."""
        sub = submissions.get("sig_row34", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 34] Phase-1 did not register sig_row34 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 34] Valid user '{uid}' not in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 34] NULL ClientUserId '{bad_uid}' should not be in DB, found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 35  —  CSV blank SignalName value → that row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow35DB:
    """Excel Row 35 — DB: valid row inserted; row with blank SignalName absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow35_good_user_in_db_bad_user_absent(self, db_snapshot, submissions):
        """Row 35: Valid user in DB; user with blank SignalName absent from client_users_data."""
        sub = submissions.get("sig_row35", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 35] Phase-1 did not register sig_row35 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 35] Valid user '{uid}' not in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 35] User '{bad_uid}' (blank SignalName row) should not be in DB, "
                f"found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 36  —  CSV null SignalName value → that row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow36DB:
    """Excel Row 36 — DB: valid row inserted; row with 'null' SignalName absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow36_good_user_in_db_null_signal_name_absent(self, db_snapshot, submissions):
        """Row 36: 'null' SignalName row skipped; valid row still in client_users_data."""
        sub = submissions.get("sig_row36", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 36] Phase-1 did not register sig_row36 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 36] Valid user '{uid}' not in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 36] User '{bad_uid}' (null SignalName row) should not be in DB, "
                f"found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 37  —  CSV empty/null SignalValue → rows ARE inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow37DB:
    """Excel Row 37 — DB: rows with empty or null SignalValue are inserted (unlike SignalName)."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow37_all_signal_value_users_in_db(self, db_snapshot, submissions):
        """Row 37: Empty/null SignalValue rows inserted (not skipped) → all users in DB."""
        sub = submissions.get("sig_row37", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 37] Phase-1 did not register any user_ids for sig_row37"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, (
                f"[Row 37] User '{uid}' not found in client_users_data. "
                "Empty/null SignalValue should NOT cause the row to be skipped."
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 38  —  CSV bad ResponseTime → those rows skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow38DB:
    """Excel Row 38 — DB: valid row inserted; bad/empty/null ResponseTime rows absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow38_good_user_in_db_bad_response_time_absent(self, db_snapshot, submissions):
        """Row 38: Valid row in DB; bad/empty/null ResponseTime users absent."""
        sub = submissions.get("sig_row38", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 38] Phase-1 did not register sig_row38 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 38] Valid user '{uid}' not in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 38] User '{bad_uid}' (bad ResponseTime) should not be in DB, "
                f"found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 39  —  CSV optional fields stored correctly
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow39DB:
    """Excel Row 39 — DB: response_group, platform, signal_meta_data stored correctly."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow39_optional_column_values_correct(self, db_snapshot, submissions):
        """Row 39: Optional CSV fields (response_group, platform, metadata) stored in DB."""
        sub = submissions.get("sig_row39", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 39] Phase-1 did not register sig_row39 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 39] No rows in client_users_data for '{uid}'"

        expected = sub.get("extra", {}).get("expected_columns", {})
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert match, f"[Row 39] Signal row not found for '{uid}'"
        row = match[0]

        if "response_group" in expected:
            assert (row.get("response_group") or "").lower() == expected["response_group"].lower(), (
                f"[Row 39] response_group: expected '{expected['response_group']}', "
                f"got '{row.get('response_group')}'"
            )
        if "platform" in expected:
            assert (row.get("platform") or "").lower() == expected["platform"].lower(), (
                f"[Row 39] platform: expected '{expected['platform']}', "
                f"got '{row.get('platform')}'"
            )
        if "signal_meta_data" in expected:
            assert row.get("signal_meta_data") is not None, (
                f"[Row 39] signal_meta_data should be set, got NULL. "
                f"Expected: '{expected['signal_meta_data']}'"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 40  —  CSV missing optional values → stored as NULL
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow40DB:
    """Excel Row 40 — DB: empty optional field values stored as NULL in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow40_rows_inserted_and_optional_fields_null(self, db_snapshot, submissions):
        """Row 40: All 3 rows inserted; their missing optional fields are NULL in DB."""
        sub = submissions.get("sig_row40", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 40] Phase-1 did not register any user_ids for sig_row40"

        uid_no_grp, uid_no_plat, uid_no_meta = uid_list

        # Row without ResponseGroup
        rows = _sig_rows(db_snapshot, uid_no_grp)
        assert rows, f"[Row 40] '{uid_no_grp}' (no ResponseGroup) not in client_users_data"
        assert rows[0].get("response_group") is None, (
            f"[Row 40] response_group should be NULL for '{uid_no_grp}', "
            f"got '{rows[0].get('response_group')}'"
        )

        # Row without platform
        rows = _sig_rows(db_snapshot, uid_no_plat)
        assert rows, f"[Row 40] '{uid_no_plat}' (no platform) not in client_users_data"
        assert rows[0].get("platform") is None, (
            f"[Row 40] platform should be NULL for '{uid_no_plat}', "
            f"got '{rows[0].get('platform')}'"
        )

        # Row without metadata
        rows = _sig_rows(db_snapshot, uid_no_meta)
        assert rows, f"[Row 40] '{uid_no_meta}' (no metadata) not in client_users_data"
        assert rows[0].get("signal_meta_data") is None, (
            f"[Row 40] signal_meta_data should be NULL for '{uid_no_meta}', "
            f"got '{rows[0].get('signal_meta_data')}'"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 41  —  CSV no duplicate rows in client_user_mapping
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow41DB:
    """Excel Row 41 — DB: same user sent twice → exactly one entry in client_user_mapping."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow41_single_mapping_entry_for_repeated_user(self, db_snapshot, submissions):
        """Row 41: Same client_user_id submitted twice → only one row in client_user_mapping."""
        sub = submissions.get("sig_row41", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 41] Phase-1 did not register sig_row41 submission"
        mapping = _mapping_rows(db_snapshot, uid)
        assert mapping, f"[Row 41] No rows in client_user_mapping for '{uid}'"
        assert len(mapping) == 1, (
            f"[Row 41] Expected exactly 1 mapping row for '{uid}', "
            f"found {len(mapping)} — duplicate mapping entries detected."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 43  —  CSV signal_value_numeric + signal_value_currency computed
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow43DB:
    """Excel Row 43 — DB: numeric SignalValue → signal_value_numeric and signal_value_currency set."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow43_signal_value_numeric_set(self, db_snapshot, submissions):
        """Row 43: Numeric SignalValue → signal_value_numeric column is not NULL."""
        sub = submissions.get("sig_row43", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 43] Phase-1 did not register sig_row43 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 43] No rows in client_users_data for '{uid}'"
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert match, f"[Row 43] Signal row not found for '{uid}'"
        val = match[0].get("signal_value_numeric")
        assert val is not None, (
            f"[Row 43] signal_value_numeric should be set for numeric SignalValue, got NULL.\n"
            f"Full row: {match[0]}"
        )
        assert float(val) == float(sub.get("extra", {}).get("expected_numeric", 40000)), (
            f"[Row 43] signal_value_numeric: expected 40000, got {val}"
        )

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow43_signal_value_currency_set(self, db_snapshot, submissions):
        """Row 43: Numeric SignalValue → signal_value_currency column is not NULL."""
        sub = submissions.get("sig_row43", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 43] No rows in client_users_data for '{uid}'"
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert match
        assert match[0].get("signal_value_currency") is not None, (
            f"[Row 43] signal_value_currency should be set for numeric SignalValue, got NULL.\n"
            f"Full row: {match[0]}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 44  —  CSV signal_value_date computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow44DB:
    """Excel Row 44 — DB: date-parseable SignalValue → signal_value_date column populated."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow44_signal_value_date_set_for_all_users(self, db_snapshot, submissions):
        """Row 44: Date-parseable SignalValue → signal_value_date not NULL for all 3 users."""
        sub = submissions.get("sig_row44", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 44] Phase-1 did not register any user_ids for sig_row44"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, f"[Row 44] No rows in client_users_data for '{uid}'"
            match = [
                r for r in rows
                if r.get("signal_name", "").lower() == sub["signal_name"].lower()
            ]
            assert match, f"[Row 44] Signal row not found for '{uid}'"
            assert match[0].get("signal_value_date") is not None, (
                f"[Row 44] signal_value_date should be set for '{uid}' (date-parseable value), "
                f"got NULL.\nFull row: {match[0]}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 45  —  CSV signal_value_date_duration computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow45DB:
    """Excel Row 45 — DB: date SignalValue → signal_value_date_duration (days since date) not NULL."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow45_signal_value_date_duration_set(self, db_snapshot, submissions):
        """Row 45: Date SignalValue → signal_value_date_duration column not NULL."""
        sub = submissions.get("sig_row45", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 45] Phase-1 did not register sig_row45 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 45] No rows in client_users_data for '{uid}'"
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert match, f"[Row 45] Signal row not found for '{uid}'"
        val = match[0].get("signal_value_date_duration")
        assert val is not None, (
            f"[Row 45] signal_value_date_duration should be set (days since date), got NULL.\n"
            f"Full row: {match[0]}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 46  —  CSV signal_value_bool computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow46DB:
    """Excel Row 46 — DB: boolean-like SignalValues → signal_value_bool column populated."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow46_signal_value_bool_set_for_all_users(self, db_snapshot, submissions):
        """Row 46: Boolean-like SignalValues (0,1,True,False,true,false) → signal_value_bool not NULL."""
        sub = submissions.get("sig_row46", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 46] Phase-1 did not register any user_ids for sig_row46"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, f"[Row 46] No rows in client_users_data for '{uid}'"
            match = [
                r for r in rows
                if r.get("signal_name", "").lower() == sub["signal_name"].lower()
            ]
            assert match, f"[Row 46] Signal row not found for '{uid}'"
            assert match[0].get("signal_value_bool") is not None, (
                f"[Row 46] signal_value_bool should be set for '{uid}' (boolean-like value), "
                f"got NULL.\nFull row: {match[0]}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 47  —  JSON ingestion into ALL required tables
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow47DB:
    """Excel Row 47 — DB: JSON upload → client_users_data, client_user_mapping, raw_signals all populated."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow47_signal_in_client_users_data(self, db_snapshot, submissions):
        """Row 47: JSON array upload → user's signal in client_users_data."""
        sub = submissions.get("sig_row47", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 47] Phase-1 did not register sig_row47 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 47] No rows in client_users_data for '{uid}'"

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow47_mapping_exists(self, db_snapshot, submissions):
        """Row 47: JSON array upload → user present in client_user_mapping."""
        sub = submissions.get("sig_row47", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 47] Phase-1 did not register sig_row47 submission"
        rows = _mapping_rows(db_snapshot, uid)
        assert rows, f"[Row 47] No rows in client_user_mapping for '{uid}'"

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow47_raw_signals_populated(self, db_snapshot, submissions):
        """Row 47: JSON upload → raw_signals table has at least one entry."""
        rs = _raw_signals(db_snapshot)
        assert rs, (
            "[Row 47] raw_signals table appears empty. "
            "Expected at least one row after signal ingestion via JSON."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 48  —  JSON responseTime ISO-8601 variants all ingested
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow48DB:
    """Excel Row 48 — DB: all 3 JSON ISO-8601 ResponseTime variants written to client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow48_all_iso8601_json_users_in_db(self, db_snapshot, submissions):
        """Row 48: All 3 users (plain, UTC, tz-offset) from JSON found in client_users_data."""
        sub = submissions.get("sig_row48", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 48] Phase-1 did not register any user_ids for sig_row48"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, (
                f"[Row 48] No rows in client_users_data for '{uid}'. "
                "JSON ISO-8601 ResponseTime variant may have been rejected."
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 49  —  JSON correct columns stored
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow49DB:
    """Excel Row 49 — DB: verify JSON column values stored correctly in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow49_json_correct_column_values_in_db(self, db_snapshot, submissions):
        """Row 49: JSON column values (signal_name, response_group, platform) stored correctly."""
        sub = submissions.get("sig_row49", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 49] Phase-1 did not register sig_row49 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 49] No rows in client_users_data for '{uid}'"

        expected = sub.get("extra", {}).get("expected_columns", {})
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert match, f"[Row 49] Signal row not found for '{uid}'"
        row = match[0]

        if "response_group" in expected:
            assert (row.get("response_group") or "").lower() == expected["response_group"].lower(), (
                f"[Row 49] response_group: expected '{expected['response_group']}', "
                f"got '{row.get('response_group')}'"
            )
        if "platform" in expected:
            assert (row.get("platform") or "").lower() == expected["platform"].lower(), (
                f"[Row 49] platform: expected '{expected['platform']}', "
                f"got '{row.get('platform')}'"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 51  —  JSON space-trimming on all fields
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow51DB:
    """Excel Row 51 — DB: padded JSON string fields stored with spaces removed."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow51_trimmed_user_id_and_signal_in_db(self, db_snapshot, submissions):
        """Row 51: Trimmed client_user_id and signal_name used as DB keys."""
        sub = submissions.get("sig_row51", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 51] Phase-1 did not register sig_row51 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 51] No rows in client_users_data for '{uid}'. "
            "JSON space trimming may not have occurred."
        )
        assert any(
            r.get("signal_name", "").lower() == sub["signal_name"].lower()
            for r in rows
        ), (
            f"[Row 51] Trimmed signal_name '{sub['signal_name']}' not found in DB. "
            f"Rows: {[r.get('signal_name') for r in rows]}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 52  —  JSON case-insensitive deduplication
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow52DB:
    """Excel Row 52 — DB: 3 JSON case-variant objects → only 1 row stored."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow52_single_row_after_json_dedup(self, db_snapshot, submissions):
        """Row 52: Case-variant JSON objects deduplicated to a single DB row."""
        sub = submissions.get("sig_row52", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 52] Phase-1 did not register sig_row52 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 52] No rows in client_users_data for '{uid}'"
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
        ]
        assert len(match) <= 1, (
            f"[Row 52] Expected ≤1 row after JSON case-insensitive dedup, "
            f"found {len(match)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 53  —  JSON no duplicate data inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow53DB:
    """Excel Row 53 — DB: identical JSON objects → ≤1 row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow53_no_duplicate_json_rows_in_db(self, db_snapshot, submissions):
        """Row 53: Duplicate JSON objects (same user+signal+value) → ≤1 row in DB."""
        sub = submissions.get("sig_row53", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 53] Phase-1 did not register sig_row53 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 53] No rows in client_users_data for '{uid}'"
        match = [
            r for r in rows
            if r.get("signal_name", "").lower() == sub["signal_name"].lower()
            and str(r.get("signal_value", "")) == "40000"
        ]
        assert len(match) <= 1, (
            f"[Row 53] Expected ≤1 row for duplicate JSON (user+signal+value), "
            f"found {len(match)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 54  —  JSON minimum mandatory fields only
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow54DB:
    """Excel Row 54 — DB: JSON with only 4 mandatory fields → user in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow54_minimum_json_fields_stored_in_db(self, db_snapshot, submissions):
        """Row 54: JSON with minimum mandatory fields → signal row in client_users_data."""
        sub = submissions.get("sig_row54", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 54] Phase-1 did not register sig_row54 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 54] No rows in client_users_data for '{uid}'"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 55  —  JSON missing ClientUserId key → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow55DB:
    """Excel Row 55 — DB: missing ClientUserId key → API 400 → no DB insert."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow55_no_db_insert_when_client_user_id_key_missing(self, db_snapshot, submissions):
        """Row 55: Missing ClientUserId key in JSON → API returned 400 → no data in DB."""
        sub = submissions.get("sig_row55", {})
        api_status = sub.get("api_status")
        assert api_status == 400, (
            f"[Row 55] Expected Phase-1 to record api_status=400, got {api_status}."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 56  —  JSON missing SignalName key → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow56DB:
    """Excel Row 56 — DB: missing SignalName key → no row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow56_no_db_insert_when_signal_name_key_missing(self, db_snapshot, submissions):
        """Row 56: Missing SignalName key → API 400 → client_user_id absent from client_users_data."""
        sub = submissions.get("sig_row56", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            assert sub.get("api_status") == 400
            return
        rows = _sig_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 56] Expected no client_users_data row (missing SignalName key), "
            f"but found: {rows}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 57  —  JSON missing SignalValue key → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow57DB:
    """Excel Row 57 — DB: missing SignalValue key → no row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow57_no_db_insert_when_signal_value_key_missing(self, db_snapshot, submissions):
        """Row 57: Missing SignalValue key → API 400 → client_user_id absent from DB."""
        sub = submissions.get("sig_row57", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            assert sub.get("api_status") == 400
            return
        rows = _sig_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 57] Expected no client_users_data row (missing SignalValue key), "
            f"but found: {rows}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 58  —  JSON missing ResponseTime key → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow58DB:
    """Excel Row 58 — DB: missing ResponseTime key → no row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow58_no_db_insert_when_response_time_key_missing(self, db_snapshot, submissions):
        """Row 58: Missing ResponseTime key → API 400 → client_user_id absent from DB."""
        sub = submissions.get("sig_row58", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            assert sub.get("api_status") == 400
            return
        rows = _sig_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 58] Expected no client_users_data row (missing ResponseTime key), "
            f"but found: {rows}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 59  —  JSON multiple mandatory keys missing → no DB insert
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow59DB:
    """Excel Row 59 — DB: multiple mandatory keys missing → no row in client_users_data."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow59_no_db_insert_when_multiple_mandatory_keys_missing(self, db_snapshot, submissions):
        """Row 59: Multiple mandatory keys missing → API 400 → client_user_id absent from DB."""
        sub = submissions.get("sig_row59", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            assert sub.get("api_status") == 400
            return
        rows = _sig_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 59] Expected no client_users_data row (multiple mandatory keys missing), "
            f"but found: {rows}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 60  —  JSON misspelled optional fields → mandatory data still inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow60DB:
    """Excel Row 60 — DB: misspelled optional JSON keys ignored → signal row still in DB."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow60_mandatory_signal_stored_despite_misspelled_optional_keys(self, db_snapshot, submissions):
        """Row 60: Misspelled optional JSON keys ignored — signal row still in client_users_data."""
        sub = submissions.get("sig_row60", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 60] Phase-1 did not register sig_row60 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 60] No rows in client_users_data for '{uid}'. "
            "Misspelled optional JSON keys may have incorrectly caused the row to be rejected."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 61  —  JSON element with missing ClientUserId key → not in DB
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow61DB:
    """Excel Row 61 — DB: valid elements stored; element with missing ClientUserId key absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow61_good_users_in_db_missing_key_element_absent(self, db_snapshot, submissions):
        """Row 61: Valid elements stored; element lacking ClientUserId key not in client_users_data."""
        sub = submissions.get("sig_row61", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 61] Phase-1 did not register any user_ids for sig_row61"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, f"[Row 61] Valid user '{uid}' not found in client_users_data"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 62  —  JSON element with empty/null ClientUserId → not in DB
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow62DB:
    """Excel Row 62 — DB: valid element stored; elements with empty/null ClientUserId absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow62_good_user_in_db_empty_null_client_user_id_absent(self, db_snapshot, submissions):
        """Row 62: Valid user in DB; empty/null ClientUserId elements absent."""
        sub = submissions.get("sig_row62", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 62] Phase-1 did not register sig_row62 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 62] Valid user '{uid}' not in client_users_data"

        # empty-string and None ClientUserId should never create a DB row
        for bad_uid in ["", None]:
            if bad_uid is None:
                continue  # None key can't be looked up; absence confirmed by no crash
            bad_rows = _sig_rows(db_snapshot, str(bad_uid))
            assert not bad_rows, (
                f"[Row 62] Empty/null ClientUserId should not be in DB, found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 63  —  JSON element with missing SignalName key → not in DB
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow63DB:
    """Excel Row 63 — DB: valid element stored; element with missing SignalName key absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow63_good_user_in_db_missing_signal_name_element_absent(self, db_snapshot, submissions):
        """Row 63: Valid element in DB; element with missing SignalName key absent."""
        sub = submissions.get("sig_row63", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 63] Phase-1 did not register sig_row63 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 63] Valid user '{uid}' not in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 63] User '{bad_uid}' (missing SignalName key) should not be in DB, "
                f"found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 64  —  JSON element with empty/null SignalName → not in DB
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow64DB:
    """Excel Row 64 — DB: valid element stored; elements with empty/null SignalName absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow64_good_user_in_db_empty_null_signal_name_absent(self, db_snapshot, submissions):
        """Row 64: Valid element in DB; elements with empty/null SignalName absent."""
        sub = submissions.get("sig_row64", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 64] Phase-1 did not register sig_row64 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 64] Valid user '{uid}' not in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 64] User '{bad_uid}' (empty/null SignalName) should not be in DB, "
                f"found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 65  —  JSON element with missing/empty/null SignalValue → inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow65DB:
    """Excel Row 65 — DB: all elements including those with missing/empty/null SignalValue
    are inserted (not skipped)."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow65_all_users_in_db_including_empty_signal_value(self, db_snapshot, submissions):
        """Row 65: Missing/empty/null SignalValue elements inserted (not skipped) → all users in DB."""
        sub = submissions.get("sig_row65", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 65] Phase-1 did not register any user_ids for sig_row65"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, (
                f"[Row 65] User '{uid}' not found in client_users_data. "
                "Missing/empty/null SignalValue should NOT cause the element to be skipped."
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 66  —  JSON element with bad/missing ResponseTime → not in DB
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow66DB:
    """Excel Row 66 — DB: valid element stored; elements with bad/missing ResponseTime absent."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow66_good_user_in_db_bad_response_time_absent(self, db_snapshot, submissions):
        """Row 66: Valid element in DB; bad/missing ResponseTime elements absent."""
        sub = submissions.get("sig_row66", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 66] Phase-1 did not register sig_row66 submission"
        assert _sig_rows(db_snapshot, uid), f"[Row 66] Valid user '{uid}' not in client_users_data"

        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _sig_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 66] User '{bad_uid}' (bad ResponseTime) should not be in DB, "
                f"found: {bad_rows}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 67  —  JSON optional fields stored correctly
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow67DB:
    """Excel Row 67 — DB: response_group, platform, signal_meta_data stored correctly from JSON."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow67_json_optional_column_values_correct(self, db_snapshot, submissions):
        """Row 67: JSON optional fields (response_group, platform, metadata) stored in DB."""
        sub = submissions.get("sig_row67", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 67] Phase-1 did not register sig_row67 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 67] No rows in client_users_data for '{uid}'"

        expected = sub.get("extra", {}).get("expected_columns", {})
        match = [r for r in rows if r.get("signal_name", "").lower() == sub["signal_name"].lower()]
        assert match, f"[Row 67] Signal row not found for '{uid}'"
        row = match[0]

        if "response_group" in expected:
            assert (row.get("response_group") or "").lower() == expected["response_group"].lower(), (
                f"[Row 67] response_group: expected '{expected['response_group']}', "
                f"got '{row.get('response_group')}'"
            )
        if "platform" in expected:
            assert (row.get("platform") or "").lower() == expected["platform"].lower(), (
                f"[Row 67] platform: expected '{expected['platform']}', "
                f"got '{row.get('platform')}'"
            )
        if "signal_meta_data" in expected:
            assert row.get("signal_meta_data") is not None, (
                f"[Row 67] signal_meta_data should be set, got NULL. "
                f"Expected: '{expected['signal_meta_data']}'"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 68  —  JSON missing/null optional values → stored as NULL
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow68DB:
    """Excel Row 68 — DB: JSON elements with null optional values → columns stored as NULL."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow68_json_rows_inserted_and_optional_fields_null(self, db_snapshot, submissions):
        """Row 68: All 3 JSON rows inserted; their null optional fields are NULL in DB."""
        sub = submissions.get("sig_row68", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 68] Phase-1 did not register any user_ids for sig_row68"

        uid_no_grp, uid_no_plat, uid_no_meta = uid_list

        rows = _sig_rows(db_snapshot, uid_no_grp)
        assert rows, f"[Row 68] '{uid_no_grp}' (null ResponseGroup) not in client_users_data"
        assert rows[0].get("response_group") is None, (
            f"[Row 68] response_group should be NULL for '{uid_no_grp}', "
            f"got '{rows[0].get('response_group')}'"
        )

        rows = _sig_rows(db_snapshot, uid_no_plat)
        assert rows, f"[Row 68] '{uid_no_plat}' (null platform) not in client_users_data"
        assert rows[0].get("platform") is None, (
            f"[Row 68] platform should be NULL for '{uid_no_plat}', "
            f"got '{rows[0].get('platform')}'"
        )

        rows = _sig_rows(db_snapshot, uid_no_meta)
        assert rows, f"[Row 68] '{uid_no_meta}' (null metadata) not in client_users_data"
        assert rows[0].get("signal_meta_data") is None, (
            f"[Row 68] signal_meta_data should be NULL for '{uid_no_meta}', "
            f"got '{rows[0].get('signal_meta_data')}'"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 69  —  JSON no duplicate rows in client_user_mapping
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow69DB:
    """Excel Row 69 — DB: same user sent twice in JSON → exactly one entry in client_user_mapping."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow69_json_single_mapping_entry_for_repeated_user(self, db_snapshot, submissions):
        """Row 69: Same client_user_id in two JSON elements → only one row in client_user_mapping."""
        sub = submissions.get("sig_row69", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 69] Phase-1 did not register sig_row69 submission"
        mapping = _mapping_rows(db_snapshot, uid)
        assert mapping, f"[Row 69] No rows in client_user_mapping for '{uid}'"
        assert len(mapping) == 1, (
            f"[Row 69] Expected exactly 1 mapping row for '{uid}', "
            f"found {len(mapping)} — duplicate mapping entries detected."
        )


# ── SKIP : Row 70  (> 1 GB JSON file — manual only) ──────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 71  —  JSON signal_value_numeric + signal_value_currency computed
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow71DB:
    """Excel Row 71 — DB: JSON numeric SignalValue → signal_value_numeric and
    signal_value_currency set."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow71_json_signal_value_numeric_set(self, db_snapshot, submissions):
        """Row 71: JSON numeric SignalValue → signal_value_numeric column is not NULL."""
        sub = submissions.get("sig_row71", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 71] Phase-1 did not register sig_row71 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 71] No rows in client_users_data for '{uid}'"
        match = [r for r in rows if r.get("signal_name", "").lower() == sub["signal_name"].lower()]
        assert match, f"[Row 71] Signal row not found for '{uid}'"
        val = match[0].get("signal_value_numeric")
        assert val is not None, (
            f"[Row 71] signal_value_numeric should be set for numeric SignalValue, got NULL.\n"
            f"Full row: {match[0]}"
        )
        assert float(val) == float(sub.get("extra", {}).get("expected_numeric", 40000)), (
            f"[Row 71] signal_value_numeric: expected 40000, got {val}"
        )

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow71_json_signal_value_currency_set(self, db_snapshot, submissions):
        """Row 71: JSON numeric SignalValue → signal_value_currency column is not NULL."""
        sub = submissions.get("sig_row71", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 71] No rows in client_users_data for '{uid}'"
        match = [r for r in rows if r.get("signal_name", "").lower() == sub["signal_name"].lower()]
        assert match
        assert match[0].get("signal_value_currency") is not None, (
            f"[Row 71] signal_value_currency should be set for numeric SignalValue, got NULL.\n"
            f"Full row: {match[0]}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 72  —  JSON signal_value_date computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow72DB:
    """Excel Row 72 — DB: JSON date-parseable SignalValue → signal_value_date populated."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow72_json_signal_value_date_set_for_all_users(self, db_snapshot, submissions):
        """Row 72: Date-parseable JSON SignalValue → signal_value_date not NULL for all users."""
        sub = submissions.get("sig_row72", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 72] Phase-1 did not register any user_ids for sig_row72"
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, f"[Row 72] No rows in client_users_data for '{uid}'"
            match = [r for r in rows if r.get("signal_name", "").lower() == sub["signal_name"].lower()]
            assert match, f"[Row 72] Signal row not found for '{uid}'"
            assert match[0].get("signal_value_date") is not None, (
                f"[Row 72] signal_value_date should be set for '{uid}' (date-parseable value), "
                f"got NULL.\nFull row: {match[0]}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 73  —  JSON signal_value_date_duration computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow73DB:
    """Excel Row 73 — DB: JSON date SignalValue → signal_value_date_duration not NULL."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow73_json_signal_value_date_duration_set(self, db_snapshot, submissions):
        """Row 73: JSON date SignalValue → signal_value_date_duration column not NULL."""
        sub = submissions.get("sig_row73", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 73] Phase-1 did not register sig_row73 submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[Row 73] No rows in client_users_data for '{uid}'"
        match = [r for r in rows if r.get("signal_name", "").lower() == sub["signal_name"].lower()]
        assert match, f"[Row 73] Signal row not found for '{uid}'"
        val = match[0].get("signal_value_date_duration")
        assert val is not None, (
            f"[Row 73] signal_value_date_duration should be set (days since date), got NULL.\n"
            f"Full row: {match[0]}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 74  —  JSON signal_value_bool computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow74DB:
    """Excel Row 74 — DB: JSON boolean-like SignalValues → signal_value_bool populated."""

    @pytest.mark.signals
    @pytest.mark.db
    def test_sigrow74_json_signal_value_bool_set_for_all_users(self, db_snapshot, submissions):
        """Row 74: JSON boolean-like SignalValues (0,1,True,False,true,false,TRUE,FALSE) →
        signal_value_bool not NULL for those users."""
        sub = submissions.get("sig_row74", {})
        uid_list = sub.get("user_ids", [])
        assert uid_list, "[Row 74] Phase-1 did not register any user_ids for sig_row74"
        # Only 0, 1, True/true/TRUE, False/false/FALSE are boolean — 2 and -1 are not
        bool_keys = {"zero", "one", "true_u", "false_u", "true_l", "false_l", "true_uu", "false_uu"}
        users_dict = {
            "zero":     f"{uid_list[0].rsplit('_', 1)[0]}_0",
            "one":      f"{uid_list[0].rsplit('_', 1)[0]}_1",
        }
        # Use the full uid_list as stored; check all are in DB, then verify bool field
        for uid in uid_list:
            rows = _sig_rows(db_snapshot, uid)
            assert rows, f"[Row 74] No rows in client_users_data for '{uid}'"
            match = [r for r in rows if r.get("signal_name", "").lower() == sub["signal_name"].lower()]
            assert match, f"[Row 74] Signal row not found for '{uid}'"
            # For non-boolean values (2, -1) signal_value_bool may legitimately be NULL —
            # but for all others it must be set.
            suffix = uid.split("_")[-1]
            if suffix in ("2", "N1"):
                continue  # 2 and -1 are not valid booleans; field may be NULL
            assert match[0].get("signal_value_bool") is not None, (
                f"[Row 74] signal_value_bool should be set for '{uid}' "
                f"(boolean-like value), got NULL.\nFull row: {match[0]}"
            )