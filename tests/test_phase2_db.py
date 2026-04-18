"""
test_phase2_db.py
─────────────────
Phase 2: DB verification only.

On first use, `db_snapshot` (session fixture from conftest.py):
  1. Sleeps DB_PROPAGATION_DELAY ONCE
  2. Runs ONE bulk SQL query for the whole run
  3. Returns a lookup dict cached for the entire session

Every test here is an instant dict lookup — no polling, no waiting,
no individual DB round trips.

This file runs AFTER test_phase1_api.py (alphabetical ordering).
By the time this file starts, all API requests in Phase 1 have already fired.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))


# ── Shared lookup helper ───────────────────────────────────────────────────────

def _up_rows(db_snapshot, client_user_id: str) -> list[dict]:
    """Return user_properties rows for a client_user_id from the cached snapshot."""
    return db_snapshot["user_properties"].get(client_user_id.lower(), [])


def _sig_rows(db_snapshot, client_user_id: str) -> list[dict]:
    """Return client_users_data rows for a client_user_id from the cached snapshot."""
    return db_snapshot["client_users_data"].get(client_user_id.lower(), [])


def _mapping_rows(db_snapshot, client_user_id: str) -> list[dict]:
    """Return client_user_mapping rows for a client_user_id from the cached snapshot."""
    return db_snapshot["client_user_mapping"].get(client_user_id.lower(), [])


def _all_up_rows(db_snapshot) -> list[dict]:
    """Flatten all user_properties rows from the cached snapshot."""
    return [
        row
        for rows in db_snapshot["user_properties"].values()
        for row in rows
    ]


# ══════════════════════════════════════════════════════════════════════════════
#  HAPPY PATH DB CHECKS
# ══════════════════════════════════════════════════════════════════════════════

class TestHappySignalsCSVDB:
    """TC-SIG-02, TC-SIG-03"""

    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_tc_sig_02_signal_in_client_users_data(self, db_snapshot, submissions):
        sub = submissions.get("happy_sig_csv", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "Phase-1 did not register happy_sig_csv submission"
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[TC-SIG-02] No rows in client_users_data for '{uid}'"
        assert any(
            r.get("signal_name", "").lower() == sub["signal_name"].lower()
            for r in rows
        ), f"[TC-SIG-02] Signal '{sub['signal_name']}' not found in: {rows}"

    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_tc_sig_03_mapping_exists(self, db_snapshot, submissions):
        sub = submissions.get("happy_sig_csv", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "Phase-1 did not register happy_sig_csv submission"
        rows = _mapping_rows(db_snapshot, uid)
        assert rows, f"[TC-SIG-03] No rows in client_user_mapping for '{uid}'"


class TestHappySignalsJSONSingleDB:
    """TC-SIG-05"""

    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_tc_sig_05_signal_in_db(self, db_snapshot, submissions):
        sub = submissions.get("happy_sig_json_single", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid
        rows = _sig_rows(db_snapshot, uid)
        assert rows, f"[TC-SIG-05] No rows in client_users_data for '{uid}'"
        assert any(
            r.get("signal_name", "").lower() == sub["signal_name"].lower()
            for r in rows
        ), f"[TC-SIG-05] Signal not found"


class TestHappySignalsJSONArrayDB:
    """TC-SIG-07"""

    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_tc_sig_07_both_users_in_db(self, db_snapshot, submissions):
        sub = submissions.get("happy_sig_json_array", {})
        uid1, uid2 = sub.get("user_ids", [None, None])
        assert _sig_rows(db_snapshot, uid1), f"[TC-SIG-07] No rows for '{uid1}'"
        assert _sig_rows(db_snapshot, uid2), f"[TC-SIG-07] No rows for '{uid2}'"


class TestHappyUpCSVDB:
    """TC-UP-02"""

    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.db
    def test_tc_up_02_property_in_db(self, db_snapshot, submissions):
        sub = submissions.get("happy_up_csv", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[TC-UP-02] No rows in user_properties for '{uid}'"
        assert any(
            r.get("property_name", "").lower() == sub["property_name"].lower()
            for r in rows
        ), f"[TC-UP-02] Property not found in: {rows}"


class TestHappyUpJSONSingleDB:
    """TC-UP-04"""

    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.db
    def test_tc_up_04_property_in_db(self, db_snapshot, submissions):
        sub = submissions.get("happy_up_json_single", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[TC-UP-04] No rows in user_properties for '{uid}'"


class TestHappyUpJSONArrayDB:
    """TC-UP-06"""

    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.db
    def test_tc_up_06_both_users_in_db(self, db_snapshot, submissions):
        sub = submissions.get("happy_up_json_array", {})
        uid1, uid2 = sub.get("user_ids", [None, None])
        assert _up_rows(db_snapshot, uid1), f"[TC-UP-06] No rows for '{uid1}'"
        assert _up_rows(db_snapshot, uid2), f"[TC-UP-06] No rows for '{uid2}'"


# ══════════════════════════════════════════════════════════════════════════════
#  REGRESSION DB CHECKS — rows 76–119
# ══════════════════════════════════════════════════════════════════════════════

class TestRow76DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row76_db_has_record(self, db_snapshot, submissions):
        sub = submissions.get("76", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 76] No user_properties row for '{uid}'"
        assert any(
            r.get("property_name", "").lower() == sub["property_name"].lower()
            for r in rows
        ), f"[Row 76] Property not found"


class TestRow77DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row77_absent_is_false_and_modified_date_set(self, db_snapshot, submissions):
        sub = submissions.get("77", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 77] No user_properties row for '{uid}'"
        row = rows[0]
        assert row.get("bsent") is False, f"[Row 77] bsent should be False, got {row.get('bsent')}"
        assert row.get("modified_date") is not None, "[Row 77] modified_date should be set"


class TestRow78DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row78_absent_false_and_value_updated(self, db_snapshot, submissions):
        sub = submissions.get("78", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 78] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 78] Property row not found"
        row = match[0]
        assert row.get("property_value") == "UpdatedValue", (
            f"[Row 78] Expected 'UpdatedValue' after update, got '{row.get('property_value')}'"
        )


class TestRow79DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row79_valid_rows_inserted(self, db_snapshot, submissions):
        sub = submissions.get("79", {})
        uid1, uid2 = sub.get("user_ids", [None, None])
        assert _up_rows(db_snapshot, uid1), f"[Row 79] G1 user '{uid1}' not in user_properties"
        assert _up_rows(db_snapshot, uid2), f"[Row 79] G2 user '{uid2}' not in user_properties"


class TestRow80DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row80_trimmed_user_id_and_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("80", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 80] No user_properties row for '{uid}'. "
            "Space trimming may have changed the stored ID."
        )
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 80] Property not found"
        assert match[0].get("property_value") == "SpacedValue", (
            f"[Row 80] Expected trimmed value 'SpacedValue', got '{match[0].get('property_value')}'"
        )


# Negative-path DB assertions:
# if these payloads are invalid, no user_properties rows should be inserted.

class TestRow81DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row81_no_db_insert_when_client_user_id_column_missing(self, db_snapshot, submissions):
        sub = submissions.get("81", {})
        prop = (sub.get("property_name") or "").lower()
        assert prop, "[Row 81] Missing property_name from Phase-1 submission metadata"
        matches = [
            r for r in _all_up_rows(db_snapshot)
            if (r.get("property_name", "") or "").lower() == prop
        ]
        assert not matches, (
            "[Row 81] Expected no user_properties row for invalid payload "
            f"(missing ClientUserId column), but found: {matches}"
        )


class TestRow82DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row82_no_db_insert_when_property_name_missing(self, db_snapshot, submissions):
        sub = submissions.get("82", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 82] Missing user_id from Phase-1 submission metadata"
        rows = _up_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 82] Expected no user_properties row for invalid payload (missing PropertyName), "
            f"but found: {rows}"
        )


class TestRow83DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row83_no_db_insert_when_property_value_missing(self, db_snapshot, submissions):
        sub = submissions.get("83", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 83] Missing user_id from Phase-1 submission metadata"
        rows = _up_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 83] Expected no user_properties row for invalid payload (missing PropertyValue), "
            f"but found: {rows}"
        )


class TestRow91DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row91_no_db_insert_when_duplicate_columns(self, db_snapshot, submissions):
        sub = submissions.get("91", {})
        uid = sub.get("user_ids", [None])[0]
        assert uid, "[Row 91] Missing user_id from Phase-1 submission metadata"
        rows = _up_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 91] Expected no user_properties row for invalid payload (duplicate columns), "
            f"but found: {rows}"
        )


class TestRow84DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row84_valid_row_inserted_bad_skipped(self, db_snapshot, submissions):
        sub = submissions.get("84", {})
        uid = sub.get("user_ids", [None])[0]
        assert _up_rows(db_snapshot, uid), f"[Row 84] Valid row for '{uid}' not in user_properties"
        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _up_rows(db_snapshot, bad_uid)
            assert not bad_rows, f"[Row 84] Invalid user '{bad_uid}' should not be inserted: {bad_rows}"


class TestRow85DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row85_valid_row_inserted(self, db_snapshot, submissions):
        sub = submissions.get("85", {})
        uid = sub.get("user_ids", [None])[0]
        assert _up_rows(db_snapshot, uid), f"[Row 85] Valid row for '{uid}' not in user_properties"
        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _up_rows(db_snapshot, bad_uid)
            assert not bad_rows, f"[Row 85] Invalid user '{bad_uid}' should not be inserted: {bad_rows}"


class TestRow86DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row86_valid_row_inserted_bad_row_skipped(self, db_snapshot, submissions):
        sub = submissions.get("86", {})
        uid = sub.get("user_ids", [None])[0]
        assert _up_rows(db_snapshot, uid), f"[Row 86] Valid row for '{uid}' not in user_properties"
        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _up_rows(db_snapshot, bad_uid)
            assert not bad_rows, f"[Row 86] Invalid user '{bad_uid}' should not be inserted: {bad_rows}"


class TestRow87DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row87_valid_row_inserted(self, db_snapshot, submissions):
        sub = submissions.get("87", {})
        uid = sub.get("user_ids", [None])[0]
        assert _up_rows(db_snapshot, uid), f"[Row 87] Valid row for '{uid}' not in user_properties"
        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _up_rows(db_snapshot, bad_uid)
            assert not bad_rows, f"[Row 87] Invalid user '{bad_uid}' should not be inserted: {bad_rows}"


class TestRow88DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row88_valid_row_inserted_bad_skipped(self, db_snapshot, submissions):
        sub = submissions.get("88", {})
        uid = sub.get("user_ids", [None])[0]
        assert _up_rows(db_snapshot, uid), f"[Row 88] Valid row for '{uid}' not in user_properties"
        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            bad_rows = _up_rows(db_snapshot, bad_uid)
            assert not bad_rows, f"[Row 88] Invalid user '{bad_uid}' should not be inserted: {bad_rows}"


class TestRow89DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row89_empty_value_row_inserted(self, db_snapshot, submissions):
        sub = submissions.get("89", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 89] Row with empty PropertyValue not inserted for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 89] Property row not found"
        assert match[0].get("property_value") == "", (
            f"[Row 89] Expected empty string, got '{match[0].get('property_value')}'"
        )


class TestRow90DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row90_required_columns_inserted(self, db_snapshot, submissions):
        sub = submissions.get("90", {})
        uid = sub.get("user_ids", [None])[0]
        assert _up_rows(db_snapshot, uid), f"[Row 90] No user_properties row for '{uid}'"


class TestRow92DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row92_text_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("92", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 92] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 92] Property not found"
        assert match[0].get("property_value") == "SomeTextValue", (
            f"[Row 92] Expected 'SomeTextValue', got '{match[0].get('property_value')}'"
        )


class TestRow93DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row93_text_value_preserved_int_not_set(self, db_snapshot, submissions):
        sub = submissions.get("93", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 93] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 93] Property row not found"
        row = match[0]
        assert row.get("property_value") == "TextWins", (
            f"[Row 93] property_value should be 'TextWins', got '{row.get('property_value')}'"
        )


class TestRow94DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row94_double_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("94", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 94] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 94] Property not found"
        val = match[0].get("property_value_double")
        assert val is not None, f"[Row 94] property_value_double is NULL"
        assert float(val) == 99.5, f"[Row 94] Expected 99.5, got {val}"


class TestRow95DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row95_text_wins_over_double(self, db_snapshot, submissions):
        sub = submissions.get("95", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 95] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match
        assert match[0].get("property_value") == "TextWins", (
            f"[Row 95] property_value should be 'TextWins'"
        )


class TestRow96DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row96_int_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("96", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 96] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 96] Property not found"
        val = match[0].get("property_value_int")
        assert val is not None, f"[Row 96] property_value_int is NULL"
        assert int(val) == 42, f"[Row 96] Expected 42, got {val}"


class TestRow97DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row97_text_wins_over_int(self, db_snapshot, submissions):
        sub = submissions.get("97", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 97] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match
        assert match[0].get("property_value") == "TextWins", (
            f"[Row 97] property_value should be 'TextWins'"
        )


class TestRow98DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row98_no_duplicate_rows(self, db_snapshot, submissions):
        sub = submissions.get("98", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 98] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 98] Expected <=1 row for property, found {len(match)}"


class TestRow99DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row99_single_row_in_db(self, db_snapshot, submissions):
        sub = submissions.get("99", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 99] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").strip().lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 99] Expected <=1 row for property, found {len(match)}"


class TestRow100DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row100_double_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("100", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 100] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 100] Property not found"
        assert match[0].get("property_value_double") is not None, "[Row 100] property_value_double is NULL"


class TestRow101DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row101_double_preserved_text_not_set(self, db_snapshot, submissions):
        sub = submissions.get("101", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 101] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 101] Property not found"
        row = match[0]
        assert row.get("property_value_double") is not None, "[Row 101] property_value_double should be preserved"
        assert row.get("property_value") != "SomeTextValue", "[Row 101] Text override should not be applied"


class TestRow102DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row102_no_duplicate_rows(self, db_snapshot, submissions):
        sub = submissions.get("102", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 102] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 102] Expected <=1 row for property, found {len(match)}"


class TestRow103DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row103_single_row_in_db(self, db_snapshot, submissions):
        sub = submissions.get("103", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 103] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").strip().lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 103] Expected <=1 row for property, found {len(match)}"


class TestRow104DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row104_date_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("104", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 104] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 104] Property not found"
        assert match[0].get("property_value_date") is not None, "[Row 104] property_value_date is NULL"


class TestRow105DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row105_date_preserved_text_not_set(self, db_snapshot, submissions):
        sub = submissions.get("105", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 105] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 105] Property not found"
        row = match[0]
        assert row.get("property_value_date") is not None, "[Row 105] property_value_date should be preserved"
        assert row.get("property_value") != "SomeTextValue", "[Row 105] Text override should not be applied"


class TestRow106DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row106_no_duplicate_rows(self, db_snapshot, submissions):
        sub = submissions.get("106", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 106] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 106] Expected <=1 row for property, found {len(match)}"


class TestRow107DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row107_single_row_in_db(self, db_snapshot, submissions):
        sub = submissions.get("107", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 107] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").strip().lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 107] Expected <=1 row for property, found {len(match)}"


class TestRow108DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row108_currency_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("108", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 108] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 108] Property not found"
        assert match[0].get("property_value_currency") is not None, "[Row 108] property_value_currency is NULL"


class TestRow109DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row109_currency_preserved_text_not_set(self, db_snapshot, submissions):
        sub = submissions.get("109", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 109] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 109] Property not found"
        row = match[0]
        assert row.get("property_value_currency") is not None, "[Row 109] property_value_currency should be preserved"
        assert row.get("property_value") != "SomeTextValue", "[Row 109] Text override should not be applied"


class TestRow110DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row110_no_duplicate_rows(self, db_snapshot, submissions):
        sub = submissions.get("110", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 110] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 110] Expected <=1 row for property, found {len(match)}"


class TestRow111DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row111_single_row_in_db(self, db_snapshot, submissions):
        sub = submissions.get("111", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 111] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").strip().lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 111] Expected <=1 row for property, found {len(match)}"


class TestRow112DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row112_bool_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("112", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 112] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 112] Property not found"
        assert match[0].get("property_value_bool") is not None, "[Row 112] property_value_bool is NULL"


class TestRow113DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row113_bool_preserved_text_not_set(self, db_snapshot, submissions):
        sub = submissions.get("113", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 113] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 113] Property not found"
        row = match[0]
        assert row.get("property_value_bool") is not None, "[Row 113] property_value_bool should be preserved"
        assert row.get("property_value") != "SomeTextValue", "[Row 113] Text override should not be applied"


class TestRow114DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row114_no_duplicate_rows(self, db_snapshot, submissions):
        sub = submissions.get("114", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 114] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 114] Expected <=1 row for property, found {len(match)}"


class TestRow115DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row115_single_row_in_db(self, db_snapshot, submissions):
        sub = submissions.get("115", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 115] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").strip().lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 115] Expected <=1 row for property, found {len(match)}"


class TestRow116DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row116_json_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("116", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 116] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 116] Property not found"
        val = match[0].get("property_value_json")
        assert val is not None, (
            f"[Row 116] property_value_json is NULL — backend may not write JSON column.\n"
            f"Full row: {match[0]}"
        )


class TestRow117DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row117_json_array_in_db(self, db_snapshot, submissions):
        sub = submissions.get("117", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 117] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match
        val = match[0].get("property_value_json")
        assert val is not None, f"[Row 117] property_value_json is NULL"
        assert isinstance(val, list), f"[Row 117] Expected JSON array, got {type(val)}"


class TestRow118DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row118_json_preserved_text_not_set(self, db_snapshot, submissions):
        sub = submissions.get("118", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 118] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match
        row = match[0]
        json_val = row.get("property_value_json")
        assert json_val is not None, (
            f"[Row 118] JSON value should be preserved, not NULL.\nFull row: {row}"
        )


class TestRow119DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row119_bool_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("119", {})
        uid = sub.get("user_ids", [None])[0]
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 119] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match
        val = match[0].get("property_value_bool")
        assert val is True, f"[Row 119] property_value_bool should be True, got {val}"
