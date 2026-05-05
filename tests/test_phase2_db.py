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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 78] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 78] Property row not found for property '{sub.get('property_name')}'"
        row = match[0]
        # FIX: method name said "absent_false_and_value_updated" but bsent + modified_date
        # were never actually asserted — only property_value was checked. Now all three are checked.
        assert row.get("bsent") is False, (
            f"[Row 78] bsent should be False after update, got '{row.get('bsent')}'"
        )
        assert row.get("modified_date") is not None, (
            "[Row 78] modified_date should be set (not null) after the update call"
        )
        assert row.get("property_value") == "UpdatedValue", (
            f"[Row 78] Expected 'UpdatedValue' after update, got '{row.get('property_value')}'"
        )


class TestRow79DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row79_valid_rows_inserted_bad_row_rejected(self, db_snapshot, submissions):
        sub = submissions.get("79", {})
        uid1, uid2 = sub.get("user_ids", [None, None])
        self.client_user_id = uid1
        self.client_user_id_2 = uid2

        # Valid rows must be present
        assert _up_rows(db_snapshot, uid1), (
            f"[Row 79] Good row G1 '{uid1}' is missing from user_properties — "
            "valid row was not inserted"
        )
        assert _up_rows(db_snapshot, uid2), (
            f"[Row 79] Good row G2 '{uid2}' is missing from user_properties — "
            "valid row was not inserted"
        )

        # Bad row (multi-datatype conflict) must NOT be present
        for bad_uid in sub.get("extra", {}).get("absent_user_ids", []):
            self.client_user_id_bad = bad_uid
            bad_rows = _up_rows(db_snapshot, bad_uid)
            assert not bad_rows, (
                f"[Row 79] BAD ROW '{bad_uid}' (had both PropertyValue + PropertyValueDouble) "
                f"was inserted when it should have been rejected. Found: {bad_rows}"
            )

class TestRow80DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row80_trimmed_user_id_and_value_in_db(self, db_snapshot, submissions):
        sub   = submissions.get("80", {})
        uid   = sub.get("user_ids", [None])[0]   # trimmed version — what SHOULD be in DB
        self.client_user_id = uid
        padded_uid = f"  {uid}  "                  # what would appear if trimming FAILED

        rows = _up_rows(db_snapshot, uid)

        if not rows:
            # Fallback: check whether the untrimmed (padded) UID was stored verbatim
            padded_rows = _up_rows(db_snapshot, padded_uid)
            if padded_rows:
                pytest.fail(
                    f"[Row 80] SPACE TRIMMING FAILED. "
                    f"Record was stored with spaces ('{padded_uid}') "
                    f"instead of the trimmed form ('{uid}'). "
                    f"Stored row: {padded_rows[0]}"
                )
            pytest.fail(
                f"[Row 80] No user_properties row found for either "
                f"trimmed UID '{uid}' or padded UID '{padded_uid}'. "
                "Record was not inserted at all."
            )

        match = [
            r for r in rows
            if r.get("property_name", "").lower() == sub["property_name"].lower()
        ]
        assert match, (
            f"[Row 80] UID '{uid}' is in DB but property '{sub['property_name']}' "
            "was not found in its rows"
        )
        stored_val = match[0].get("property_value")
        assert stored_val == "SpacedValue", (
            f"[Row 80] PropertyValue was not trimmed correctly. "
            f"Expected 'SpacedValue', got '{stored_val}'"
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
        self.client_user_id_bad = uid
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
        self.client_user_id_bad = uid
        assert uid, "[Row 83] Missing user_id from Phase-1 submission metadata"
        rows = _up_rows(db_snapshot, uid)
        assert not rows, (
            f"[Row 83] Expected no user_properties row for invalid payload (missing PropertyValue), "
            f"but found: {rows}"
        )


class TestExtraDuplicateColumnsDB:
    """EXTRA (not in Excel): When duplicate columns are rejected (400), no DB row should exist."""

    @pytest.mark.regression
    @pytest.mark.db
    def test_extra_dup_cols_no_db_insert(self, db_snapshot, submissions):
        sub = submissions.get("EXTRA_dup_cols", {})
        uid = sub.get("user_ids", [None])[0]
        if not uid:
            pytest.skip("EXTRA_dup_cols submission not found — API phase may have been skipped")
        rows = _up_rows(db_snapshot, uid)
        assert not rows, (
            f"[EXTRA] Expected no user_properties row for duplicate-column payload, "
            f"but found: {rows}"
        )


class TestRow84DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row84_valid_row_inserted_bad_skipped(self, db_snapshot, submissions):
        sub = submissions.get("84", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 90] No user_properties row for '{uid}'"


class TestRow92DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row92_text_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("92", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
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
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 93] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 93] Property row not found"
        row = match[0]

        # FIX: was hardcoded "TextWins" — that value only ever existed in the old
        # wrong simultaneous-send scenario. Now reads from submissions so it stays
        # in sync with whatever the API fixture actually submitted.
        expected_text = sub.get("extra", {}).get("expected_value", "ExistingTextValue")
        assert row.get("property_value") == expected_text, (
            f"[Row 93] property_value should still be '{expected_text}' (original text preserved), "
            f"got '{row.get('property_value')}'"
        )
        assert row.get("property_value_int") is None, (
            f"[Row 93] property_value_int should be NULL (int rejected when text already exists), "
            f"got {row.get('property_value_int')}"
        )

class TestRow94DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row94_no_duplicate_text_rows(self, db_snapshot, submissions):
        sub = submissions.get("94", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 94] No user_properties row for '{uid}' — "
            "record was not inserted at all"
        )
        match = [
            r for r in rows
            if r.get("property_name", "").lower() == sub["property_name"].lower()
        ]
        assert match, f"[Row 94] Property '{sub['property_name']}' not found for user '{uid}'"
        assert len(match) <= 1, (
            f"[Row 94] DUPLICATE ROWS DETECTED — expected at most 1 row for this property, "
            f"found {len(match)}: {match}"
        )

class TestRow95DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row95_single_row_after_case_insensitive_dedup(self, db_snapshot, submissions):
        sub = submissions.get("95", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 95] No user_properties row for '{uid}' — "
            "record was not inserted at all"
        )
        match = [
            r for r in rows
            if r.get("property_name", "").strip().lower() == sub["property_name"].lower()
        ]
        assert len(match) <= 1, (
            f"[Row 95] Case-insensitive dedup failed — space-padded rows were stored as "
            f"separate entries. Expected <=1 row, found {len(match)}: {match}"
        )


class TestRow96DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row96_int_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("96", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
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
    def test_row97_int_preserved_text_rejected(self, db_snapshot, submissions):
        """
        FIX: Previous assertion was completely backwards — was asserting text wins and int is NULL.
        Spec: when property_value_int already exists, a text-type submission must be rejected.
        Int column must be preserved. Text column must NOT contain the attempted value.
        """
        sub = submissions.get("97", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 97] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 97] Property row not found"
        row = match[0]

        # FIX: int must be preserved — was previously asserting it is NULL (completely wrong)
        assert row.get("property_value_int") is not None, (
            f"[Row 97] property_value_int should be preserved (was {sub['extra']['initial_int_value']}). "
            f"Got NULL — either the initial insert failed or type-protection is not working."
        )

        # FIX: text must NOT have been stored — was previously asserting text == "TextWins" (completely wrong)
        attempted_text = sub.get("extra", {}).get("attempted_text", "SomeTextValue")
        assert row.get("property_value") != attempted_text, (
            f"[Row 97] property_value must NOT be '{attempted_text}' — "
            f"text-type insertion must be rejected when int already exists in property_value_int."
        )


class TestRow98DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row98_no_duplicate_rows(self, db_snapshot, submissions):
        sub = submissions.get("98", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
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
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 118] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 118] Property row not found"
        row = match[0]

        # JSON must be preserved
        assert row.get("property_value_json") is not None, (
            f"[Row 118] property_value_json should be preserved, got NULL.\nFull row: {row}"
        )
        # FIX: previously only checked json != None; now also verifies type-protection
        # rejected the text attempt (aligns with sequential scenario)
        attempted_text = sub.get("extra", {}).get("attempted_text", "SomeTextValue")
        assert row.get("property_value") != attempted_text, (
            f"[Row 118] property_value must NOT be '{attempted_text}' — "
            f"text insertion must be rejected when JSON already exists in property_value_json."
        )


class TestRow119DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row119_no_duplicate_json_rows(self, db_snapshot, submissions):
        sub = submissions.get("119", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, (
            f"[Row 119] No user_properties row for '{uid}' — "
            "JSON value was not inserted at all"
        )
        match = [
            r for r in rows
            if r.get("property_name", "").lower() == sub["property_name"].lower()
        ]
        assert match, f"[Row 119] Property '{sub['property_name']}' not found for user '{uid}'"
        assert len(match) <= 1, (
            f"[Row 119] DUPLICATE JSON ROWS — expected <=1 row for this property, "
            f"found {len(match)}: {match}"
        )

# ══════════════════════════════════════════════════════════════════════════════
#  REGRESSION DB CHECKS — rows 120–152 (JSON format)
# ══════════════════════════════════════════════════════════════════════════════

class TestRow120DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row120_db_has_record(self, db_snapshot, submissions):
        sub = submissions.get("120", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 120] No user_properties row for '{uid}'"


class TestRow121DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row121_absent_false_and_modified_date_set(self, db_snapshot, submissions):
        sub = submissions.get("121", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 121] No user_properties row for '{uid}'"
        assert rows[0].get("bsent") is False, f"[Row 121] bsent should be False"
        assert rows[0].get("modified_date") is not None, "[Row 121] modified_date should be set"


class TestRow122DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row122_value_updated(self, db_snapshot, submissions):
        sub = submissions.get("122", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 122] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match and match[0].get("property_value") == "UpdatedValue", (
            f"[Row 122] Expected 'UpdatedValue', got '{match[0].get('property_value') if match else None}'"
        )


class TestRow123DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row123_trimmed_values_in_db(self, db_snapshot, submissions):
        sub = submissions.get("123", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 123] No user_properties row for trimmed UID '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, f"[Row 123] Property not found"
        assert match[0].get("property_value") == "SpacedValue", (
            f"[Row 123] Expected 'SpacedValue', got '{match[0].get('property_value')}'"
        )


class TestRow124DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row124_no_db_insert_when_client_user_id_missing(self, db_snapshot, submissions):
        sub = submissions.get("124", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id_bad = uid
        prop = (sub.get("property_name") or "").lower()
        matches = [r for r in _all_up_rows(db_snapshot) if (r.get("property_name") or "").lower() == prop]
        assert not matches, f"[Row 124] Expected no insert (missing ClientUserId), found: {matches}"


class TestRow125DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row125_no_db_insert_when_property_name_missing(self, db_snapshot, submissions):
        sub = submissions.get("125", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id_bad = uid
        assert not _up_rows(db_snapshot, uid), f"[Row 125] Expected no insert (missing PropertyName)"


class TestRow126DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row126_no_db_insert_when_property_value_missing(self, db_snapshot, submissions):
        sub = submissions.get("126", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id_bad = uid
        assert not _up_rows(db_snapshot, uid), f"[Row 126] Expected no insert (missing PropertyValue)"


class TestRow127DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row127_valid_row_inserted(self, db_snapshot, submissions):
        sub = submissions.get("127", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 127] Valid row for '{uid}' not in user_properties"


class TestRow128DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row128_valid_row_inserted_null_skipped(self, db_snapshot, submissions):
        sub = submissions.get("128", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 128] Valid row for '{uid}' not in user_properties"


class TestRow129DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row129_valid_row_inserted(self, db_snapshot, submissions):
        sub = submissions.get("129", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 129] Valid row for '{uid}' not in user_properties"


class TestRow130DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row130_valid_row_inserted(self, db_snapshot, submissions):
        sub = submissions.get("130", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 130] Valid row for '{uid}' not in user_properties"


class TestRow131DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row131_new_user_inserted_and_mapped(self, db_snapshot, submissions):
        sub = submissions.get("131", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 131] No user_properties row for new user '{uid}'"
        assert _mapping_rows(db_snapshot, uid), f"[Row 131] New user '{uid}' not in client_user_mapping"


class TestRow132DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row132_valid_row_inserted(self, db_snapshot, submissions):
        sub = submissions.get("132", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        assert _up_rows(db_snapshot, uid), f"[Row 132] Valid row for '{uid}' not in user_properties"


class TestRow134DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row134_text_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("134", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 134] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match and match[0].get("property_value") == "SomeTextValue", (
            f"[Row 134] Expected 'SomeTextValue', got '{match[0].get('property_value') if match else None}'"
        )


class TestRow135DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row135_text_wins_over_double(self, db_snapshot, submissions):
        sub = submissions.get("135", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 135] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 135] Property not found"
        assert match[0].get("property_value") == "TextWins", f"[Row 135] Expected 'TextWins'"
        assert match[0].get("property_value_double") is None, "[Row 135] property_value_double should be NULL"


class TestRow136DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row136_no_duplicate_text_rows(self, db_snapshot, submissions):
        sub = submissions.get("136", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 136] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 136] Duplicate rows found: {len(match)}"


class TestRow137DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row137_case_insensitive_dedup_text(self, db_snapshot, submissions):
        sub = submissions.get("137", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 137] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 137] Case-insensitive dedup failed, found {len(match)} rows"


class TestRow138DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row138_double_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("138", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 138] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match and match[0].get("property_value_double") is not None, "[Row 138] property_value_double is NULL"


class TestRow139DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row139_double_preserved_text_not_applied(self, db_snapshot, submissions):
        sub = submissions.get("139", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 139] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 139] Property not found"
        assert match[0].get("property_value_double") is not None, "[Row 139] property_value_double should be preserved"
        assert match[0].get("property_value") != "SomeTextValue", "[Row 139] Text override should not be applied"


class TestRow140DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row140_no_duplicate_double_rows(self, db_snapshot, submissions):
        sub = submissions.get("140", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 140] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 140] Duplicate rows: {len(match)}"


class TestRow141DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row141_date_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("141", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 141] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match and match[0].get("property_value_date") is not None, "[Row 141] property_value_date is NULL"


class TestRow142DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row142_date_preserved_text_not_applied(self, db_snapshot, submissions):
        sub = submissions.get("142", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 142] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 142] Property not found"
        assert match[0].get("property_value_date") is not None, "[Row 142] property_value_date should be preserved"
        assert match[0].get("property_value") != "SomeTextValue", "[Row 142] Text override should not be applied"


class TestRow143DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row143_no_duplicate_date_rows(self, db_snapshot, submissions):
        sub = submissions.get("143", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 143] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 143] Duplicate rows: {len(match)}"


class TestRow144DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row144_currency_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("144", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 144] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match and match[0].get("property_value_currency") is not None, "[Row 144] property_value_currency is NULL"


class TestRow145DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row145_currency_preserved_text_not_applied(self, db_snapshot, submissions):
        sub = submissions.get("145", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 145] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 145] Property not found"
        assert match[0].get("property_value_currency") is not None, "[Row 145] property_value_currency should be preserved"
        assert match[0].get("property_value") != "SomeTextValue", "[Row 145] Text override should not be applied"


class TestRow146DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row146_no_duplicate_currency_rows(self, db_snapshot, submissions):
        sub = submissions.get("146", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 146] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 146] Duplicate rows: {len(match)}"


class TestRow147DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row147_bool_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("147", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 147] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match and match[0].get("property_value_bool") is not None, "[Row 147] property_value_bool is NULL"


class TestRow148DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row148_bool_preserved_text_not_applied(self, db_snapshot, submissions):
        sub = submissions.get("148", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 148] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 148] Property not found"
        assert match[0].get("property_value_bool") is not None, "[Row 148] property_value_bool should be preserved"
        assert match[0].get("property_value") != "SomeTextValue", "[Row 148] Text override should not be applied"


class TestRow149DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row149_no_duplicate_bool_rows(self, db_snapshot, submissions):
        sub = submissions.get("149", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 149] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 149] Duplicate rows: {len(match)}"


class TestRow150DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row150_json_value_in_db(self, db_snapshot, submissions):
        sub = submissions.get("150", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 150] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match and match[0].get("property_value_json") is not None, "[Row 150] property_value_json is NULL"


class TestRow151DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row151_json_preserved_text_not_applied(self, db_snapshot, submissions):
        sub = submissions.get("151", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 151] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert match, "[Row 151] Property not found"
        assert match[0].get("property_value_json") is not None, "[Row 151] property_value_json should be preserved"
        assert match[0].get("property_value") != "SomeTextValue", "[Row 151] Text override should not be applied"


class TestRow152DB:
    @pytest.mark.regression
    @pytest.mark.db
    def test_row152_no_duplicate_json_rows(self, db_snapshot, submissions):
        sub = submissions.get("152", {})
        uid = sub.get("user_ids", [None])[0]
        self.client_user_id = uid
        rows = _up_rows(db_snapshot, uid)
        assert rows, f"[Row 152] No user_properties row for '{uid}'"
        match = [r for r in rows if r.get("property_name", "").lower() == sub["property_name"].lower()]
        assert len(match) <= 1, f"[Row 152] Duplicate JSON rows: {len(match)}"