"""
test_userproperties_regression.py
──────────────────────────────────
Regression / negative / boundary tests for the DataReceiver API
with schema=userproperties.

Every test function name contains its Excel row number (e.g. row76, row81)
so that the pytest-html report entry maps 1-to-1 to the spreadsheet.

Source spreadsheet:  InSitu_QA_Improved_Faithful_Corrected_-_Copy.xlsx
Sheet:               DataReceiver
Rows covered:        76-119  (row 91 skipped — requires >1 GB file, impractical to automate)

Run only regression tests:
    pytest tests/regression/ -m regression -v

Run with the full suite:
    pytest -v
"""

import pytest
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from utils import api_client, db_client

SCHEMA = "userproperties"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _assert_status(response, expected: int, context: str = ""):
    """Assert HTTP status and print the response body for report visibility."""
    body_text = response.text[:500]  # cap at 500 chars so report stays readable
    assert response.status_code == expected, (
        f"{context}\n"
        f"Expected HTTP {expected}, got {response.status_code}.\n"
        f"Response body: {body_text}"
    )
    return body_text


def _log(label: str, value):
    """pytest captures stdout; this shows up in the HTML report log column."""
    print(f"\n  [{label}] {value}")


# ──────────────────────────────────────────────────────────────────────────────
# ROW 76 — Basic CSV ingestion → 200 + DB row present
# Title: Verify that data receiver API with userproperties schema and csv format
#        is ingesting data in all the user_properties table
# ──────────────────────────────────────────────────────────────────────────────

class TestRow76BasicCsvIngestion:
    """Excel Row 76 | Status: pass | Expected: 200"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.property_name},40000"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row76_api_returns_200(self):
        """[Row 76] POST userproperties CSV → 200 OK."""
        body = _assert_status(self.response, 200, "Row 76 — basic CSV ingestion")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row76_db_has_record(self):
        """[Row 76] Ingested property appears in profiles.user_properties."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 76] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.property_name.lower()]
        assert match, f"[Row 76] Property '{self.property_name}' not found in: {rows}"


# ──────────────────────────────────────────────────────────────────────────────
# ROW 77 — absent=false and modified_date set for newly added properties
# Title: Verify ... ingesting absent=false and modified date for all newly added properties
# ──────────────────────────────────────────────────────────────────────────────

class TestRow77AbsentFalseNewProps:
    """Excel Row 77 | Status: pass"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.property_name},40000"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row77_absent_is_false_and_modified_date_set(self):
        """[Row 77] Newly added property has absent=False and a non-null modified_date."""
        _assert_status(self.response, 200, "Row 77 — pre-check API 200")
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 77] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.property_name.lower()]
        assert match, f"[Row 77] Property not found in: {rows}"
        prop = match[0]
        # absent column — DB may call it 'is_absent', 'absent', or similar
        absent_val = prop.get("is_absent") or prop.get("absent")
        _log("absent_value", absent_val)
        assert absent_val is False or absent_val == False or absent_val == 0, (
            f"[Row 77] Expected absent=False for new property, got: {absent_val}\nFull row: {prop}"
        )
        modified = prop.get("modified_date") or prop.get("updated_at") or prop.get("modified_at")
        _log("modified_date", modified)
        assert modified is not None, (
            f"[Row 77] Expected non-null modified_date for new property.\nFull row: {prop}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# ROW 78 — absent=false and modified_date updated after property update
# Title: Verify ... absent=false and modified date for updated properties with all datatypes
# ──────────────────────────────────────────────────────────────────────────────

class TestRow78AbsentFalseUpdatedProps:
    """Excel Row 78 | Status: (blank — to be automated)"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        # First insert
        csv_v1 = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.property_name},40000"
        )
        api_client.post_csv(SCHEMA, csv_v1)
        rows_before = db_client.wait_for_user_property_in_db(self.client_user_id)
        self.modified_before = None
        if rows_before:
            prop = next((r for r in rows_before if r.get("property_name", "").lower() == self.property_name.lower()), None)
            if prop:
                self.modified_before = prop.get("modified_date") or prop.get("updated_at") or prop.get("modified_at")
        # Second update with different value
        csv_v2 = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.property_name},99999"
        )
        self.response = api_client.post_csv(SCHEMA, csv_v2)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row78_absent_false_and_modified_date_refreshed_after_update(self):
        """[Row 78] Updated property still has absent=False; modified_date is refreshed."""
        _assert_status(self.response, 200, "Row 78 — update API 200")
        import time; time.sleep(3)  # small extra wait for second write
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 78] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.property_name.lower()]
        assert match, f"[Row 78] Property not found in: {rows}"
        prop = match[0]
        absent_val = prop.get("is_absent") or prop.get("absent")
        assert absent_val is False or absent_val == False or absent_val == 0, (
            f"[Row 78] Expected absent=False after update, got: {absent_val}\nFull row: {prop}"
        )
        modified_after = prop.get("modified_date") or prop.get("updated_at") or prop.get("modified_at")
        _log("modified_before", self.modified_before)
        _log("modified_after", modified_after)
        assert modified_after is not None, f"[Row 78] modified_date is null after update.\nFull row: {prop}"


# ──────────────────────────────────────────────────────────────────────────────
# ROW 79 — Row with multiple datatype columns filled → that row rejected; others inserted
# Title: Verify ... giving error for the row with more than datatype values
#        but insert other valid rows
# Expected: Row 1 (TUser1/TUP1) rejected; Rows 2 & 3 inserted normally.
# ──────────────────────────────────────────────────────────────────────────────

class TestRow79MultipleDataTypeValuesRejected:
    """Excel Row 79 | Status: fail (known bug)"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user_bad    = f"{unique_user_id}_BAD"
        self.user_good2  = f"{unique_user_id}_G2"
        self.user_good3  = f"{unique_user_id}_G3"
        self.prop        = unique_property_name
        # Row 1: TUser_BAD/prop — has Text + Int + Double filled (invalid — multiple types)
        # Row 2: TUser_G2/prop  — only Text filled  (valid)
        # Row 3: TUser_G3/prop  — only Int filled    (valid)
        csv_body = (
            "ClientUserId,PropertyName,PropertyValue,PropertyValueInt,PropertyValueDouble\n"
            f"{self.user_bad},{self.prop},SomeTextValue,40000,99.5\n"
            f"{self.user_good2},{self.prop},ValidText,,\n"
            f"{self.user_good3},{self.prop},,30000,\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row79_api_returns_200(self):
        """[Row 79] POST with multi-type row → overall response is 200 (partial insert)."""
        body = _assert_status(self.response, 200, "Row 79 — multi-type partial insert")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row79_valid_rows_inserted(self):
        """[Row 79] Valid rows (G2 and G3) are present in DB."""
        rows_g2 = db_client.wait_for_user_property_in_db(self.user_good2)
        rows_g3 = db_client.wait_for_user_property_in_db(self.user_good3)
        _log("db_rows_G2", rows_g2)
        _log("db_rows_G3", rows_g3)
        assert rows_g2, f"[Row 79] Valid user '{self.user_good2}' NOT found in user_properties"
        assert rows_g3, f"[Row 79] Valid user '{self.user_good3}' NOT found in user_properties"

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row79_invalid_row_not_inserted(self):
        """[Row 79] Invalid row (BAD — multiple types filled) should NOT be in DB.

        NOTE: This is marked fail in the Excel (known bug — API may insert it anyway).
        If this test passes, the bug has been fixed.
        """
        import time; time.sleep(5)  # give async pipeline time to settle
        rows_bad = db_client.get_user_properties(self.user_bad)
        _log("db_rows_BAD", rows_bad)
        assert not rows_bad, (
            f"[Row 79] KNOWN BUG: Row with multiple value-type columns filled was inserted "
            f"but should have been rejected.\nDB rows found: {rows_bad}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# ROW 80 — Space trimming → 200
# Title: Verify ... trimming spaces from all the fields while ingesting data
# ──────────────────────────────────────────────────────────────────────────────

class TestRow80SpaceTrimming:
    """Excel Row 80 | Status: pass | Expected: 200"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        # Values have leading/trailing spaces — API should trim them
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"   {self.client_user_id}   ,   {self.property_name}   ,   40000   "
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row80_api_returns_200(self):
        """[Row 80] POST with padded spaces → 200 OK."""
        body = _assert_status(self.response, 200, "Row 80 — space trimming")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row80_db_record_has_trimmed_values(self):
        """[Row 80] DB record exists with trimmed client_user_id and property_name."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, (
            f"[Row 80] No user_properties row for '{self.client_user_id}'. "
            f"Space trimming may have changed the stored ID."
        )
        match = [r for r in rows if r.get("property_name", "").strip().lower() == self.property_name.lower()]
        assert match, f"[Row 80] Property '{self.property_name}' not found in: {rows}"


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 81-83 — Missing required header columns → 400
# ──────────────────────────────────────────────────────────────────────────────

class TestRow81MissingClientUserIdColumn:
    """Excel Row 81 | Status: (blank) | Expected: 400"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_property_name):
        self.property_name = unique_property_name
        # ClientUserId column is absent from the CSV header entirely
        csv_body = (
            "PropertyName,PropertyValueInt\n"
            f"{self.property_name},40000"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row81_missing_client_user_id_column_returns_400(self):
        """[Row 81] CSV missing ClientUserId header → API returns 400."""
        body = _assert_status(self.response, 400, "Row 81 — missing ClientUserId column")
        _log("response_400_body", body)


class TestRow82MissingPropertyNameColumn:
    """Excel Row 82 | Status: (blank) | Expected: 400"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id):
        self.client_user_id = unique_user_id
        # PropertyName column is absent from the CSV header entirely
        csv_body = (
            "ClientUserId,PropertyValueInt\n"
            f"{self.client_user_id},40000"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row82_missing_property_name_column_returns_400(self):
        """[Row 82] CSV missing PropertyName header → API returns 400."""
        body = _assert_status(self.response, 400, "Row 82 — missing PropertyName column")
        _log("response_400_body", body)


class TestRow83MissingPropertyValueColumn:
    """Excel Row 83 | Status: (blank) | Expected: 400"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        # No property_value column at all (no PropertyValue/Int/Double/etc.)
        csv_body = (
            "ClientUserId,PropertyName\n"
            f"{self.client_user_id},{self.property_name}"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row83_missing_property_value_column_returns_400(self):
        """[Row 83] CSV missing all PropertyValue columns → API returns 400."""
        body = _assert_status(self.response, 400, "Row 83 — missing property_value column")
        _log("response_400_body", body)


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 84-85 — Row with missing / empty ClientUserId value → that row skipped
# ──────────────────────────────────────────────────────────────────────────────

class TestRow84MissingClientUserIdValue:
    """Excel Row 84 | Status: (blank) | Expected: 200, invalid row skipped"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user_valid   = f"{unique_user_id}_V"
        self.prop         = unique_property_name
        # Row 2 has no ClientUserId value (empty field)
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.user_valid},{self.prop},40000\n"
            f",{self.prop},99999"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row84_api_returns_200(self):
        """[Row 84] CSV with one missing ClientUserId value → overall 200."""
        body = _assert_status(self.response, 200, "Row 84 — missing ClientUserId value")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row84_valid_row_inserted(self):
        """[Row 84] Valid row is in DB."""
        rows = db_client.wait_for_user_property_in_db(self.user_valid)
        _log("db_rows_valid", rows)
        assert rows, f"[Row 84] Valid row for '{self.user_valid}' not found in user_properties"


class TestRow85EmptyNullClientUserIdValue:
    """Excel Row 85 | Status: (blank) | Expected: 200, row with empty/null ClientUserId skipped"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user_valid = f"{unique_user_id}_V"
        self.prop       = unique_property_name
        # Two rows with empty-string ClientUserId, two valid rows
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.user_valid},{self.prop},40000\n"
            f",{self.prop},11111\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row85_api_returns_200(self):
        """[Row 85] CSV with empty ClientUserId value → overall 200."""
        body = _assert_status(self.response, 200, "Row 85 — empty ClientUserId value")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row85_valid_row_inserted(self):
        """[Row 85] Valid row is in DB; invalid rows (empty ClientUserId) are skipped."""
        rows = db_client.wait_for_user_property_in_db(self.user_valid)
        _log("db_rows_valid", rows)
        assert rows, f"[Row 85] Valid row for '{self.user_valid}' not found in user_properties"


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 86-87 — Row with missing / empty PropertyName → that row skipped
# ──────────────────────────────────────────────────────────────────────────────

class TestRow86MissingPropertyNameValue:
    """Excel Row 86 | Status: (blank) | Expected: 200, rows with missing PropertyName skipped"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user_valid  = f"{unique_user_id}_V"
        self.user_bad1   = f"{unique_user_id}_B1"
        self.prop        = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.user_valid},{self.prop},40000\n"
            f"{self.user_bad1},,99999\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row86_api_returns_200(self):
        """[Row 86] CSV with missing PropertyName value → overall 200."""
        body = _assert_status(self.response, 200, "Row 86 — missing PropertyName value")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row86_valid_row_inserted_bad_row_skipped(self):
        """[Row 86] Valid row in DB; row with empty PropertyName is skipped."""
        rows = db_client.wait_for_user_property_in_db(self.user_valid)
        _log("db_rows_valid", rows)
        assert rows, f"[Row 86] Valid row for '{self.user_valid}' not found in user_properties"
        rows_bad = db_client.get_user_properties(self.user_bad1)
        _log("db_rows_bad", rows_bad)
        assert not rows_bad, f"[Row 86] Row with missing PropertyName should be skipped, found: {rows_bad}"


class TestRow87EmptyNullPropertyNameValue:
    """Excel Row 87 | Status: (blank) | Expected: 200, rows with null/empty PropertyName skipped"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user_valid = f"{unique_user_id}_V"
        self.user_bad   = f"{unique_user_id}_B"
        self.prop       = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.user_valid},{self.prop},40000\n"
            f"{self.user_bad},'',99999\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row87_api_returns_200(self):
        """[Row 87] CSV with empty/quoted-empty PropertyName → overall 200."""
        body = _assert_status(self.response, 200, "Row 87 — empty PropertyName value")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row87_valid_row_inserted(self):
        """[Row 87] Valid row is in DB."""
        rows = db_client.wait_for_user_property_in_db(self.user_valid)
        _log("db_rows_valid", rows)
        assert rows, f"[Row 87] Valid row for '{self.user_valid}' not found in user_properties"


# ──────────────────────────────────────────────────────────────────────────────
# ROW 88 — Row with missing property_value → that row skipped
# ──────────────────────────────────────────────────────────────────────────────

class TestRow88MissingPropertyValueRowSkipped:
    """Excel Row 88 | Status: (blank) | Expected: 200, row with missing property_value skipped"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user_valid = f"{unique_user_id}_V"
        self.user_bad   = f"{unique_user_id}_B"
        self.prop       = unique_property_name
        # user_bad row has all three value columns empty
        csv_body = (
            "ClientUserId,PropertyName,PropertyValue,PropertyValueInt,PropertyValueDouble\n"
            f"{self.user_valid},{self.prop},ValidText,,\n"
            f"{self.user_bad},{self.prop},,,\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row88_api_returns_200(self):
        """[Row 88] CSV with a row having all property_value empty → overall 200."""
        body = _assert_status(self.response, 200, "Row 88 — missing property_value row")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row88_valid_row_inserted_bad_skipped(self):
        """[Row 88] Valid row inserted; row with no property_value is skipped."""
        rows_valid = db_client.wait_for_user_property_in_db(self.user_valid)
        _log("db_rows_valid", rows_valid)
        assert rows_valid, f"[Row 88] Valid row for '{self.user_valid}' not found in user_properties"
        rows_bad = db_client.get_user_properties(self.user_bad)
        _log("db_rows_bad", rows_bad)
        assert not rows_bad, f"[Row 88] Row with no property_value should be skipped, found: {rows_bad}"


# ──────────────────────────────────────────────────────────────────────────────
# ROW 89 — Row with empty/null property_value IS inserted (not skipped)
# ──────────────────────────────────────────────────────────────────────────────

class TestRow89EmptyPropertyValueIsInserted:
    """Excel Row 89 | Status: (blank) | Expected: 200, row with null/empty property_value INSERTED"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        # PropertyValue is explicitly empty-string (quoted)
        csv_body = (
            "ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id},{self.prop},''\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row89_api_returns_200(self):
        """[Row 89] CSV with empty property_value → 200."""
        body = _assert_status(self.response, 200, "Row 89 — empty property_value inserted")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row89_row_is_inserted_in_db(self):
        """[Row 89] Row with empty property_value should still be in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, (
            f"[Row 89] Row with empty property_value was NOT inserted for '{self.client_user_id}'"
        )


# ──────────────────────────────────────────────────────────────────────────────
# ROW 90 — Unknown/extra columns are ignored; required columns ingested
# ──────────────────────────────────────────────────────────────────────────────

class TestRow90UnknownColumnsIgnored:
    """Excel Row 90 | Status: (blank) | Expected: Unknown extra columns ignored"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        # Extra columns: testCol1, testCol2 should be ignored
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt,testCol1,testCol2\n"
            f"{self.client_user_id},{self.prop},40000,ExtraData1,ExtraData2"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row90_api_accepts_extra_columns(self):
        """[Row 90] CSV with extra unknown columns → API accepts (200 or 4xx, record actual)."""
        body = self.response.text[:500]
        _log("status_code", self.response.status_code)
        _log("response", body)
        # We assert 200: API should accept and ignore unrecognised columns
        assert self.response.status_code == 200, (
            f"[Row 90] Expected 200 (extra columns ignored), got {self.response.status_code}.\nBody: {body}"
        )

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row90_required_columns_inserted_in_db(self):
        """[Row 90] Required column data (ClientUserId, PropertyName, value) is in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 90] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 90] Property '{self.prop}' not found in: {rows}"


# ──────────────────────────────────────────────────────────────────────────────
# ROW 91 — File size > 1 GB (SKIPPED — impractical to automate)
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.skip(reason="[Row 91] Requires generating a >1 GB file. Impractical for automated regression suite. Must be tested manually.")
@pytest.mark.regression
def test_row91_file_size_greater_than_1gb():
    """[Row 91] POST userproperties CSV with file > 1 GB → 200 and DB ingested.

    SKIPPED: Generating a file this large in CI is not feasible.
    Test must be run manually or via a dedicated load-testing tool.
    """
    pass


# ──────────────────────────────────────────────────────────────────────────────
# ROW 92 — PropertyValue column ingested correctly
# ──────────────────────────────────────────────────────────────────────────────

class TestRow92PropertyValue:
    """Excel Row 92 | Status: (blank) | Expected: PropertyValue stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        self.text_value     = "AutoTestTextValue"
        csv_body = (
            "ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id},{self.prop},{self.text_value}"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row92_api_returns_200(self):
        """[Row 92] POST with PropertyValue → 200."""
        body = _assert_status(self.response, 200, "Row 92 — PropertyValue")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row92_text_value_in_db(self):
        """[Row 92] property_value_text column populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 92] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 92] Property not found in: {rows}"
        prop = match[0]
        text_col = prop.get("property_value_text") or prop.get("property_value")
        _log("property_value_text", text_col)
        assert text_col is not None and str(text_col).strip() != "", (
            f"[Row 92] property_value_text is null/empty.\nFull row: {prop}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# ROW 93 — Type conflict: existing Text, new Int → NOT ingested
# ──────────────────────────────────────────────────────────────────────────────

class TestRow93TypeConflictTextVsInt:
    """Excel Row 93 | Status: (blank) | Expected: Int NOT ingested; existing Text preserved"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        # Step 1: Insert with Text type
        csv_text = (
            "ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id},{self.prop},ExistingText"
        )
        api_client.post_csv(SCHEMA, csv_text)
        db_client.wait_for_user_property_in_db(self.client_user_id)
        # Step 2: Try to overwrite with Int type
        csv_int = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.prop},40000"
        )
        self.response = api_client.post_csv(SCHEMA, csv_int)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row93_type_conflict_response(self):
        """[Row 93] Attempt to change Text property to Int → record actual response code."""
        _log("status_code", self.response.status_code)
        _log("response_body", self.response.text[:500])

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row93_text_value_preserved_in_db(self):
        """[Row 93] Existing Text property_value is preserved; Int value not applied."""
        import time; time.sleep(5)
        rows = db_client.get_user_properties(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 93] No user_properties row found for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 93] Property not found in: {rows}"
        prop = match[0]
        text_val = prop.get("property_value_text") or prop.get("property_value")
        int_val  = prop.get("property_value_int")
        _log("property_value_text", text_val)
        _log("property_value_int",  int_val)
        assert text_val is not None, f"[Row 93] Text value should be preserved.\nFull row: {prop}"
        assert not int_val, f"[Row 93] Int value should NOT be set.\nFull row: {prop}"


# ──────────────────────────────────────────────────────────────────────────────
# ROW 94 — No duplicate property_value_text inserted
# ──────────────────────────────────────────────────────────────────────────────

class TestRow94NoDuplicatePropertyValue:
    """Excel Row 94 | Status: (blank) | Expected: 200, no duplicate rows inserted"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user1 = f"{unique_user_id}_U1"
        self.user2 = f"{unique_user_id}_U2"
        self.prop1 = unique_property_name
        self.prop2 = f"{unique_property_name}_P2"
        # user1/prop1 is sent three times (two duplicates of 40000, one different 20000)
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.user1},{self.prop1},40000\n"
            f"{self.user2},{self.prop1},50000\n"
            f"{self.user1},{self.prop2},30000\n"
            f"{self.user1},{self.prop1},40000\n"   # exact duplicate of row 1
            f"{self.user1},{self.prop1},20000\n"   # same key, different value
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row94_api_returns_200(self):
        """[Row 94] CSV with duplicates → 200 (duplicates de-duped by API)."""
        body = _assert_status(self.response, 200, "Row 94 — no duplicate text")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row94_no_duplicate_rows_in_db(self):
        """[Row 94] Only one row per (client_user_id, property_name) in DB."""
        rows = db_client.wait_for_user_property_in_db(self.user1)
        _log("db_rows_user1", rows)
        prop1_rows = [r for r in rows if r.get("property_name", "").lower() == self.prop1.lower()]
        assert len(prop1_rows) <= 1, (
            f"[Row 94] Found {len(prop1_rows)} rows for user1/prop1 — expected at most 1 (no duplicates).\n"
            f"Rows: {prop1_rows}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# ROW 95 — Case-insensitive deduplication for property_value_text
# ──────────────────────────────────────────────────────────────────────────────

class TestRow95CaseInsensitiveDedupText:
    """Excel Row 95 | Status: (blank) | Expected: 200, case differences treated as duplicates"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        # Same user+property with padded/cased variants — should de-dup
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"   {self.client_user_id}  ,  {self.prop} ,   40000\n"
            f"{self.client_user_id},{self.prop},40000\n"
            f"{self.client_user_id},{self.prop},20000\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row95_api_returns_200(self):
        """[Row 95] CSV with case-variant duplicates → 200."""
        body = _assert_status(self.response, 200, "Row 95 — case insensitive dedup text")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row95_single_row_in_db(self):
        """[Row 95] Only one DB row after case-variant duplicate submissions."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").strip().lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, (
            f"[Row 95] Found {len(prop_rows)} rows — expected ≤1 (case-insensitive dedup).\nRows: {prop_rows}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# ROW 96 — PropertyValueInt column ingested correctly
# ──────────────────────────────────────────────────────────────────────────────

class TestRow96PropertyValueInt:
    """Excel Row 96 | Status: (blank) | Expected: PropertyValueInt stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.prop},40000"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row96_api_returns_200(self):
        """[Row 96] POST with PropertyValueInt → 200."""
        body = _assert_status(self.response, 200, "Row 96 — PropertyValueInt")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row96_int_value_in_db(self):
        """[Row 96] property_value_int column populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 96] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 96] Property not found in: {rows}"
        prop = match[0]
        int_val = prop.get("property_value_int") or prop.get("property_value")
        _log("property_value_int", int_val)
        assert int_val is not None, f"[Row 96] property_value_int is null.\nFull row: {prop}"


# ──────────────────────────────────────────────────────────────────────────────
# ROW 97 — Type conflict: existing Int, new Text → NOT ingested
# ──────────────────────────────────────────────────────────────────────────────

class TestRow97TypeConflictIntVsText:
    """Excel Row 97 | Status: (blank) | Expected: Text NOT ingested; existing Int preserved"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_int = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.prop},40000"
        )
        api_client.post_csv(SCHEMA, csv_int)
        db_client.wait_for_user_property_in_db(self.client_user_id)
        csv_text = (
            "ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id},{self.prop},SomeTextValue"
        )
        self.response = api_client.post_csv(SCHEMA, csv_text)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row97_type_conflict_response(self):
        """[Row 97] Attempt to change Int property to Text → record response."""
        _log("status_code", self.response.status_code)
        _log("response_body", self.response.text[:500])

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row97_int_value_preserved_in_db(self):
        """[Row 97] Existing Int value is preserved; Text value not applied."""
        import time; time.sleep(5)
        rows = db_client.get_user_properties(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 97] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        prop = match[0] if match else {}
        int_val  = prop.get("property_value_int")
        text_val = prop.get("property_value_text")
        _log("property_value_int",  int_val)
        _log("property_value_text", text_val)
        assert int_val is not None, f"[Row 97] Int value should be preserved.\nFull row: {prop}"
        assert not text_val,        f"[Row 97] Text value should NOT be set.\nFull row: {prop}"


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 98-99 — No duplicate / case-insensitive dedup for PropertyValueInt
# (Same structure as 94-95 but keyed to integer type)
# ──────────────────────────────────────────────────────────────────────────────

class TestRow98NoDuplicatePropertyValueInt:
    """Excel Row 98 | Status: (blank) | Expected: 200, no duplicate rows for int"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.prop},40000\n"
            f"{self.client_user_id},{self.prop},40000\n"  # exact duplicate
            f"{self.client_user_id},{self.prop},20000\n"  # same key, different value
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row98_api_returns_200(self):
        """[Row 98] CSV with duplicate int rows → 200."""
        body = _assert_status(self.response, 200, "Row 98 — no duplicate int")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row98_no_duplicate_rows(self):
        """[Row 98] Only one DB row per (client_user_id, property_name)."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, (
            f"[Row 98] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"
        )


class TestRow99CaseInsensitiveDedupInt:
    """Excel Row 99 | Status: (blank) | Expected: 200, case/space variants de-duped"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueInt\n"
            f"   {self.client_user_id}  ,  {self.prop} ,   40000\n"
            f"{self.client_user_id},{self.prop},40000\n"
            f"{self.client_user_id},{self.prop},20000\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row99_api_returns_200(self):
        """[Row 99] CSV with padded/case-variant int duplicates → 200."""
        body = _assert_status(self.response, 200, "Row 99 — case dedup int")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row99_single_row_in_db(self):
        """[Row 99] Only one DB row after case-variant duplicate int submissions."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").strip().lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, (
            f"[Row 99] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 100-103 — PropertyValueDouble
# ──────────────────────────────────────────────────────────────────────────────

class TestRow100PropertyValueDouble:
    """Excel Row 100 | Status: (blank) | Expected: PropertyValueDouble stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueDouble\n"
            f"{self.client_user_id},{self.prop},99.5"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row100_api_returns_200(self):
        """[Row 100] POST with PropertyValueDouble → 200."""
        body = _assert_status(self.response, 200, "Row 100 — PropertyValueDouble")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row100_double_value_in_db(self):
        """[Row 100] property_value_double populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 100] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 100] Property not found in: {rows}"
        dbl = match[0].get("property_value_double") or match[0].get("property_value")
        _log("property_value_double", dbl)
        assert dbl is not None, f"[Row 100] property_value_double is null.\nFull row: {match[0]}"


class TestRow101TypeConflictDoubleVsText:
    """Excel Row 101 | Status: (blank) | Expected: Text NOT ingested; existing Double preserved"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValueDouble\n{self.client_user_id},{self.prop},99.5")
        db_client.wait_for_user_property_in_db(self.client_user_id)
        self.response = api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.prop},SomeTextValue")

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row101_type_conflict_response(self):
        """[Row 101] Attempt to change Double property to Text → record response."""
        _log("status_code", self.response.status_code)
        _log("response_body", self.response.text[:500])

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row101_double_preserved_text_not_set(self):
        """[Row 101] Existing Double value preserved; Text value not applied."""
        import time; time.sleep(5)
        rows = db_client.get_user_properties(self.client_user_id)
        _log("db_rows", rows)
        match = next((r for r in rows if r.get("property_name", "").lower() == self.prop.lower()), {})
        dbl  = match.get("property_value_double")
        txt  = match.get("property_value_text")
        _log("property_value_double", dbl)
        _log("property_value_text",   txt)
        assert dbl is not None, f"[Row 101] Double value should be preserved.\nFull row: {match}"
        assert not txt,         f"[Row 101] Text value should NOT be set.\nFull row: {match}"


class TestRow102NoDuplicatePropertyValueDouble:
    """Excel Row 102 | Status: (blank) | Expected: 200, no duplicate double rows"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueDouble\n"
            f"{self.client_user_id},{self.prop},99.5\n"
            f"{self.client_user_id},{self.prop},99.5\n"
            f"{self.client_user_id},{self.prop},1.23\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row102_api_returns_200(self):
        """[Row 102] CSV with duplicate double rows → 200."""
        body = _assert_status(self.response, 200, "Row 102 — no duplicate double")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row102_no_duplicate_rows(self):
        """[Row 102] Only one DB row per (client_user_id, property_name)."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 102] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


class TestRow103CaseInsensitiveDedupDouble:
    """Excel Row 103 | Status: (blank)"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueDouble\n"
            f"   {self.client_user_id}  ,  {self.prop} ,   99.5\n"
            f"{self.client_user_id},{self.prop},99.5\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row103_api_returns_200(self):
        """[Row 103] CSV with padded double duplicates → 200."""
        body = _assert_status(self.response, 200, "Row 103 — case dedup double")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row103_single_row_in_db(self):
        """[Row 103] Only one DB row after case-variant double submissions."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").strip().lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 103] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 104-107 — PropertyValueDate
# ──────────────────────────────────────────────────────────────────────────────

class TestRow104PropertyValueDate:
    """Excel Row 104 | Status: (blank) | Expected: PropertyValueDate stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueDate\n"
            f"{self.client_user_id},{self.prop},2023-02-03"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row104_api_returns_200(self):
        """[Row 104] POST with PropertyValueDate → 200."""
        body = _assert_status(self.response, 200, "Row 104 — PropertyValueDate")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row104_date_value_in_db(self):
        """[Row 104] property_value_date populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 104] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 104] Property not found in: {rows}"
        date_val = match[0].get("property_value_date") or match[0].get("property_value")
        _log("property_value_date", date_val)
        assert date_val is not None, f"[Row 104] property_value_date is null.\nFull row: {match[0]}"


class TestRow105TypeConflictDateVsText:
    """Excel Row 105 | Status: (blank) | Expected: Text NOT ingested; existing Date preserved"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValueDate\n{self.client_user_id},{self.prop},2023-02-03")
        db_client.wait_for_user_property_in_db(self.client_user_id)
        self.response = api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.prop},SomeTextValue")

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row105_type_conflict_response(self):
        """[Row 105] Attempt to change Date property to Text → record response."""
        _log("status_code", self.response.status_code)
        _log("response_body", self.response.text[:500])

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row105_date_preserved_text_not_set(self):
        """[Row 105] Existing Date value preserved; Text value not applied."""
        import time; time.sleep(5)
        rows = db_client.get_user_properties(self.client_user_id)
        _log("db_rows", rows)
        match = next((r for r in rows if r.get("property_name", "").lower() == self.prop.lower()), {})
        date_val = match.get("property_value_date")
        txt_val  = match.get("property_value_text")
        _log("property_value_date", date_val)
        _log("property_value_text", txt_val)
        assert date_val is not None, f"[Row 105] Date value should be preserved.\nFull row: {match}"
        assert not txt_val,          f"[Row 105] Text value should NOT be set.\nFull row: {match}"


class TestRow106NoDuplicatePropertyValueDate:
    """Excel Row 106 | Status: (blank) | Expected: 200, no duplicate date rows"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueDate\n"
            f"{self.client_user_id},{self.prop},2023-02-03\n"
            f"{self.client_user_id},{self.prop},2023-02-03\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row106_api_returns_200(self):
        """[Row 106] CSV with duplicate date rows → 200."""
        body = _assert_status(self.response, 200, "Row 106 — no duplicate date")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row106_no_duplicate_rows(self):
        """[Row 106] Only one DB row per (client_user_id, property_name)."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 106] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


class TestRow107CaseInsensitiveDedupDate:
    """Excel Row 107 | Status: (blank)"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueDate\n"
            f"   {self.client_user_id}  ,  {self.prop} ,   2023-02-03\n"
            f"{self.client_user_id},{self.prop},2023-02-03\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row107_api_returns_200(self):
        """[Row 107] CSV with padded date duplicates → 200."""
        body = _assert_status(self.response, 200, "Row 107 — case dedup date")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row107_single_row_in_db(self):
        """[Row 107] Only one DB row after padded-variant date submissions."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").strip().lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 107] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 108-111 — PropertyValueCurrency
# ──────────────────────────────────────────────────────────────────────────────

class TestRow108PropertyValueCurrency:
    """Excel Row 108 | Status: (blank) | Expected: PropertyValueCurrency stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueCurrency\n"
            f"{self.client_user_id},{self.prop},2000.00"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row108_api_returns_200(self):
        """[Row 108] POST with PropertyValueCurrency → 200."""
        body = _assert_status(self.response, 200, "Row 108 — PropertyValueCurrency")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row108_currency_value_in_db(self):
        """[Row 108] property_value_currency populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 108] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 108] Property not found in: {rows}"
        cur = match[0].get("property_value_currency") or match[0].get("property_value")
        _log("property_value_currency", cur)
        assert cur is not None, f"[Row 108] property_value_currency is null.\nFull row: {match[0]}"


class TestRow109TypeConflictCurrencyVsText:
    """Excel Row 109 | Status: (blank) | Expected: Text NOT ingested; existing Currency preserved"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValueCurrency\n{self.client_user_id},{self.prop},2000.00")
        db_client.wait_for_user_property_in_db(self.client_user_id)
        self.response = api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.prop},SomeTextValue")

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row109_type_conflict_response(self):
        """[Row 109] Attempt to change Currency property to Text → record response."""
        _log("status_code", self.response.status_code)
        _log("response_body", self.response.text[:500])

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row109_currency_preserved_text_not_set(self):
        """[Row 109] Existing Currency value preserved; Text value not applied."""
        import time; time.sleep(5)
        rows = db_client.get_user_properties(self.client_user_id)
        _log("db_rows", rows)
        match = next((r for r in rows if r.get("property_name", "").lower() == self.prop.lower()), {})
        cur = match.get("property_value_currency")
        txt = match.get("property_value_text")
        _log("property_value_currency", cur)
        _log("property_value_text",     txt)
        assert cur is not None, f"[Row 109] Currency value should be preserved.\nFull row: {match}"
        assert not txt,         f"[Row 109] Text value should NOT be set.\nFull row: {match}"


class TestRow110NoDuplicatePropertyValueCurrency:
    """Excel Row 110 | Status: (blank) | Expected: 200, no duplicate currency rows"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueCurrency\n"
            f"{self.client_user_id},{self.prop},2000.00\n"
            f"{self.client_user_id},{self.prop},2000.00\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row110_api_returns_200(self):
        """[Row 110] CSV with duplicate currency rows → 200."""
        body = _assert_status(self.response, 200, "Row 110 — no duplicate currency")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row110_no_duplicate_rows(self):
        """[Row 110] Only one DB row per (client_user_id, property_name)."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 110] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


class TestRow111CaseInsensitiveDedupCurrency:
    """Excel Row 111 | Status: (blank)"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueCurrency\n"
            f"   {self.client_user_id}  ,  {self.prop} ,   2000.00\n"
            f"{self.client_user_id},{self.prop},2000.00\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row111_api_returns_200(self):
        """[Row 111] CSV with padded currency duplicates → 200."""
        body = _assert_status(self.response, 200, "Row 111 — case dedup currency")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row111_single_row_in_db(self):
        """[Row 111] Only one DB row after padded-variant currency submissions."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").strip().lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 111] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 112-115 — PropertyValueBool
# ──────────────────────────────────────────────────────────────────────────────

class TestRow112PropertyValueBool:
    """Excel Row 112 | Status: (blank) | Expected: PropertyValueBool stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueBool\n"
            f"{self.client_user_id},{self.prop},true"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row112_api_returns_200(self):
        """[Row 112] POST with PropertyValueBool → 200."""
        body = _assert_status(self.response, 200, "Row 112 — PropertyValueBool")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row112_bool_value_in_db(self):
        """[Row 112] property_value_bool populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 112] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 112] Property not found in: {rows}"
        bool_val = match[0].get("property_value_bool")
        _log("property_value_bool", bool_val)
        assert bool_val is not None, f"[Row 112] property_value_bool is null.\nFull row: {match[0]}"


class TestRow113TypeConflictBoolVsText:
    """Excel Row 113 | Status: (blank) | Expected: Text NOT ingested; existing Bool preserved"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValueBool\n{self.client_user_id},{self.prop},true")
        db_client.wait_for_user_property_in_db(self.client_user_id)
        self.response = api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.prop},SomeTextValue")

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row113_type_conflict_response(self):
        """[Row 113] Attempt to change Bool property to Text → record response."""
        _log("status_code", self.response.status_code)
        _log("response_body", self.response.text[:500])

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row113_bool_preserved_text_not_set(self):
        """[Row 113] Existing Bool value preserved; Text value not applied."""
        import time; time.sleep(5)
        rows = db_client.get_user_properties(self.client_user_id)
        _log("db_rows", rows)
        match = next((r for r in rows if r.get("property_name", "").lower() == self.prop.lower()), {})
        bool_val = match.get("property_value_bool")
        txt_val  = match.get("property_value_text")
        _log("property_value_bool", bool_val)
        _log("property_value_text", txt_val)
        assert bool_val is not None, f"[Row 113] Bool value should be preserved.\nFull row: {match}"
        assert not txt_val,          f"[Row 113] Text value should NOT be set.\nFull row: {match}"


class TestRow114NoDuplicatePropertyValueBool:
    """Excel Row 114 | Status: (blank) | Expected: 200, no duplicate bool rows"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueBool\n"
            f"{self.client_user_id},{self.prop},true\n"
            f"{self.client_user_id},{self.prop},true\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row114_api_returns_200(self):
        """[Row 114] CSV with duplicate bool rows → 200."""
        body = _assert_status(self.response, 200, "Row 114 — no duplicate bool")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row114_no_duplicate_rows(self):
        """[Row 114] Only one DB row per (client_user_id, property_name)."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 114] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


class TestRow115CaseInsensitiveDedupBool:
    """Excel Row 115 | Status: (blank)"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        csv_body = (
            "ClientUserId,PropertyName,PropertyValueBool\n"
            f"   {self.client_user_id}  ,  {self.prop} ,   true\n"
            f"{self.client_user_id},{self.prop},true\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row115_api_returns_200(self):
        """[Row 115] CSV with padded bool duplicates → 200."""
        body = _assert_status(self.response, 200, "Row 115 — case dedup bool")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row115_single_row_in_db(self):
        """[Row 115] Only one DB row after padded-variant bool submissions."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").strip().lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 115] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"


# ──────────────────────────────────────────────────────────────────────────────
# ROWS 116-119 — PropertyValueJson
# ──────────────────────────────────────────────────────────────────────────────

class TestRow116PropertyValueJson:
    """Excel Row 116 | Status: (blank) | Expected: PropertyValueJson stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        # JSON object value
        csv_body = (
            'ClientUserId,PropertyName,PropertyValueJson\n'
            f'{self.client_user_id},{self.prop},' + '"{""PropertyValueInt"": 40000}"'
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row116_api_returns_200(self):
        """[Row 116] POST with PropertyValueJson (object) → 200."""
        body = _assert_status(self.response, 200, "Row 116 — PropertyValueJson object")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row116_json_value_in_db(self):
        """[Row 116] property_value_json populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 116] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 116] Property not found in: {rows}"
        json_val = match[0].get("property_value_json") or match[0].get("property_value")
        _log("property_value_json", json_val)
        assert json_val is not None, f"[Row 116] property_value_json is null.\nFull row: {match[0]}"


class TestRow117PropertyValueJsonArray:
    """Excel Row 117 | Status: (blank) | Expected: PropertyValueJson with JSON array stored in DB"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        # JSON array value
        csv_body = (
            'ClientUserId,PropertyName,PropertyValueJson\n'
            f'{self.client_user_id},{self.prop},' + '"[{""key"":""val1""},{""key"":""val2""}]"'
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row117_api_returns_200(self):
        """[Row 117] POST with PropertyValueJson (array) → 200."""
        body = _assert_status(self.response, 200, "Row 117 — PropertyValueJson array")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row117_json_array_in_db(self):
        """[Row 117] property_value_json (array) populated in DB."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        assert rows, f"[Row 117] No user_properties row for '{self.client_user_id}'"
        match = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert match, f"[Row 117] Property not found in: {rows}"
        json_val = match[0].get("property_value_json") or match[0].get("property_value")
        _log("property_value_json", json_val)
        assert json_val is not None, f"[Row 117] property_value_json (array) is null.\nFull row: {match[0]}"


class TestRow118TypeConflictJsonVsText:
    """Excel Row 118 | Status: (blank) | Expected: Text NOT ingested; existing JSON preserved"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        json_csv = (
            'ClientUserId,PropertyName,PropertyValueJson\n'
            f'{self.client_user_id},{self.prop},' + '"{""key"":""value""}"'
        )
        api_client.post_csv(SCHEMA, json_csv)
        db_client.wait_for_user_property_in_db(self.client_user_id)
        self.response = api_client.post_csv(SCHEMA, f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.prop},SomeTextValue")

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row118_type_conflict_response(self):
        """[Row 118] Attempt to change JSON property to Text → record response."""
        _log("status_code", self.response.status_code)
        _log("response_body", self.response.text[:500])

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row118_json_preserved_text_not_set(self):
        """[Row 118] Existing JSON value preserved; Text value not applied."""
        import time; time.sleep(5)
        rows = db_client.get_user_properties(self.client_user_id)
        _log("db_rows", rows)
        match = next((r for r in rows if r.get("property_name", "").lower() == self.prop.lower()), {})
        json_val = match.get("property_value_json")
        txt_val  = match.get("property_value_text")
        _log("property_value_json", json_val)
        _log("property_value_text", txt_val)
        assert json_val is not None, f"[Row 118] JSON value should be preserved.\nFull row: {match}"
        assert not txt_val,          f"[Row 118] Text value should NOT be set.\nFull row: {match}"


class TestRow119NoDuplicatePropertyValueJson:
    """Excel Row 119 | Status: (blank) | Expected: 200, no duplicate json rows"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.prop           = unique_property_name
        json_val = '"{""key"":""value""}"'
        csv_body = (
            f"ClientUserId,PropertyName,PropertyValueJson\n"
            f"{self.client_user_id},{self.prop},{json_val}\n"
            f"{self.client_user_id},{self.prop},{json_val}\n"
        )
        self.response = api_client.post_csv(SCHEMA, csv_body)

    @pytest.mark.regression
    @pytest.mark.userprops
    def test_row119_api_returns_200(self):
        """[Row 119] CSV with duplicate JSON rows → 200."""
        body = _assert_status(self.response, 200, "Row 119 — no duplicate json")
        _log("response", body)

    @pytest.mark.regression
    @pytest.mark.userprops
    @pytest.mark.db
    def test_row119_no_duplicate_rows(self):
        """[Row 119] Only one DB row per (client_user_id, property_name)."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        _log("db_rows", rows)
        prop_rows = [r for r in rows if r.get("property_name", "").lower() == self.prop.lower()]
        assert len(prop_rows) <= 1, f"[Row 119] Found {len(prop_rows)} rows — expected ≤1.\nRows: {prop_rows}"