"""
test_phase1_api.py
──────────────────
Phase 1: fire every API request, assert the HTTP response only.
No DB calls here. Each test registers what it submitted into `submissions`
so Phase 2 knows which user IDs to verify.

Covers:
  Happy-path signals    (TC-SIG-01, 04, 06)
  Happy-path userprops  (TC-UP-01, 03, 05)
  Regression rows 76–119

Run order: this file runs BEFORE test_phase2_db.py (alphabetical).
"""
import time
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from utils import api_client

SCHEMA_UP  = "userproperties"
SCHEMA_SIG = "signals"


# ══════════════════════════════════════════════════════════════════════════════
#  HAPPY PATH — SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

class TestHappySignalsCSV:
    """TC-SIG-01"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA_SIG,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15,AutoTestGroup",
        )
        submissions["happy_sig_csv"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.api
    def test_api_returns_200(self):
        body = api_client.assert_happy_response(self.response, SCHEMA_SIG, "csv")
        assert body


class TestHappySignalsJSONSingle:
    """TC-SIG-04"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA_SIG, {
            "ClientUserId":  self.client_user_id,
            "SignalName":    self.signal_name,
            "SignalValue":   "40000.0",
            "ResponseTime":  "2024-01-15T10:00:00Z",
            "ResponseGroup": "AutoTestGroup",
        })
        submissions["happy_sig_json_single"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.api
    def test_api_returns_200(self):
        body = api_client.assert_happy_response(self.response, SCHEMA_SIG, "json")
        assert body


class TestHappySignalsJSONArray:
    """TC-SIG-06"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_id_1   = f"{unique_user_id}_A"
        self.user_id_2   = f"{unique_user_id}_B"
        self.signal_name = unique_signal_name
        self.response = api_client.post_json(SCHEMA_SIG, [
            {"ClientUserId": self.user_id_1, "SignalName": self.signal_name,
             "SignalValue": "111.0", "ResponseTime": "2024-01-15T10:00:00Z", "ResponseGroup": "AutoTestGroup"},
            {"ClientUserId": self.user_id_2, "SignalName": self.signal_name,
             "SignalValue": "222.0", "ResponseTime": "2024-01-15T11:00:00Z", "ResponseGroup": "AutoTestGroup"},
        ])
        submissions["happy_sig_json_array"] = {
            "user_ids":    [self.user_id_1, self.user_id_2],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.api
    def test_api_returns_200(self):
        body = api_client.assert_happy_response(self.response, SCHEMA_SIG, "json")
        assert body


# ══════════════════════════════════════════════════════════════════════════════
#  HAPPY PATH — USER PROPERTIES
# ══════════════════════════════════════════════════════════════════════════════

class TestHappyUpCSV:
    """TC-UP-01"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id},{self.property_name},AutoTestValue",
        )
        submissions["happy_up_csv"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.api
    def test_api_returns_200(self):
        body = api_client.assert_happy_response(self.response, SCHEMA_UP, "csv")
        assert body


class TestHappyUpJSONSingle:
    """TC-UP-03"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":  self.client_user_id,
            "PropertyName":  self.property_name,
            "PropertyValue": "AutoTestTextValue",
        })
        submissions["happy_up_json_single"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.api
    def test_api_returns_200(self):
        body = api_client.assert_happy_response(self.response, SCHEMA_UP, "json")
        assert body


class TestHappyUpJSONArray:
    """TC-UP-05"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_id_1     = f"{unique_user_id}_A"
        self.user_id_2     = f"{unique_user_id}_B"
        self.property_name = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": self.user_id_1, "PropertyName": self.property_name, "PropertyValue": "AutoTestValue1"},
            {"ClientUserId": self.user_id_2, "PropertyName": self.property_name, "PropertyValue": "AutoTestValue2"},
        ])
        submissions["happy_up_json_array"] = {
            "user_ids":      [self.user_id_1, self.user_id_2],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.api
    def test_api_returns_200(self):
        body = api_client.assert_happy_response(self.response, SCHEMA_UP, "json")
        assert body


# ══════════════════════════════════════════════════════════════════════════════
#  REGRESSION — ROWS 76–119  (API phase only)
# ══════════════════════════════════════════════════════════════════════════════

class TestRow76BasicCsvIngestion:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id},{self.property_name},AutoTestValue",
        )
        submissions["76"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row76_api_returns_200(self):
        body = api_client.assert_happy_response(self.response, SCHEMA_UP, "csv")
        assert body, "[Row 76] Expected 200 OK with body"


class TestRow77AbsentFalseNewProps:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":  self.client_user_id,
            "PropertyName":  self.property_name,
            "PropertyValue": "AutoTestValue",
        })
        submissions["77"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row77_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 77] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow78AbsentFalseUpdatedProps:
    """Two-step: send a record, then send it again (update). Both IDs tracked."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        # Initial insert
        api_client.post_json(SCHEMA_UP, {
            "ClientUserId":  self.client_user_id,
            "PropertyName":  self.property_name,
            "PropertyValue": "OriginalValue",
        })
        time.sleep(1)
        # Update same user+prop
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":  self.client_user_id,
            "PropertyName":  self.property_name,
            "PropertyValue": "UpdatedValue",
        })
        submissions["78"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_value": "UpdatedValue"},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row78_api_returns_200_on_update(self):
        assert self.response.status_code == 200, (
            f"[Row 78] Update call expected 200, got {self.response.status_code}"
        )


class TestRow79MultipleDataTypeValuesRejected:
    """Row 79 (Excel 80): CSV contains a bad row with both PropertyValue + PropertyValueDouble
    set at the same time (multi-datatype conflict). That row must be rejected/skipped.
    The two flanking valid rows must still be inserted."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_good1    = f"{unique_user_id}_G1"
        self.user_bad      = f"{unique_user_id}_BAD"
        self.user_good2    = f"{unique_user_id}_G2"
        self.property_name = unique_property_name

        # Header has both PropertyValue AND PropertyValueDouble columns.
        # Good rows leave PropertyValueDouble empty; bad row fills both.
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue,PropertyValueDouble\n"
            f"{self.user_good1},{self.property_name},AutoTestValue1,\n"
            f"{self.user_bad},{self.property_name},BadTextValue,99.5\n"
            f"{self.user_good2},{self.property_name},AutoTestValue2,",
        )
        submissions["79"] = {
            "user_ids":      [self.user_good1, self.user_good2],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"absent_user_ids": [self.user_bad]},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row79_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 79] Expected 200 (valid rows accepted, bad row skipped), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow80SpaceTrimming:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id       = unique_user_id
        self.client_user_id_padded = f"  {unique_user_id}  "
        self.property_name         = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id_padded},{self.property_name},  SpacedValue  ",
        )
        submissions["80"] = {
            "user_ids":      [self.client_user_id],  # trimmed version is what lands in DB
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_value": "SpacedValue"},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row80_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 80] Expected 200, got {self.response.status_code}"
        )


class TestRow81MissingClientUserIdColumn:
    @pytest.fixture(autouse=True)
    def _send(self, unique_property_name, submissions):
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"PropertyName,PropertyValue\n{self.property_name},SomeValue",
        )
        submissions["81"] = {
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row81_missing_client_user_id_returns_400(self):
        assert self.response.status_code == 400, (
            f"[Row 81] Missing ClientUserId column should return 400, got {self.response.status_code}.\n"
            f"Body: {self.response.text}"
        )


class TestRow82MissingPropertyNameColumn:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, submissions):
        self.client_user_id = unique_user_id
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyValue\n{self.client_user_id},SomeValue",
        )
        submissions["82"] = {
            "user_ids": [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row82_missing_property_name_returns_400(self):
        assert self.response.status_code == 400, (
            f"[Row 82] Missing PropertyName column should return 400, got {self.response.status_code}"
        )


class TestRow83MissingPropertyValueColumn:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName\n{self.client_user_id},{self.property_name}",
        )
        submissions["83"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row83_missing_property_value_returns_400(self):
        assert self.response.status_code == 400, (
            f"[Row 83] Missing PropertyValue column should return 400, got {self.response.status_code}"
        )


class TestRow84MissingClientUserIdValue:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid     = f"{unique_user_id}_V"
        self.property_name  = unique_property_name
        # One row with blank ClientUserId, one valid row
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f",{self.property_name},BadRow\n"
            f"{self.user_valid},{self.property_name},GoodValue",
        )
        submissions["84"] = {
            "user_ids":      [self.user_valid],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"absent_user_ids": [""]},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row84_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 84] Expected 200 (bad row skipped), got {self.response.status_code}"
        )


class TestRow85EmptyNullClientUserIdValue:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"NULL,{self.property_name},BadRow\n"
            f"{self.user_valid},{self.property_name},GoodValue",
        )
        submissions["85"] = {
            "user_ids":      [self.user_valid],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"absent_user_ids": ["NULL", ""]},  # NULL literal AND empty string
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row85_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 85] Expected 200, got {self.response.status_code}"
        )


class TestRow86MissingPropertyNameValue:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{unique_user_id},,BadRow\n"
            f"{self.user_valid},{self.property_name},GoodValue",
        )
        submissions["86"] = {
            "user_ids":      [self.user_valid],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"absent_user_ids": [unique_user_id]},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row86_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 86] Expected 200, got {self.response.status_code}"
        )


class TestRow87EmptyNullPropertyNameValue:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{unique_user_id},NULL,BadRow\n"
            f"{self.user_valid},{self.property_name},GoodValue",
        )
        submissions["87"] = {
            "user_ids":      [self.user_valid],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"absent_user_ids": [unique_user_id]},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row87_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 87] Expected 200, got {self.response.status_code}"
        )


class TestRow88MissingPropertyValueRowSkipped:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{unique_user_id},{self.property_name},\n"
            f"{self.user_valid},{self.property_name},GoodValue",
        )
        submissions["88"] = {
            "user_ids":      [self.user_valid],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"absent_user_ids": [unique_user_id]},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row88_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 88] Expected 200 (empty value row skipped), got {self.response.status_code}.\n"
            f"Body: {self.response.text}"
        )


class TestRow89EmptyPropertyValueIsInserted:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":  self.client_user_id,
            "PropertyName":  self.property_name,
            "PropertyValue": "",
        })
        submissions["89"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_value": ""},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row89_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 89] Expected 200 (empty string value is valid), got {self.response.status_code}.\n"
            f"Body: {self.response.text}"
        )


class TestRow90UnknownColumnsIgnored:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue,UnknownCol1,UnknownCol2\n"
            f"{self.client_user_id},{self.property_name},GoodValue,extra1,extra2",
        )
        submissions["90"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row90_api_ignores_extra_columns(self):
        assert self.response.status_code == 200, (
            f"[Row 90] Expected 200 (unknown columns ignored), got {self.response.status_code}.\n"
            f"Body: {self.response.text}"
        )


# ─── EXTRA TEST — no corresponding Excel row ─────────────────────────────────
# Excel row 92 = "file size >1GB — marked 'not tested'" and is skipped.
# This test validates duplicate CSV column headers and is a valid additional
# guard, but does NOT map to any Excel row number. The key "EXTRA_dup_cols"
# keeps it out of the row-coverage audit.

class TestExtraDuplicateColumnsRejected:
    """EXTRA (not in Excel): Duplicate column headers in CSV must return 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue,PropertyValue\n"
            f"{self.client_user_id},{self.property_name},Val1,Val2",
        )
        submissions["EXTRA_dup_cols"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_extra_duplicate_columns_rejected(self):
        assert self.response.status_code == 400, (
            f"[EXTRA] Duplicate column headers should return 400, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )

class TestRow92PropertyValueText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":  self.client_user_id,
            "PropertyName":  self.property_name,
            "PropertyValue": "SomeTextValue",
        })
        submissions["92"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value", "expected_value": "SomeTextValue"},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row92_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 92] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow93TypeConflictTextVsInt:
    """Send PropertyValue (text) and PropertyValueInt for same user — text wins."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":      self.client_user_id,
            "PropertyName":      self.property_name,
            "PropertyValue":     "TextWins",
            "PropertyValueInt":  42,
        })
        submissions["93"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value", "expected_value": "TextWins"},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row93_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 93] Expected 200, got {self.response.status_code}"
        )


class TestRow94NoDuplicatePropertyValueText:
    """Row 94 (Excel 95): CSV with identical PropertyValue rows for the same user+property.
    API should return 200; DB should not contain duplicate rows."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{self.client_user_id},{self.property_name},AutoTestTextValue\n"
            f"{self.client_user_id},{self.property_name},AutoTestTextValue\n"  # exact duplicate
            f"{self.client_user_id},{self.property_name},DifferentTextValue",  # different — should coexist
        )
        submissions["94"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row94_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 94] Expected 200, got {self.response.status_code}. "
            f"Body: {self.response.text}"
        )


class TestRow95CaseInsensitiveDedupText:
    """Row 95 (Excel 96): CSV with space-padded user ID rows that are duplicates after trimming.
    API should treat them as the same record and not insert duplicates."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"   {self.client_user_id}  ,  {self.property_name} ,AutoTestTextValue\n"  # padded — same after trim
            f"{self.client_user_id},{self.property_name},AutoTestTextValue\n"           # clean duplicate
            f"{self.client_user_id},{self.property_name},DifferentTextValue",           # different value
        )
        submissions["95"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row95_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 95] Expected 200, got {self.response.status_code}. "
            f"Body: {self.response.text}"
        )


class TestRow96PropertyValueInt:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":      self.client_user_id,
            "PropertyName":      self.property_name,
            "PropertyValueInt":  42,
        })
        submissions["96"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value_int", "expected_value": 42},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row96_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 96] Expected 200, got {self.response.status_code}"
        )


class TestRow97TypeConflictIntVsText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":      self.client_user_id,
            "PropertyName":      self.property_name,
            "PropertyValue":     "TextWins",
            "PropertyValueInt":  42,
        })
        submissions["97"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value", "expected_value": "TextWins"},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row97_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 97] Expected 200, got {self.response.status_code}"
        )


class TestRow98NoDuplicatePropertyValueInt:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueInt\n"
            f"{self.client_user_id},{self.property_name},40000\n"
            f"{self.client_user_id},{self.property_name},40000\n"
            f"{self.client_user_id},{self.property_name},20000",
        )
        submissions["98"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row98_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 98] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow99CaseInsensitiveDedupInt:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueInt\n"
            f"   {self.client_user_id}  ,  {self.property_name} ,   40000\n"
            f"{self.client_user_id},{self.property_name},40000\n"
            f"{self.client_user_id},{self.property_name},20000",
        )
        submissions["99"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row99_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 99] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow100PropertyValueDouble:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDouble\n"
            f"{self.client_user_id},{self.property_name},99.5",
        )
        submissions["100"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row100_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 100] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow101TypeConflictDoubleVsText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDouble\n{self.client_user_id},{self.property_name},99.5",
        )
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.property_name},SomeTextValue",
        )
        submissions["101"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row101_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 101] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow102NoDuplicatePropertyValueDouble:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDouble\n"
            f"{self.client_user_id},{self.property_name},99.5\n"
            f"{self.client_user_id},{self.property_name},99.5\n"
            f"{self.client_user_id},{self.property_name},1.23",
        )
        submissions["102"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row102_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 102] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow103CaseInsensitiveDedupDouble:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDouble\n"
            f"   {self.client_user_id}  ,  {self.property_name} ,   99.5\n"
            f"{self.client_user_id},{self.property_name},99.5\n"
            f"{self.client_user_id},{self.property_name},1.23",
        )
        submissions["103"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row103_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 103] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow104PropertyValueDate:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDate\n"
            f"{self.client_user_id},{self.property_name},2023-02-03",
        )
        submissions["104"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row104_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 104] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow105TypeConflictDateVsText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDate\n{self.client_user_id},{self.property_name},2023-02-03",
        )
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.property_name},SomeTextValue",
        )
        submissions["105"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row105_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 105] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow106NoDuplicatePropertyValueDate:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDate\n"
            f"{self.client_user_id},{self.property_name},2023-02-03\n"
            f"{self.client_user_id},{self.property_name},2023-02-03",
        )
        submissions["106"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row106_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 106] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow107CaseInsensitiveDedupDate:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueDate\n"
            f"   {self.client_user_id}  ,  {self.property_name} ,   2023-02-03\n"
            f"{self.client_user_id},{self.property_name},2023-02-03",
        )
        submissions["107"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row107_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 107] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow108PropertyValueCurrency:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueCurrency\n"
            f"{self.client_user_id},{self.property_name},2000.00",
        )
        submissions["108"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row108_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 108] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow109TypeConflictCurrencyVsText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueCurrency\n{self.client_user_id},{self.property_name},2000.00",
        )
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.property_name},SomeTextValue",
        )
        submissions["109"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row109_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 109] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow110NoDuplicatePropertyValueCurrency:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueCurrency\n"
            f"{self.client_user_id},{self.property_name},2000.00\n"
            f"{self.client_user_id},{self.property_name},2000.00",
        )
        submissions["110"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row110_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 110] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow111CaseInsensitiveDedupCurrency:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueCurrency\n"
            f"   {self.client_user_id}  ,  {self.property_name} ,   2000.00\n"
            f"{self.client_user_id},{self.property_name},2000.00",
        )
        submissions["111"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row111_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 111] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow112PropertyValueBool:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueBool\n"
            f"{self.client_user_id},{self.property_name},true",
        )
        submissions["112"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row112_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 112] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow113TypeConflictBoolVsText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueBool\n{self.client_user_id},{self.property_name},true",
        )
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n{self.client_user_id},{self.property_name},SomeTextValue",
        )
        submissions["113"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row113_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 113] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow114NoDuplicatePropertyValueBool:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueBool\n"
            f"{self.client_user_id},{self.property_name},true\n"
            f"{self.client_user_id},{self.property_name},true",
        )
        submissions["114"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row114_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 114] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow115CaseInsensitiveDedupBool:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name = unique_property_name
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValueBool\n"
            f"   {self.client_user_id}  ,  {self.property_name} ,   true\n"
            f"{self.client_user_id},{self.property_name},true",
        )
        submissions["115"] = {
            "user_ids": [self.client_user_id],
            "property_name": self.property_name,
            "api_status": self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row115_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 115] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow116PropertyValueJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.json_payload   = {"key": "value", "nested": {"x": 1}}
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":       self.client_user_id,
            "PropertyName":       self.property_name,
            "PropertyValueJson":  self.json_payload,
        })
        submissions["116"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value_json", "expected_value": self.json_payload},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row116_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 116] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow117PropertyValueJsonArray:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.json_payload   = [1, 2, {"three": 3}]
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":       self.client_user_id,
            "PropertyName":       self.property_name,
            "PropertyValueJson":  self.json_payload,
        })
        submissions["117"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value_json", "expected_value": self.json_payload},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row117_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 117] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


class TestRow118TypeConflictJsonVsText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.json_payload   = {"key": "value"}
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":       self.client_user_id,
            "PropertyName":       self.property_name,
            "PropertyValue":      "TextValue",
            "PropertyValueJson":  self.json_payload,
        })
        submissions["118"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value_json", "expected_value": self.json_payload},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row118_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 118] Expected 200, got {self.response.status_code}"
        )


class TestRow119NoDuplicatePropertyValueJson:
    """Row 119 (Excel 120): Sending the same PropertyValueJson twice for the same user+property.
    No duplicate rows should be stored in the DB."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.json_payload   = {"key": "value", "nested": {"x": 1}}

        # Send identical JSON payload twice
        api_client.post_json(SCHEMA_UP, {
            "ClientUserId":      self.client_user_id,
            "PropertyName":      self.property_name,
            "PropertyValueJson": self.json_payload,
        })
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":      self.client_user_id,
            "PropertyName":      self.property_name,
            "PropertyValueJson": self.json_payload,
        })
        submissions["119"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row119_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 119] Expected 200, got {self.response.status_code}. "
            f"Body: {self.response.text}"
        )

# ══════════════════════════════════════════════════════════════════════════════
#  REGRESSION — ROWS 120–152  (JSON format)
# ══════════════════════════════════════════════════════════════════════════════

class TestRow120BasicJsonIngestion:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId": self.client_user_id,
            "PropertyName": self.property_name,
            "PropertyValue": "AutoTestValue",
        })
        submissions["120"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row120_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 120] Got {self.response.status_code}. Body: {self.response.text}"


class TestRow121AbsentFalseNewPropsJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId": self.client_user_id,
            "PropertyName": self.property_name,
            "PropertyValue": "AutoTestValue",
        })
        submissions["121"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row121_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 121] Got {self.response.status_code}"


class TestRow122AbsentFalseUpdatedPropsJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "OriginalValue"})
        time.sleep(1)
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "UpdatedValue"})
        submissions["122"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code, "extra": {"expected_value": "UpdatedValue"}}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row122_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 122] Got {self.response.status_code}"


class TestRow123SpaceTrimmingJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId": f"  {self.client_user_id}  ",
            "PropertyName": f"  {self.property_name}  ",
            "PropertyValue": "  SpacedValue  ",
        })
        submissions["123"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code, "extra": {"expected_value": "SpacedValue"}}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row123_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 123] Got {self.response.status_code}"


class TestRow124MissingClientUserIdJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_property_name, submissions):
        self.property_name = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"PropertyName": self.property_name, "PropertyValue": "SomeValue"})
        submissions["124"] = {"property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row124_missing_client_user_id_returns_400(self):
        assert self.response.status_code == 400, f"[Row 124] Expected 400, got {self.response.status_code}. Body: {self.response.text}"


class TestRow125MissingPropertyNameJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, submissions):
        self.client_user_id = unique_user_id
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyValue": "SomeValue"})
        submissions["125"] = {"user_ids": [self.client_user_id], "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row125_missing_property_name_returns_400(self):
        assert self.response.status_code == 400, f"[Row 125] Expected 400, got {self.response.status_code}. Body: {self.response.text}"


class TestRow126MissingPropertyValueJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name})
        submissions["126"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row126_missing_property_value_returns_400(self):
        assert self.response.status_code == 400, f"[Row 126] Expected 400, got {self.response.status_code}. Body: {self.response.text}"


class TestRow127MissingClientUserIdValueJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"PropertyName": self.property_name, "PropertyValue": "BadRow"},
            {"ClientUserId": self.user_valid, "PropertyName": self.property_name, "PropertyValue": "GoodValue"},
        ])
        submissions["127"] = {"user_ids": [self.user_valid], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row127_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 127] Got {self.response.status_code}"


class TestRow128NullClientUserIdJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": None, "PropertyName": self.property_name, "PropertyValue": "BadRow"},
            {"ClientUserId": self.user_valid, "PropertyName": self.property_name, "PropertyValue": "GoodValue"},
        ])
        submissions["128"] = {"user_ids": [self.user_valid], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row128_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 128] Got {self.response.status_code}"


class TestRow129MissingPropertyNameValueJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": unique_user_id, "PropertyValue": "BadRow"},
            {"ClientUserId": self.user_valid, "PropertyName": self.property_name, "PropertyValue": "GoodValue"},
        ])
        submissions["129"] = {"user_ids": [self.user_valid], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row129_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 129] Got {self.response.status_code}"


class TestRow130NullPropertyNameJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": unique_user_id, "PropertyName": None, "PropertyValue": "BadRow"},
            {"ClientUserId": self.user_valid, "PropertyName": self.property_name, "PropertyValue": "GoodValue"},
        ])
        submissions["130"] = {"user_ids": [self.user_valid], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row130_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 130] Got {self.response.status_code}"


class TestRow131NewUserNotInMappingJson:
    """Positive test: a brand-new ClientUserId not yet in client_user_mapping must still be ingested."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId": self.client_user_id,
            "PropertyName": self.property_name,
            "PropertyValue": "AutoTestValue",
        })
        submissions["131"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row131_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 131] Got {self.response.status_code}"


class TestRow132NullPropertyValueRowSkippedJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_valid    = f"{unique_user_id}_V"
        self.property_name = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": unique_user_id, "PropertyName": self.property_name},
            {"ClientUserId": self.user_valid, "PropertyName": self.property_name, "PropertyValue": "GoodValue"},
        ])
        submissions["132"] = {"user_ids": [self.user_valid], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row132_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 132] Got {self.response.status_code}"


# row133 intentionally absent — Excel row 134 = 'file size >1GB, not tested'


class TestRow134PropertyValueTextJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "SomeTextValue"})
        submissions["134"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code, "extra": {"expected_col": "property_value", "expected_value": "SomeTextValue"}}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row134_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 134] Got {self.response.status_code}"


class TestRow135TypeConflictTextJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "TextWins", "PropertyValueDouble": 99.5})
        submissions["135"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code, "extra": {"expected_col": "property_value", "expected_value": "TextWins"}}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row135_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 135] Got {self.response.status_code}"


class TestRow136NoDuplicateTextJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "AutoTestTextValue"},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "AutoTestTextValue"},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "DifferentTextValue"},
        ])
        submissions["136"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row136_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 136] Got {self.response.status_code}"


class TestRow137CaseInsensitiveDedupTextJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "AutoTestTextValue"},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "AUTOTESTTEXTVALUE"},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "DifferentValue"},
        ])
        submissions["137"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row137_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 137] Got {self.response.status_code}"


class TestRow138PropertyValueDoubleJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDouble": 99.5})
        submissions["138"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row138_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 138] Got {self.response.status_code}"


class TestRow139TypeConflictDoubleJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDouble": 99.5})
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "SomeTextValue"})
        submissions["139"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row139_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 139] Got {self.response.status_code}"


class TestRow140NoDuplicateDoubleJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDouble": 99.5},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDouble": 99.5},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDouble": 1.23},
        ])
        submissions["140"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row140_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 140] Got {self.response.status_code}"


class TestRow141PropertyValueDateJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDate": "2023-02-03"})
        submissions["141"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row141_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 141] Got {self.response.status_code}"


class TestRow142TypeConflictDateJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDate": "2023-02-03"})
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "SomeTextValue"})
        submissions["142"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row142_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 142] Got {self.response.status_code}"


class TestRow143NoDuplicateDateJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDate": "2023-02-03"},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueDate": "2023-02-03"},
        ])
        submissions["143"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row143_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 143] Got {self.response.status_code}"


class TestRow144PropertyValueCurrencyJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueCurrency": 2000.00})
        submissions["144"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row144_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 144] Got {self.response.status_code}"


class TestRow145TypeConflictCurrencyJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueCurrency": 2000.00})
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "SomeTextValue"})
        submissions["145"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row145_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 145] Got {self.response.status_code}"


class TestRow146NoDuplicateCurrencyJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueCurrency": 2000.00},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueCurrency": 2000.00},
        ])
        submissions["146"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row146_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 146] Got {self.response.status_code}"


class TestRow147PropertyValueBoolJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueBool": True})
        submissions["147"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row147_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 147] Got {self.response.status_code}"


class TestRow148TypeConflictBoolJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueBool": True})
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "SomeTextValue"})
        submissions["148"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row148_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 148] Got {self.response.status_code}"


class TestRow149NoDuplicateBoolJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, [
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueBool": True},
            {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueBool": True},
        ])
        submissions["149"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row149_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 149] Got {self.response.status_code}"


class TestRow150PropertyValueJsonJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.json_payload   = {"key": "value", "nested": {"x": 1}}
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueJson": self.json_payload})
        submissions["150"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row150_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 150] Got {self.response.status_code}"


class TestRow151TypeConflictJsonJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueJson": {"key": "value"}})
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValue": "SomeTextValue"})
        submissions["151"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row151_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 151] Got {self.response.status_code}"


class TestRow152NoDuplicateJsonJson:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        payload = {"key": "value", "nested": {"x": 1}}
        api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueJson": payload})
        self.response = api_client.post_json(SCHEMA_UP, {"ClientUserId": self.client_user_id, "PropertyName": self.property_name, "PropertyValueJson": payload})
        submissions["152"] = {"user_ids": [self.client_user_id], "property_name": self.property_name, "api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row152_api_returns_200(self):
        assert self.response.status_code == 200, f"[Row 152] Got {self.response.status_code}"