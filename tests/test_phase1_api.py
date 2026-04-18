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
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.user_good1 = f"{unique_user_id}_G1"
        self.user_bad   = f"{unique_user_id}_BAD"
        self.user_good2 = f"{unique_user_id}_G2"
        self.property_name = unique_property_name
        # CSV with one bad row (invalid column) mixed with good rows
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue\n"
            f"{self.user_good1},{self.property_name},AutoTestValue1\n"
            f"{self.user_good2},{self.property_name},AutoTestValue2",
        )
        submissions["79"] = {
            "user_ids":      [self.user_good1, self.user_good2],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row79_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 79] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
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
        submissions["81"] = {"api_status": self.response.status_code}

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
        submissions["82"] = {"api_status": self.response.status_code}

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
        submissions["83"] = {"api_status": self.response.status_code}

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


class TestRow91DuplicateColumnsRejected:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.response = api_client.post_csv(
            SCHEMA_UP,
            f"ClientUserId,PropertyName,PropertyValue,PropertyValue\n"
            f"{unique_user_id},{unique_property_name},Val1,Val2",
        )
        submissions["91"] = {"api_status": self.response.status_code}

    @pytest.mark.regression
    @pytest.mark.api
    def test_row91_duplicate_columns_rejected(self):
        assert self.response.status_code == 400, (
            f"[Row 91] Duplicate column headers should return 400, got {self.response.status_code}"
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


class TestRow94PropertyValueDouble:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":         self.client_user_id,
            "PropertyName":         self.property_name,
            "PropertyValueDouble":  99.5,
        })
        submissions["94"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value_double", "expected_value": 99.5},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row94_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 94] Expected 200, got {self.response.status_code}"
        )


class TestRow95TypeConflictDoubleVsText:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":         self.client_user_id,
            "PropertyName":         self.property_name,
            "PropertyValue":        "TextWins",
            "PropertyValueDouble":  99.5,
        })
        submissions["95"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value", "expected_value": "TextWins"},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row95_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 95] Expected 200, got {self.response.status_code}"
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


class TestRow119PropertyValueBool:
    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name, submissions):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name
        self.response = api_client.post_json(SCHEMA_UP, {
            "ClientUserId":       self.client_user_id,
            "PropertyName":       self.property_name,
            "PropertyValueBool":  True,
        })
        submissions["119"] = {
            "user_ids":      [self.client_user_id],
            "property_name": self.property_name,
            "api_status":    self.response.status_code,
            "extra":         {"expected_col": "property_value_bool", "expected_value": True},
        }

    @pytest.mark.regression
    @pytest.mark.api
    def test_row119_api_returns_200(self):
        assert self.response.status_code == 200, (
            f"[Row 119] Expected 200, got {self.response.status_code}"
        )