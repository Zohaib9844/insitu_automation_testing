"""
test_signals_phase1_api.py
──────────────────────────
Phase 1 — API assertions ONLY for the signals schema (CSV + JSON).

Maps directly to the InSitu QA Excel sheet — every row is listed so the
HTML report "Case Order" column tells you the exact Excel row.

  Row 5   TC-SIG-01 : CSV basic happy path                          → 200
  Row 6   TC-SIG-04 : JSON single happy path                        → 200
  Row 7   TC-SIG-06 : JSON array happy path                         → 200
  Row 13            : Wrong CSV column names                         → 400
  Row 14            : Wrong JSON field names                         → 400
  Row 16            : format=csv but JSON body sent                  → 400
  Row 17            : format=json but CSV body sent                  → 400
  Row 19            : CSV — all required tables receive data         → 200
  Row 20            : CSV — responseTime ISO-8601 variants accepted  → 200
  Row 21            : CSV — correct columns in client_users_data     → 200
  Row 23            : CSV — space-trimming on all fields             → 200
  Row 24            : CSV — case-insensitive dedup                   → 200
  Row 25            : CSV — no duplicate insertion                   → 200
  Row 26            : CSV — minimum mandatory fields only            → 200
  Row 27            : CSV — missing ClientUserId column              → 400
  Row 28            : CSV — missing SignalName column                → 400
  Row 29            : CSV — missing SignalValue column               → 400
  Row 30            : CSV — missing ResponseTime column              → 400
  Row 32            : CSV — misspelled optional columns ignored      → 200
  Row 33            : CSV — blank ClientUserId value in row skipped  → 200
  Row 34            : CSV — NULL ClientUserId value in row skipped   → 200
  Row 35            : CSV — blank SignalName value in row skipped    → 200
  Row 36            : CSV — NULL SignalName value in row skipped     → 200
  Row 37            : CSV — empty/null SignalValue inserted anyway   → 200
  Row 38            : CSV — bad ResponseTime in row skipped          → 200
  Row 39            : CSV — optional fields (ResponseGroup/platform) → 200
  Row 40            : CSV — missing optional values stored as NULL   → 200
  Row 41            : CSV — no duplicate client_user_mapping rows    → 200
  Row 43            : CSV — signal_value_numeric + currency fields   → 200
  Row 44            : CSV — signal_value_date field                  → 200
  Row 45            : CSV — signal_value_date_duration field         → 200
  Row 46            : CSV — signal_value_boolean field               → 200
  Row 47            : JSON — all required tables receive data        → 200
  Row 48            : JSON — responseTime ISO-8601 variants          → 200
  Row 49            : JSON — correct columns in client_users_data    → 200
  Row 51            : JSON — space-trimming on all fields            → 200
  Row 52            : JSON — case-insensitive dedup                  → 200
  Row 53            : JSON — no duplicate insertion                  → 200
  Row 54            : JSON — minimum mandatory fields only           → 200
  Row 55            : JSON — missing ClientUserId column             → 400
  Row 56            : JSON — missing SignalName column               → 400
  Row 57            : JSON — missing SignalValue column              → 400
  Row 58            : JSON — missing ResponseTime column             → 400
  Row 59            : JSON — multiple mandatory columns missing      → 400
  Row 60            : JSON — misspelled optional columns ignored     → 200

SKIPPED rows (manual or known-bug — noted inline):
  Row 18 : 512 MB size-limit test  (manual only)
  Row 42 : > 1 GB file             (manual only)
  Row 70 : > 1 GB JSON file (manual only)

──────────────────────────────────────────────────────────────────────────────
Run just Phase 1 (API only):
  pytest tests/test_signals_phase1_api.py -v \\
         --html=reports/signals_report.html --self-contained-html

Run Phase 1 + Phase 2 together (full signal suite):
  pytest tests/test_signals_phase1_api.py tests/test_signals_phase2_db.py \\
         -v --html=reports/signals_report.html --self-contained-html
──────────────────────────────────────────────────────────────────────────────
"""
import time
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from utils import api_client

SCHEMA = "signals"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 5  —  TC-SIG-01 : CSV Happy Path
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow5BasicCsvHappy:
    """Excel Row 5 — TC-SIG-01: POST signals CSV → 200 OK, correct schema/format/filePath."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15,AutoTestGroup",
        )
        submissions["sig_row5"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow5_csv_returns_200_with_correct_body(self):
        """TC-SIG-01: Basic CSV upload returns 200 with correct schema/format/filePath."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "csv")
        assert body, "[Row 5] Response body must not be empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 6  —  TC-SIG-04 : JSON Single Object Happy Path
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow6JsonSingleHappy:
    """Excel Row 6 — TC-SIG-04: POST signals JSON (single object) → 200 OK."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, {
            "ClientUserId":  self.client_user_id,
            "SignalName":    self.signal_name,
            "SignalValue":   "40000.0",
            "ResponseTime":  "2024-01-15T10:00:00Z",
            "ResponseGroup": "AutoTestGroup",
        })
        submissions["sig_row6"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow6_json_single_returns_200_with_correct_body(self):
        """TC-SIG-04: JSON single-object upload returns 200 with correct body."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body, "[Row 6] Response body must not be empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 7  —  TC-SIG-06 : JSON Array Happy Path
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow7JsonArrayHappy:
    """Excel Row 7 — TC-SIG-06: POST signals JSON (array) → 200 OK."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_id_1   = f"{unique_user_id}_A"
        self.user_id_2   = f"{unique_user_id}_B"
        self.signal_name = unique_signal_name
        self.response = api_client.post_json(SCHEMA, [
            {
                "ClientUserId":  self.user_id_1,
                "SignalName":    self.signal_name,
                "SignalValue":   "111.0",
                "ResponseTime":  "2024-01-15T10:00:00Z",
                "ResponseGroup": "AutoTestGroup",
            },
            {
                "ClientUserId":  self.user_id_2,
                "SignalName":    self.signal_name,
                "SignalValue":   "222.0",
                "ResponseTime":  "2024-01-15T11:00:00Z",
                "ResponseGroup": "AutoTestGroup",
            },
        ])
        submissions["sig_row7"] = {
            "user_ids":    [self.user_id_1, self.user_id_2],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow7_json_array_returns_200_with_correct_body(self):
        """TC-SIG-06: JSON array upload returns 200 with correct body."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body, "[Row 7] Response body must not be empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 13  —  Wrong CSV column names → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow13WrongCsvFormat:
    """Excel Row 13: CSV with entirely wrong column names → 400 + 'No Mapping Found for ClientUserId'."""

    @pytest.fixture(autouse=True)
    def _send(self, submissions):
        self.response = api_client.post_csv(
            SCHEMA,
            "WrongHeader1,WrongHeader2,WrongHeader3,WrongHeader4\n"
            "BadData1,BadData2,BadData3,BadData4",
        )
        submissions["sig_row13"] = {
            "user_ids":   [],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow13_wrong_csv_columns_returns_400(self):
        """Row 13: Invalid CSV column names must be rejected with 400."""
        assert self.response.status_code == 400, (
            f"[Row 13] Expected 400, got {self.response.status_code}. "
            f"Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 14  —  Wrong JSON field names → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow14WrongJsonFormat:
    """Excel Row 14: JSON with entirely wrong field names → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, submissions):
        self.response = api_client.post_json(SCHEMA, {
            "WrongField1": "Test User 2",
            "WrongField2": "2024-01-11T13:32:25.728Z",
            "WrongField3": "Test Signal",
            "WrongField4": "Test Value",
        })
        submissions["sig_row14"] = {
            "user_ids":   [],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow14_wrong_json_fields_returns_400(self):
        """Row 14: Invalid JSON field names must be rejected with 400."""
        assert self.response.status_code == 400, (
            f"[Row 14] Expected 400, got {self.response.status_code}. "
            f"Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 16  —  format=csv but JSON body sent → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow16FormatMismatchCsvParamJsonBody:
    """Excel Row 16: format=csv query param but body is JSON → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        # Uses CSV headers but sends a JSON-serialised dict as the body text
        import json, requests, config
        self.response = requests.post(
            config.API_BASE_URL,
            params={"schema": SCHEMA, "format": "csv"},
            headers=config.API_HEADERS_CSV,          # Content-Type: text/plain
            data=json.dumps({                        # body is JSON, not CSV
                "ClientUserId":  self.client_user_id,
                "SignalName":    self.signal_name,
                "SignalValue":   "40000.0",
                "ResponseTime":  "2024-01-15T10:00:00Z",
                "ResponseGroup": "TestData",
            }).encode("utf-8"),
            timeout=config.API_TIMEOUT,
        )
        submissions["sig_row16"] = {
            "user_ids":   [],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow16_csv_format_with_json_body_returns_400(self):
        """Row 16: format=csv but JSON body → 400."""
        assert self.response.status_code == 400, (
            f"[Row 16] Expected 400 when format=csv but JSON body sent, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 17  —  format=json but CSV body sent → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow17FormatMismatchJsonParamCsvBody:
    """Excel Row 17: format=json query param but body is CSV text → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        import requests, config
        self.response = requests.post(
            config.API_BASE_URL,
            params={"schema": SCHEMA, "format": "json"},
            headers=config.API_HEADERS_JSON,          # Content-Type: application/json
            data=(                                    # body is CSV text, not JSON
                f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup\n"
                f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15,TestData"
            ).encode("utf-8"),
            timeout=config.API_TIMEOUT,
        )
        submissions["sig_row17"] = {
            "user_ids":   [],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow17_json_format_with_csv_body_returns_400(self):
        """Row 17: format=json but CSV body → 400."""
        assert self.response.status_code == 400, (
            f"[Row 17] Expected 400 when format=json but CSV body sent, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ── SKIP : Row 18  (512 MB size-limit — manual test only) ────────────────────


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 19  —  CSV ingests data into all required tables
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow19CsvAllTablesIngested:
    """Excel Row 19: CSV upload → API 200; DB phase verifies client_users_data,
    client_user_mapping, and raw_signals all receive rows."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15",
        )
        submissions["sig_row19"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"check_tables": ["client_users_data", "client_user_mapping", "raw_signals"]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow19_csv_returns_200(self):
        """Row 19: CSV upload with minimum required fields returns 200."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "csv")
        assert body, "[Row 19] Response body empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 20  —  CSV responseTime ISO-8601 format variants
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow20CsvResponseTimeFormats:
    """Excel Row 20: ResponseTime accepts multiple ISO-8601 datetime variants in CSV."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_tz      = f"{unique_user_id}_TZ"
        self.user_utc     = f"{unique_user_id}_UTC"
        self.user_plain   = f"{unique_user_id}_PLAIN"
        self.user_date    = f"{unique_user_id}_DATE"
        self.signal_name  = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_tz},{self.signal_name},40000.0,2023-01-22T18:30:30-05:00,TestData,kb,meta1\n"
            f"{self.user_utc},{self.signal_name},40000.0,2023-01-22T18:30:30Z,TestData,kb,meta2\n"
            f"{self.user_plain},{self.signal_name},40000.0,2023-01-22T18:30:30,TestData,kb,meta3\n"
            f"{self.user_date},{self.signal_name},40000.0,2023-01-22,TestData,kb,meta4",
        )
        submissions["sig_row20"] = {
            "user_ids":    [self.user_tz, self.user_utc, self.user_plain, self.user_date],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow20_iso8601_variants_accepted(self):
        """Row 20: Multiple ISO-8601 ResponseTime formats must all be accepted → 200."""
        assert self.response.status_code == 200, (
            f"[Row 20] Expected 200 for ISO-8601 variant dates, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 21  —  CSV correct columns ingested into client_users_data
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow21CsvCorrectColumnsIngested:
    """Excel Row 21: CSV upload stores all mapped columns correctly in client_users_data."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15,AutoTestGroup,kb,AutoMeta",
        )
        submissions["sig_row21"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra": {
                "expected_columns": {
                    "signal_name":    self.signal_name,
                    "signal_value":   "40000.0",
                    "response_group": "AutoTestGroup",
                    "platform":       "kb",
                },
            },
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow21_csv_correct_columns_returns_200(self):
        """Row 21: CSV with all columns returns 200; DB phase checks column values."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "csv")
        assert body, "[Row 21] Response body empty"


# ── SKIP : Row 22  (projectName CSV — known bug, Azure ticket #5705) ─────────


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 23  —  CSV space trimming on all fields
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow23CsvSpaceTrimming:
    """Excel Row 23: API trims leading/trailing spaces from all CSV field values."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id        = unique_user_id
        self.client_user_id_padded = f"   {unique_user_id}   "
        self.signal_name           = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.client_user_id_padded},  {self.signal_name}  ,  40000.0  ,"
            f"  2024-01-15  ,  AutoTestGroup  ,  kb  ,  AutoMeta  ",
        )
        submissions["sig_row23"] = {
            "user_ids":    [self.client_user_id],  # trimmed version lands in DB
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow23_csv_space_trimming_returns_200(self):
        """Row 23: CSV with padded fields returns 200; DB phase checks trimmed values."""
        assert self.response.status_code == 200, (
            f"[Row 23] Expected 200 for space-padded CSV, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 24  —  CSV case-insensitive deduplication
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow24CsvCaseInsensitiveDedup:
    """Excel Row 24: Three rows with same signal but different casing → stored once (dedup)."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        uid  = self.client_user_id
        sig  = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{uid},{sig},40000.0,2024-01-15,TestData,kb,meta\n"
            f"{uid.lower()},{sig.lower()},40000.0,2024-01-15,TestData,kb,meta\n"
            f"{uid.upper()},{sig.upper()},40000.0,2024-01-15,TestData,kb,meta",
        )
        submissions["sig_row24"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow24_case_insensitive_dedup_returns_200(self):
        """Row 24: Case-variant duplicates return 200; DB phase verifies single row stored."""
        assert self.response.status_code == 200, (
            f"[Row 24] Expected 200 for case-variant dedup, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 25  —  CSV no duplicate data inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow25CsvNoDuplicate:
    """Excel Row 25: Two rows with identical (ClientUserId, SignalName, SignalValue) → deduped."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        uid = self.client_user_id
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{uid},{sig},40000.0,2024-03-02,Test1Data,kb1,meta1\n"
            f"{uid},{sig},40000.0,2024-04-02,Test2Data,kb2,meta2",
        )
        submissions["sig_row25"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow25_no_duplicate_insertion_returns_200(self):
        """Row 25: Duplicate (user+signal+value) returns 200; DB phase verifies ≤1 row."""
        assert self.response.status_code == 200, (
            f"[Row 25] Expected 200 for duplicate submission, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 26  —  CSV minimum mandatory fields only
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow26CsvMinimumFields:
    """Excel Row 26: Only the 4 mandatory columns (ClientUserId, SignalName, SignalValue, ResponseTime)."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15",
        )
        submissions["sig_row26"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow26_csv_minimum_fields_returns_200(self):
        """Row 26: Minimum required CSV columns → 200 OK."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "csv")
        assert body, "[Row 26] Response body empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 27  —  CSV missing ClientUserId column → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow27CsvMissingClientUserIdColumn:
    """Excel Row 27: CSV header missing ClientUserId column → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_signal_name, submissions):
        self.signal_name = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"SignalName,SignalValue,ResponseTime\n"
            f"{self.signal_name},40000.0,2024-01-15",
        )
        submissions["sig_row27"] = {
            "user_ids":   [],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow27_missing_client_user_id_column_returns_400(self):
        """Row 27: Missing ClientUserId column → 400 with 'No Mapping Found for ClientUserId'."""
        assert self.response.status_code == 400, (
            f"[Row 27] Expected 400 for missing ClientUserId column, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 28  —  CSV missing SignalName column → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow28CsvMissingSignalNameColumn:
    """Excel Row 28: CSV header missing SignalName column → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, submissions):
        self.client_user_id = unique_user_id
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalValue,ResponseTime\n"
            f"{self.client_user_id},40000.0,2024-01-15",
        )
        submissions["sig_row28"] = {
            "user_ids":   [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow28_missing_signal_name_column_returns_400(self):
        """Row 28: Missing SignalName column → 400 with 'No Mapping Found for SignalName'."""
        assert self.response.status_code == 400, (
            f"[Row 28] Expected 400 for missing SignalName column, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 29  —  CSV missing SignalValue column → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow29CsvMissingSignalValueColumn:
    """Excel Row 29: CSV header missing SignalValue column → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,ResponseTime\n"
            f"{self.client_user_id},{self.signal_name},2024-01-15",
        )
        submissions["sig_row29"] = {
            "user_ids":   [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow29_missing_signal_value_column_returns_400(self):
        """Row 29: Missing SignalValue column → 400."""
        assert self.response.status_code == 400, (
            f"[Row 29] Expected 400 for missing SignalValue column, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 30  —  CSV missing ResponseTime column → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow30CsvMissingResponseTimeColumn:
    """Excel Row 30: CSV header missing ResponseTime column → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue\n"
            f"{self.client_user_id},{self.signal_name},40000.0",
        )
        submissions["sig_row30"] = {
            "user_ids":   [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow30_missing_response_time_column_returns_400(self):
        """Row 30: Missing ResponseTime column → 400."""
        assert self.response.status_code == 400, (
            f"[Row 30] Expected 400 for missing ResponseTime column, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ── SKIP : Row 31  (typo in column name — ambiguous expected behaviour) ───────


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 32  —  CSV misspelled/extra optional columns are ignored → 200
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow32CsvMisspelledOptionalColumns:
    """Excel Row 32: Misspelled optional column names are silently ignored → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,RespGrp,Platfrm,MetaDeta\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15,TestData,kb,meta",
        )
        submissions["sig_row32"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow32_misspelled_optional_columns_ignored(self):
        """Row 32: Misspelled optional column names should not cause 4xx — mandatory data still ingested."""
        assert self.response.status_code == 200, (
            f"[Row 32] Expected 200 (optional column typos ignored), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 33  —  CSV blank ClientUserId value in one row → row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow33CsvBlankClientUserIdValue:
    """Excel Row 33: Row with blank ClientUserId value is skipped; other rows ingested."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good    = f"{unique_user_id}_GOOD"
        self.signal_name  = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_good},{sig},40000.0,2024-01-15,TestData,kb,meta\n"
            f",{sig},40000.0,2024-01-15,TestData,kb,meta\n"                 # blank UID
            f"{unique_user_id}_BAD2,{sig},40000.0,2024-01-15,TestData,kb,meta",  # valid row 2
        )
        submissions["sig_row33"] = {
            "user_ids":    [self.user_good, f"{unique_user_id}_BAD2"],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": [""]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow33_blank_client_user_id_row_skipped_returns_200(self):
        """Row 33: Blank ClientUserId in a row → that row skipped, others ingested → 200."""
        assert self.response.status_code == 200, (
            f"[Row 33] Expected 200 (bad row skipped), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 34  —  CSV NULL ClientUserId value → row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow34CsvNullClientUserIdValue:
    """Excel Row 34: Row with literal 'NULL' ClientUserId is skipped; others ingested."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good   = f"{unique_user_id}_GOOD"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_good},{sig},40000.0,2024-01-15,TestData,kb,meta\n"
            f"NULL,{sig},40000.0,2024-01-15,TestData,kb,meta\n"            # NULL UID
            f"null,{sig},40000.0,2024-01-15,TestData,kb,meta",             # null UID
        )
        submissions["sig_row34"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": ["NULL", "null"]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow34_null_client_user_id_row_skipped_returns_200(self):
        """Row 34: NULL/null ClientUserId rows skipped → 200."""
        assert self.response.status_code == 200, (
            f"[Row 34] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 35  —  CSV blank SignalName value → row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow35CsvBlankSignalNameValue:
    """Excel Row 35: Rows with blank SignalName value are skipped; valid rows ingested."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good   = f"{unique_user_id}_GOOD"
        self.user_bad    = f"{unique_user_id}_BAD"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_good},{sig},40000.0,2024-01-15,TestData,kb,meta\n"
            f"{self.user_bad},,40000.0,2024-01-15,TestData,kb,meta",       # blank signal name
        )
        submissions["sig_row35"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": [self.user_bad]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow35_blank_signal_name_row_skipped_returns_200(self):
        """Row 35: Blank SignalName in a row → that row skipped → 200."""
        assert self.response.status_code == 200, (
            f"[Row 35] Expected 200 (blank SignalName row skipped), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 36  —  CSV empty/null SignalName value → row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow36CsvNullSignalNameValue:
    """Excel Row 36: Rows with 'null' SignalName value are skipped."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good    = f"{unique_user_id}_GOOD"
        self.user_null    = f"{unique_user_id}_NULL"
        self.signal_name  = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_good},{sig},40000.0,2024-01-15,TestData,kb,meta\n"
            f"{self.user_null},null,40000.0,2024-01-15,TestData,kb,meta",  # null signal name
        )
        submissions["sig_row36"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": [self.user_null]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow36_null_signal_name_row_skipped_returns_200(self):
        """Row 36: 'null' SignalName → that row skipped → 200."""
        assert self.response.status_code == 200, (
            f"[Row 36] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 37  —  CSV empty/null SignalValue is accepted and inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow37CsvEmptySignalValueInserted:
    """Excel Row 37: Empty/null SignalValue rows ARE inserted (unlike SignalName)."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_empty  = f"{unique_user_id}_EMPTY"
        self.user_null   = f"{unique_user_id}_NULL"
        self.user_good   = f"{unique_user_id}_GOOD"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_good},{sig},40000.0,2024-01-15,TestData,kb,meta\n"
            f"{self.user_empty},{sig},,2024-01-15,TestData,kb,meta\n"      # empty value
            f"{self.user_null},{sig},null,2024-01-15,TestData,kb,meta",    # null value
        )
        submissions["sig_row37"] = {
            "user_ids":    [self.user_good, self.user_empty, self.user_null],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow37_empty_signal_value_inserted_returns_200(self):
        """Row 37: Empty/null SignalValue rows are inserted (not skipped) → 200."""
        assert self.response.status_code == 200, (
            f"[Row 37] Expected 200 (empty SignalValue rows inserted), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 38  —  CSV bad/missing ResponseTime value → row skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow38CsvBadResponseTimeRowSkipped:
    """Excel Row 38: Rows with bad/empty ResponseTime are skipped; valid rows ingested."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good   = f"{unique_user_id}_GOOD"
        self.user_bad_rt = f"{unique_user_id}_BADRT"
        self.user_empty  = f"{unique_user_id}_EMPTY"
        self.user_null   = f"{unique_user_id}_NULL"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_good},{sig},40000.0,2024-01-15,TestData,kb,meta\n"
            f"{self.user_bad_rt},{sig},40000.0,abc,TestData,kb,meta\n"     # not a date
            f"{self.user_empty},{sig},40000.0,,TestData,kb,meta\n"         # empty date
            f"{self.user_null},{sig},40000.0,null,TestData,kb,meta",       # null date
        )
        submissions["sig_row38"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": [self.user_bad_rt, self.user_empty, self.user_null]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow38_bad_response_time_rows_skipped_returns_200(self):
        """Row 38: Bad/empty ResponseTime rows skipped → 200."""
        assert self.response.status_code == 200, (
            f"[Row 38] Expected 200 (bad ResponseTime rows skipped), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 39  —  CSV optional fields (ResponseGroup, platform, metadata)
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow39CsvOptionalFields:
    """Excel Row 39: All optional fields (ResponseGroup, platform, metadata) accepted → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15,AutoTestGroup,kb,AutoMeta",
        )
        submissions["sig_row39"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra": {
                "expected_columns": {
                    "response_group":        "AutoTestGroup",
                    "platform":              "kb",
                    "signal_meta_data":      "AutoMeta",
                },
            },
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow39_optional_fields_accepted_returns_200(self):
        """Row 39: CSV with all optional fields → 200."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "csv")
        assert body, "[Row 39] Response body empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 40  —  CSV missing optional field values → stored as NULL
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow40CsvNullOptionalValues:
    """Excel Row 40: Rows with empty optional field values → those columns stored as NULL."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_no_grp  = f"{unique_user_id}_NOGRP"
        self.user_no_plat = f"{unique_user_id}_NOPLAT"
        self.user_no_meta = f"{unique_user_id}_NOMETA"
        self.signal_name  = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_no_grp},{sig},40000,2024-01-15,,kb,meta1\n"
            f"{self.user_no_plat},{sig},40000,2024-01-15,GrpData,,meta2\n"
            f"{self.user_no_meta},{sig},40000,2024-01-15,GrpData,kb,",
        )
        submissions["sig_row40"] = {
            "user_ids":    [self.user_no_grp, self.user_no_plat, self.user_no_meta],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow40_null_optional_values_returns_200(self):
        """Row 40: Empty optional values → rows inserted → 200."""
        assert self.response.status_code == 200, (
            f"[Row 40] Expected 200 (null optional values), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 41  —  CSV no duplicate rows in mapping/signal tables
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow41CsvNoDuplicateMapping:
    """Excel Row 41: Same client_user_id sent twice with different signal data —
    client_user_mapping should have only one entry per user."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        uid = self.client_user_id
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{uid},{sig},40000,2024-01-15,Data1,kb1,meta1\n"
            f"{uid},{sig},99999,2024-01-16,Data2,kb2,meta2",               # same user, diff values
        )
        submissions["sig_row41"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow41_no_duplicate_mapping_returns_200(self):
        """Row 41: Same user twice → 200; DB phase verifies mapping has exactly one entry."""
        assert self.response.status_code == 200, (
            f"[Row 41] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ── SKIP : Row 42  (> 1 GB file test — manual only) ──────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 43  —  CSV signal_value_numeric + signal_value_currency computed
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow43CsvComputedNumericCurrency:
    """Excel Row 43: Numeric SignalValue → signal_value_numeric and signal_value_currency filled."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.client_user_id},{self.signal_name},40000,2024-01-15,Data,kb,meta",
        )
        submissions["sig_row43"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"expected_numeric": 40000, "check_currency": True},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow43_numeric_signal_returns_200(self):
        """Row 43: Numeric SignalValue → 200; DB phase checks signal_value_numeric & signal_value_currency."""
        assert self.response.status_code == 200, (
            f"[Row 43] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 44  —  CSV signal_value_date computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow44CsvComputedSignalValueDate:
    """Excel Row 44: Date-parseable SignalValue → signal_value_date column populated."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_tz    = f"{unique_user_id}_TZ"
        self.user_plain = f"{unique_user_id}_PLAIN"
        self.user_date  = f"{unique_user_id}_DATE"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.user_tz},{sig},2024-02-03T18:30:30-05:00,2024-01-15,Data,kb,meta\n"
            f"{self.user_plain},{sig},2024-02-03T18:30:30,2024-01-15,Data,kb,meta\n"
            f"{self.user_date},{sig},2024-02-03,2024-01-15,Data,kb,meta",
        )
        submissions["sig_row44"] = {
            "user_ids":    [self.user_tz, self.user_plain, self.user_date],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow44_date_signal_value_returns_200(self):
        """Row 44: Date-parseable SignalValue → 200; DB phase checks signal_value_date."""
        assert self.response.status_code == 200, (
            f"[Row 44] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 45  —  CSV signal_value_date_duration computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow45CsvComputedDateDuration:
    """Excel Row 45: Date SignalValue → signal_value_date_duration (days since date) populated."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n"
            f"{self.client_user_id},{self.signal_name},2024-02-03,2024-01-15,Data,kb,meta",
        )
        submissions["sig_row45"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow45_date_duration_field_returns_200(self):
        """Row 45: Date SignalValue → 200; DB phase checks signal_value_date_duration is not NULL."""
        assert self.response.status_code == 200, (
            f"[Row 45] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 46  —  CSV signal_value_bool computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow46CsvComputedBoolField:
    """Excel Row 46: Boolean-like SignalValues (0,1,True,False,true,false) → signal_value_bool."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        sig = unique_signal_name
        uid = unique_user_id
        self.users = {
            "zero":    f"{uid}_0",
            "one":     f"{uid}_1",
            "true_u":  f"{uid}_TU",
            "false_u": f"{uid}_FU",
            "true_l":  f"{uid}_TL",
            "false_l": f"{uid}_FL",
        }
        self.signal_name = sig
        rows = "\n".join([
            f"{self.users['zero']},{sig},0,2024-01-15,Data,kb,meta",
            f"{self.users['one']},{sig},1,2024-01-15,Data,kb,meta",
            f"{self.users['true_u']},{sig},True,2024-01-15,Data,kb,meta",
            f"{self.users['false_u']},{sig},False,2024-01-15,Data,kb,meta",
            f"{self.users['true_l']},{sig},true,2024-01-15,Data,kb,meta",
            f"{self.users['false_l']},{sig},false,2024-01-15,Data,kb,meta",
        ])
        self.response = api_client.post_csv(
            SCHEMA,
            f"ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup,platform,metadata\n{rows}",
        )
        submissions["sig_row46"] = {
            "user_ids":    list(self.users.values()),
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow46_bool_signal_values_returns_200(self):
        """Row 46: Boolean-like SignalValues → 200; DB phase checks signal_value_bool."""
        assert self.response.status_code == 200, (
            f"[Row 46] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 47  —  JSON ingestion into all required tables
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow47JsonAllTablesIngested:
    """Excel Row 47: JSON upload → API 200; DB phase verifies all 3 tables."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, [{
            "ClientUserId": self.client_user_id,
            "SignalName":   self.signal_name,
            "SignalValue":  40000,
            "ResponseTime": "2024-01-15",
        }])
        submissions["sig_row47"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"check_tables": ["client_users_data", "client_user_mapping", "raw_signals"]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow47_json_all_tables_returns_200(self):
        """Row 47: JSON array upload returns 200."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body, "[Row 47] Response body empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 48  —  JSON responseTime ISO-8601 variants
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow48JsonResponseTimeFormats:
    """Excel Row 48: JSON ResponseTime accepts ISO-8601 with/without timezone."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_plain = f"{unique_user_id}_PLAIN"
        self.user_utc   = f"{unique_user_id}_UTC"
        self.user_tz    = f"{unique_user_id}_TZ"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_plain, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-22T18:30:30",  "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_utc,   "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-22T13:30:30Z", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_tz,    "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-22T18:30:30-05:00", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row48"] = {
            "user_ids":    [self.user_plain, self.user_utc, self.user_tz],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow48_json_iso8601_variants_accepted(self):
        """Row 48: Multiple ISO-8601 ResponseTime formats accepted → 200."""
        assert self.response.status_code == 200, (
            f"[Row 48] Expected 200 for ISO-8601 variants in JSON, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 49  —  JSON correct columns ingested into client_users_data
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow49JsonCorrectColumnsIngested:
    """Excel Row 49: JSON upload stores all mapped columns correctly."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, [{
            "ClientUserId":  self.client_user_id,
            "SignalName":    self.signal_name,
            "SignalValue":   40000,
            "ResponseTime":  "2024-01-15",
            "ResponseGroup": "AutoTestGroup",
            "platform":      "kb",
            "metadata":      "AutoMeta",
        }])
        submissions["sig_row49"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra": {
                "expected_columns": {
                    "signal_name":    self.signal_name,
                    "response_group": "AutoTestGroup",
                    "platform":       "kb",
                },
            },
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow49_json_correct_columns_returns_200(self):
        """Row 49: JSON with all columns → 200; DB phase checks column values."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body, "[Row 49] Response body empty"


# ── SKIP : Row 50  (projectName JSON — known bug, Azure ticket #5705) ────────


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 51  —  JSON space trimming on all fields
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow51JsonSpaceTrimming:
    """Excel Row 51: API trims leading/trailing spaces from JSON string fields."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id        = unique_user_id
        self.client_user_id_padded = f"  {unique_user_id}  "
        self.signal_name           = unique_signal_name
        self.response = api_client.post_json(SCHEMA, {
            "ClientUserId":  self.client_user_id_padded,
            "SignalName":    f"  {self.signal_name}  ",
            "SignalValue":   40000,
            "ResponseTime":  "2024-01-15",
            "ResponseGroup": "  AutoTestGroup  ",
            "platform":      "  kb  ",
        })
        submissions["sig_row51"] = {
            "user_ids":    [self.client_user_id],   # trimmed version
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow51_json_space_trimming_returns_200(self):
        """Row 51: JSON with padded string fields → 200; DB phase checks trimmed values."""
        assert self.response.status_code == 200, (
            f"[Row 51] Expected 200 for space-padded JSON, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 52  —  JSON case-insensitive deduplication
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow52JsonCaseInsensitiveDedup:
    """Excel Row 52: JSON array with three case-variant duplicates → stored once."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        uid = self.client_user_id
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": uid,          "SignalName": sig,          "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": uid.lower(),  "SignalName": sig.lower(),  "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": uid.upper(),  "SignalName": sig.upper(),  "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row52"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow52_json_case_insensitive_dedup_returns_200(self):
        """Row 52: Case-variant JSON duplicates → 200; DB phase verifies single row."""
        assert self.response.status_code == 200, (
            f"[Row 52] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 53  —  JSON no duplicate data inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow53JsonNoDuplicate:
    """Excel Row 53: Two JSON objects same (ClientUserId, SignalName, SignalValue) → deduped."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        uid = self.client_user_id
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": uid, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-03-02", "ResponseGroup": "Test1Data", "platform": "kb1"},
            {"ClientUserId": uid, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-04-02", "ResponseGroup": "Test2Data", "platform": "kb2"},
        ])
        submissions["sig_row53"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow53_json_no_duplicate_returns_200(self):
        """Row 53: Duplicate JSON objects → 200; DB phase verifies ≤1 row in client_users_data."""
        assert self.response.status_code == 200, (
            f"[Row 53] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 54  —  JSON minimum mandatory fields only
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow54JsonMinimumFields:
    """Excel Row 54: JSON with only the 4 mandatory fields → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, [{
            "ClientUserId": self.client_user_id,
            "SignalName":   self.signal_name,
            "SignalValue":  40000,
            "ResponseTime": "2024-01-15",
        }])
        submissions["sig_row54"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow54_json_minimum_fields_returns_200(self):
        """Row 54: JSON with minimum mandatory fields → 200."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body, "[Row 54] Response body empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 55  —  JSON missing ClientUserId key → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow55JsonMissingClientUserIdKey:
    """Excel Row 55: JSON object with no ClientUserId key → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_signal_name, submissions):
        self.signal_name = unique_signal_name
        self.response = api_client.post_json(SCHEMA, {
            "SignalName":   self.signal_name,
            "SignalValue":  40000,
            "ResponseTime": "2024-01-15",
        })
        submissions["sig_row55"] = {
            "user_ids":   [],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow55_json_missing_client_user_id_returns_400(self):
        """Row 55: Missing ClientUserId in JSON → 400."""
        assert self.response.status_code == 400, (
            f"[Row 55] Expected 400 for missing ClientUserId, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 56  —  JSON missing SignalName key → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow56JsonMissingSignalNameKey:
    """Excel Row 56: JSON object with no SignalName key → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, submissions):
        self.client_user_id = unique_user_id
        self.response = api_client.post_json(SCHEMA, {
            "ClientUserId": self.client_user_id,
            "SignalValue":  40000,
            "ResponseTime": "2024-01-15",
        })
        submissions["sig_row56"] = {
            "user_ids":   [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow56_json_missing_signal_name_returns_400(self):
        """Row 56: Missing SignalName in JSON → 400."""
        assert self.response.status_code == 400, (
            f"[Row 56] Expected 400 for missing SignalName, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 57  —  JSON missing SignalValue key → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow57JsonMissingSignalValueKey:
    """Excel Row 57: JSON object with no SignalValue key → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, {
            "ClientUserId": self.client_user_id,
            "SignalName":   self.signal_name,
            "ResponseTime": "2024-01-15",
        })
        submissions["sig_row57"] = {
            "user_ids":   [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow57_json_missing_signal_value_returns_400(self):
        """Row 57: Missing SignalValue in JSON → 400."""
        assert self.response.status_code == 400, (
            f"[Row 57] Expected 400 for missing SignalValue, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 58  —  JSON missing ResponseTime key → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow58JsonMissingResponseTimeKey:
    """Excel Row 58: JSON object with no ResponseTime key → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, {
            "ClientUserId": self.client_user_id,
            "SignalName":   self.signal_name,
            "SignalValue":  40000,
        })
        submissions["sig_row58"] = {
            "user_ids":   [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow58_json_missing_response_time_returns_400(self):
        """Row 58: Missing ResponseTime in JSON → 400."""
        assert self.response.status_code == 400, (
            f"[Row 58] Expected 400 for missing ResponseTime, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 59  —  JSON multiple mandatory keys missing → 400
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow59JsonMultipleMandatoryKeysMissing:
    """Excel Row 59: JSON missing both SignalName and SignalValue → 400."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, submissions):
        self.client_user_id = unique_user_id
        self.response = api_client.post_json(SCHEMA, {
            "ClientUserId": self.client_user_id,
            "ResponseTime": "2024-01-15",
            # SignalName and SignalValue both missing
        })
        submissions["sig_row59"] = {
            "user_ids":   [self.client_user_id],
            "api_status": self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow59_json_multiple_mandatory_missing_returns_400(self):
        """Row 59: Multiple mandatory keys missing → 400."""
        assert self.response.status_code == 400, (
            f"[Row 59] Expected 400 for multiple missing mandatory fields, "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 60  —  JSON misspelled/extra optional fields ignored → 200
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow60JsonMisspelledOptionalFields:
    """Excel Row 60: Misspelled optional JSON keys are silently ignored → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, {
            "ClientUserId":   self.client_user_id,
            "SignalName":     self.signal_name,
            "SignalValue":    40000,
            "ResponseTime":   "2024-01-15",
            "RespGrp":        "Typo",         # misspelled ResponseGroup
            "platfrm":        "kb",           # misspelled platform
            "MetaDeta":       "meta",         # misspelled metadata
        })
        submissions["sig_row60"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow60_json_misspelled_optional_fields_ignored(self):
        """Row 60: Misspelled optional JSON keys should not cause rejection → 200."""
        assert self.response.status_code == 200, (
            f"[Row 60] Expected 200 (misspelled optional fields ignored), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )

# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 61  —  JSON array: element with missing ClientUserId key → skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow61JsonMissingClientUserIdInElement:
    """Excel Row 61: JSON array where one element has no ClientUserId key →
    that element is skipped; valid elements are ingested → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good1  = f"{unique_user_id}_G1"
        self.user_good2  = f"{unique_user_id}_G2"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_good1, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            # missing ClientUserId key entirely
            {"SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_good2, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row61"] = {
            "user_ids":    [self.user_good1, self.user_good2],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": []},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow61_element_with_missing_client_user_id_key_skipped_returns_200(self):
        """Row 61: JSON element with missing ClientUserId key skipped; others ingested → 200."""
        assert self.response.status_code == 200, (
            f"[Row 61] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 62  —  JSON array: element with empty or null ClientUserId → skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow62JsonEmptyNullClientUserIdInElement:
    """Excel Row 62: JSON array where elements have empty-string or null ClientUserId →
    those elements are skipped; valid elements are ingested → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good   = f"{unique_user_id}_GOOD"
        self.user_empty  = f"{unique_user_id}_EMPTY"   # will be absent from DB
        self.user_null   = f"{unique_user_id}_NULL"    # will be absent from DB
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_good, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": "",   "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": None, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row62"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": ["", None]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow62_empty_null_client_user_id_skipped_returns_200(self):
        """Row 62: Elements with empty/null ClientUserId skipped → 200."""
        assert self.response.status_code == 200, (
            f"[Row 62] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 63  —  JSON array: element with missing SignalName key → skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow63JsonMissingSignalNameInElement:
    """Excel Row 63: JSON array where one element has no SignalName key →
    that element is skipped; valid elements are ingested → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good = f"{unique_user_id}_GOOD"
        self.user_bad  = f"{unique_user_id}_BAD"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_good, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            # missing SignalName key entirely
            {"ClientUserId": self.user_bad, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row63"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": [self.user_bad]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow63_element_with_missing_signal_name_key_skipped_returns_200(self):
        """Row 63: JSON element with missing SignalName key skipped; others ingested → 200."""
        assert self.response.status_code == 200, (
            f"[Row 63] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 64  —  JSON array: element with empty or null SignalName → skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow64JsonEmptyNullSignalNameInElement:
    """Excel Row 64: JSON array where elements have empty-string or null SignalName →
    those elements are skipped; valid elements are ingested → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good   = f"{unique_user_id}_GOOD"
        self.user_empty  = f"{unique_user_id}_EMPTY"
        self.user_null   = f"{unique_user_id}_NULL"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_good,  "SignalName": sig,  "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_empty, "SignalName": "",   "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_null,  "SignalName": None, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row64"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": [self.user_empty, self.user_null]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow64_empty_null_signal_name_skipped_returns_200(self):
        """Row 64: Elements with empty/null SignalName skipped → 200."""
        assert self.response.status_code == 200, (
            f"[Row 64] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 65  —  JSON array: element with missing/empty/null SignalValue → inserted
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow65JsonEmptyNullSignalValueInserted:
    """Excel Row 65: JSON elements with missing, empty, or null SignalValue are
    inserted (not skipped) — same behaviour as CSV Row 37 → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good    = f"{unique_user_id}_GOOD"
        self.user_missing = f"{unique_user_id}_MISS"
        self.user_empty   = f"{unique_user_id}_EMPTY"
        self.user_null    = f"{unique_user_id}_NULL"
        self.signal_name  = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_good,    "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            # missing SignalValue key
            {"ClientUserId": self.user_missing, "SignalName": sig,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_empty,   "SignalName": sig, "SignalValue": "",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_null,    "SignalName": sig, "SignalValue": None,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row65"] = {
            "user_ids":    [self.user_good, self.user_missing, self.user_empty, self.user_null],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow65_empty_null_signal_value_inserted_returns_200(self):
        """Row 65: Elements with missing/empty/null SignalValue are inserted (not skipped) → 200."""
        assert self.response.status_code == 200, (
            f"[Row 65] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 66  —  JSON array: element with bad/missing ResponseTime → skipped
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow66JsonBadResponseTimeRowSkipped:
    """Excel Row 66: JSON elements with missing, empty, null, or non-date ResponseTime
    are skipped; valid elements are ingested → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_good    = f"{unique_user_id}_GOOD"
        self.user_missing = f"{unique_user_id}_MISS"
        self.user_empty   = f"{unique_user_id}_EMPTY"
        self.user_null    = f"{unique_user_id}_NULL"
        self.user_bad     = f"{unique_user_id}_BAD"
        self.signal_name  = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_good,    "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            # missing ResponseTime key entirely
            {"ClientUserId": self.user_missing, "SignalName": sig, "SignalValue": 40000,
             "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_empty,   "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_null,    "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": None, "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_bad,     "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "not-a-date", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row66"] = {
            "user_ids":    [self.user_good],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"absent_user_ids": [
                self.user_missing, self.user_empty, self.user_null, self.user_bad,
            ]},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow66_bad_response_time_elements_skipped_returns_200(self):
        """Row 66: JSON elements with bad/missing ResponseTime skipped → 200."""
        assert self.response.status_code == 200, (
            f"[Row 66] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 67  —  JSON optional fields stored correctly
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow67JsonOptionalFields:
    """Excel Row 67: All JSON optional fields (ResponseGroup, platform, metadata)
    accepted and stored correctly → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, [{
            "ClientUserId":  self.client_user_id,
            "SignalName":    self.signal_name,
            "SignalValue":   40000,
            "ResponseTime":  "2024-01-15",
            "ResponseGroup": "AutoTestGroup",
            "platform":      "kb",
            "metadata":      "AutoMeta",
        }])
        submissions["sig_row67"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra": {
                "expected_columns": {
                    "response_group":   "AutoTestGroup",
                    "platform":         "kb",
                    "signal_meta_data": "AutoMeta",
                },
            },
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow67_json_optional_fields_accepted_returns_200(self):
        """Row 67: JSON with all optional fields → 200."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body, "[Row 67] Response body empty"


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 68  —  JSON missing optional field values → stored as NULL
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow68JsonNullOptionalValues:
    """Excel Row 68: JSON elements with empty/null optional field values →
    those columns stored as NULL in client_users_data → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_no_grp  = f"{unique_user_id}_NOGRP"
        self.user_no_plat = f"{unique_user_id}_NOPLAT"
        self.user_no_meta = f"{unique_user_id}_NOMETA"
        self.signal_name  = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_no_grp,  "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": None, "platform": "kb", "metadata": "meta1"},
            {"ClientUserId": self.user_no_plat, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "GrpData", "platform": None, "metadata": "meta2"},
            {"ClientUserId": self.user_no_meta, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "GrpData", "platform": "kb", "metadata": None},
        ])
        submissions["sig_row68"] = {
            "user_ids":    [self.user_no_grp, self.user_no_plat, self.user_no_meta],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow68_json_null_optional_values_returns_200(self):
        """Row 68: JSON with null optional field values → rows inserted → 200."""
        assert self.response.status_code == 200, (
            f"[Row 68] Expected 200 (null optional values), "
            f"got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 69  —  JSON no duplicate rows in client_user_mapping
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow69JsonNoDuplicateMapping:
    """Excel Row 69: Same client_user_id sent twice in separate JSON objects →
    client_user_mapping should have only one entry for that user → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        uid = self.client_user_id
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": uid, "SignalName": sig, "SignalValue": 40000,
             "ResponseTime": "2024-01-15", "ResponseGroup": "Data1", "platform": "kb1"},
            {"ClientUserId": uid, "SignalName": sig, "SignalValue": 99999,
             "ResponseTime": "2024-01-16", "ResponseGroup": "Data2", "platform": "kb2"},
        ])
        submissions["sig_row69"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow69_json_no_duplicate_mapping_returns_200(self):
        """Row 69: Same user sent twice in JSON → 200; DB phase verifies one mapping entry."""
        assert self.response.status_code == 200, (
            f"[Row 69] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ── SKIP : Row 70  (> 1 GB JSON file — manual only) ──────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 71  —  JSON signal_value_numeric + signal_value_currency computed
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow71JsonComputedNumericCurrency:
    """Excel Row 71: JSON with numeric SignalValue →
    signal_value_numeric and signal_value_currency populated → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, [{
            "ClientUserId":  self.client_user_id,
            "SignalName":    self.signal_name,
            "SignalValue":   40000,
            "ResponseTime":  "2024-01-15",
            "ResponseGroup": "TestData",
            "platform":      "kb",
        }])
        submissions["sig_row71"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
            "extra":       {"expected_numeric": 40000, "check_currency": True},
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow71_json_numeric_signal_returns_200(self):
        """Row 71: JSON numeric SignalValue → 200; DB phase checks signal_value_numeric & currency."""
        assert self.response.status_code == 200, (
            f"[Row 71] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 72  —  JSON signal_value_date computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow72JsonComputedSignalValueDate:
    """Excel Row 72: JSON with date-parseable SignalValue →
    signal_value_date column populated → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.user_tz    = f"{unique_user_id}_TZ"
        self.user_plain = f"{unique_user_id}_PLAIN"
        self.user_date  = f"{unique_user_id}_DATE"
        self.signal_name = unique_signal_name
        sig = self.signal_name
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.user_tz,    "SignalName": sig,
             "SignalValue": "2024-02-03T18:30:30-05:00", "ResponseTime": "2024-01-15",
             "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_plain, "SignalName": sig,
             "SignalValue": "2024-02-03T18:30:30",       "ResponseTime": "2024-01-15",
             "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.user_date,  "SignalName": sig,
             "SignalValue": "2024-02-03",                "ResponseTime": "2024-01-15",
             "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row72"] = {
            "user_ids":    [self.user_tz, self.user_plain, self.user_date],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow72_json_date_signal_value_returns_200(self):
        """Row 72: JSON date-parseable SignalValue → 200; DB phase checks signal_value_date."""
        assert self.response.status_code == 200, (
            f"[Row 72] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 73  —  JSON signal_value_date_duration computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow73JsonComputedDateDuration:
    """Excel Row 73: JSON with date-parseable SignalValue →
    signal_value_date_duration (days since date) populated → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name
        self.response = api_client.post_json(SCHEMA, [{
            "ClientUserId":  self.client_user_id,
            "SignalName":    self.signal_name,
            "SignalValue":   "2024-02-03",
            "ResponseTime":  "2024-01-15",
            "ResponseGroup": "TestData",
            "platform":      "kb",
        }])
        submissions["sig_row73"] = {
            "user_ids":    [self.client_user_id],
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow73_json_date_duration_field_returns_200(self):
        """Row 73: JSON date SignalValue → 200; DB phase checks signal_value_date_duration not NULL."""
        assert self.response.status_code == 200, (
            f"[Row 73] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL ROW 74  —  JSON signal_value_bool computed field
# ══════════════════════════════════════════════════════════════════════════════

class TestSigRow74JsonComputedBoolField:
    """Excel Row 74: JSON with boolean-like SignalValues (0,1,2,-1,True,False,true,false,TRUE,FALSE)
    → signal_value_bool populated for valid boolean values → 200."""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, submissions):
        sig = unique_signal_name
        uid = unique_user_id
        self.users = {
            "zero":     f"{uid}_0",
            "one":      f"{uid}_1",
            "two":      f"{uid}_2",
            "neg_one":  f"{uid}_N1",
            "true_u":   f"{uid}_TU",
            "false_u":  f"{uid}_FU",
            "true_l":   f"{uid}_TL",
            "false_l":  f"{uid}_FL",
            "true_uu":  f"{uid}_TUU",
            "false_uu": f"{uid}_FUU",
        }
        self.signal_name = sig
        self.response = api_client.post_json(SCHEMA, [
            {"ClientUserId": self.users["zero"],     "SignalName": sig, "SignalValue": "0",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["one"],      "SignalName": sig, "SignalValue": "1",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["two"],      "SignalName": sig, "SignalValue": "2",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["neg_one"],  "SignalName": sig, "SignalValue": "-1",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["true_u"],   "SignalName": sig, "SignalValue": "True",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["false_u"],  "SignalName": sig, "SignalValue": "False",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["true_l"],   "SignalName": sig, "SignalValue": "true",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["false_l"],  "SignalName": sig, "SignalValue": "false",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["true_uu"],  "SignalName": sig, "SignalValue": "TRUE",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
            {"ClientUserId": self.users["false_uu"], "SignalName": sig, "SignalValue": "FALSE",
             "ResponseTime": "2024-01-15", "ResponseGroup": "TestData", "platform": "kb"},
        ])
        submissions["sig_row74"] = {
            "user_ids":    list(self.users.values()),
            "signal_name": self.signal_name,
            "api_status":  self.response.status_code,
        }

    @pytest.mark.signals
    @pytest.mark.api
    def test_sigrow74_json_bool_signal_values_returns_200(self):
        """Row 74: JSON boolean-like SignalValues → 200; DB phase checks signal_value_bool."""
        assert self.response.status_code == 200, (
            f"[Row 74] Expected 200, got {self.response.status_code}. Body: {self.response.text}"
        )