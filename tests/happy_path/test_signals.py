"""
test_signals.py
───────────────
Happy-path tests for the DataReceiver API with schema=signals.

Test matrix
───────────
  TC-SIG-01  signals / CSV   → API returns 200 with correct schema/format/filePath
  TC-SIG-02  signals / CSV   → DB row present in profiles.client_users_data
  TC-SIG-03  signals / CSV   → DB row present in profiles.client_user_mapping
  TC-SIG-04  signals / JSON  (single object) → API returns 200
  TC-SIG-05  signals / JSON  (single object) → DB row present
  TC-SIG-06  signals / JSON  (array)         → API returns 200
  TC-SIG-07  signals / JSON  (array)         → Both DB rows present
"""
import pytest

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from utils import api_client, db_client

SCHEMA = "signals"


# ── CSV happy path ─────────────────────────────────────────────────────────────

class TestSignalsCSV:
    """TC-SIG-01 .. TC-SIG-03"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name):
        """POST the CSV payload once; store response + identifiers for assertions."""
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name

        csv_body = (
            "ClientUserId,SignalName,SignalValue,ResponseTime,ResponseGroup\n"
            f"{self.client_user_id},{self.signal_name},40000.0,2024-01-15,AutoTestGroup"
        )

        self.response = api_client.post_csv(SCHEMA, csv_body)

    # ── TC-SIG-01 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.signals
    def test_api_returns_200_with_correct_body(self):
        """TC-SIG-01: POST signals CSV → 200 OK with correct schema/format/filePath."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "csv")
        assert body  # non-empty dict guard

    # ── TC-SIG-02 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_db_client_users_data_has_record(self):
        """TC-SIG-02: Uploaded signal appears in profiles.client_users_data."""
        rows = db_client.wait_for_signal_in_db(self.client_user_id)
        assert rows, (
            f"No rows in profiles.client_users_data for client_user_id='{self.client_user_id}'"
        )
        match = [r for r in rows if r.get("signal_name", "").lower() == self.signal_name.lower()]
        assert match, (
            f"Signal '{self.signal_name}' not found in client_users_data rows: {rows}"
        )

    # ── TC-SIG-03 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_db_client_user_mapping_has_record(self):
        """TC-SIG-03: User mapped in profiles.client_user_mapping after CSV ingest.

        NOTE: get_client_user_mapping() already uses LOWER() on both sides, so
        case differences (API lowercases the stored ID) are not the issue.
        The mapping write is async just like client_users_data — we MUST poll.
        """
        rows = db_client.wait_for_user_mapping_in_db(self.client_user_id)
        assert rows, (
            f"No rows in profiles.client_user_mapping for client_user_id='{self.client_user_id}'\n"
            f"Waited {db_client.config.DB_PROPAGATION_DELAY}s. The API may lowercase the stored ID,\n"
            f"but LOWER() in the query already handles that. Root cause is likely async DB lag."
        )


# ── JSON (single object) happy path ───────────────────────────────────────────

class TestSignalsJSONSingle:
    """TC-SIG-04 .. TC-SIG-05"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name):
        self.client_user_id = unique_user_id
        self.signal_name    = unique_signal_name

        payload = {
            "ClientUserId":  self.client_user_id,
            "SignalName":    self.signal_name,
            "SignalValue":   "40000.0",
            "ResponseTime":  "2024-01-15T10:00:00Z",
            "ResponseGroup": "AutoTestGroup",
        }

        self.response = api_client.post_json(SCHEMA, payload)

    # ── TC-SIG-04 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.signals
    def test_api_returns_200_with_correct_body(self):
        """TC-SIG-04: POST signals JSON (single) → 200 OK with correct response body."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body

    # ── TC-SIG-05 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_db_client_users_data_has_record(self):
        """TC-SIG-05: Uploaded JSON signal appears in profiles.client_users_data."""
        rows = db_client.wait_for_signal_in_db(self.client_user_id)
        assert rows, (
            f"No rows in profiles.client_users_data for client_user_id='{self.client_user_id}'"
        )
        match = [r for r in rows if r.get("signal_name", "").lower() == self.signal_name.lower()]
        assert match, f"Signal '{self.signal_name}' not found in: {rows}"


# ── JSON (array) happy path ───────────────────────────────────────────────────

class TestSignalsJSONArray:
    """TC-SIG-06 .. TC-SIG-07"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_signal_name, run_id):
        self.user_id_1   = f"{unique_user_id}_A"
        self.user_id_2   = f"{unique_user_id}_B"
        self.signal_name = unique_signal_name

        payload = [
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
        ]

        self.response = api_client.post_json(SCHEMA, payload)

    # ── TC-SIG-06 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.signals
    def test_api_returns_200_with_correct_body(self):
        """TC-SIG-06: POST signals JSON (array) → 200 OK with correct response body."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body

    # ── TC-SIG-07 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.signals
    @pytest.mark.db
    def test_db_both_array_users_have_records(self):
        """TC-SIG-07: Both users from JSON array appear in profiles.client_users_data."""
        rows_1 = db_client.wait_for_signal_in_db(self.user_id_1)
        rows_2 = db_client.wait_for_signal_in_db(self.user_id_2)

        assert rows_1, f"No DB rows for user '{self.user_id_1}'"
        assert rows_2, f"No DB rows for user '{self.user_id_2}'"