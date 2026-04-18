"""
test_userproperties.py
──────────────────────
Happy-path tests for the DataReceiver API with schema=userproperties.

Test matrix
───────────
  TC-UP-01   userproperties / CSV   → API 200 with correct schema/format/filePath
  TC-UP-02   userproperties / CSV   → DB row present in profiles.user_properties
  TC-UP-03   userproperties / JSON  (single) → API 200
  TC-UP-04   userproperties / JSON  (single) → DB row present
  TC-UP-05   userproperties / JSON  (array)  → API 200
  TC-UP-06   userproperties / JSON  (array)  → Both DB rows present
"""
import pytest

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from utils import api_client, db_client

SCHEMA = "userproperties"


# ── CSV happy path ─────────────────────────────────────────────────────────────

class TestUserPropertiesCSV:
    """TC-UP-01 .. TC-UP-02"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id  = unique_user_id
        self.property_name   = unique_property_name

        csv_body = (
            "ClientUserId,PropertyName,PropertyValue\n"  # ✅
            f"{self.client_user_id},{self.property_name},AutoTestValue"
        )

        self.response = api_client.post_csv(SCHEMA, csv_body)

    # ── TC-UP-01 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.userprops
    def test_api_returns_200_with_correct_body(self):
        """TC-UP-01: POST userproperties CSV → 200 OK with correct response body."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "csv")
        assert body

    # ── TC-UP-02 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.db
    def test_db_user_properties_has_record(self):
        """TC-UP-02: Uploaded user property appears in profiles.user_properties."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        assert rows, (
            f"No rows in profiles.user_properties for client_user_id='{self.client_user_id}'"
        )
        match = [
            r for r in rows
            if r.get("property_name", "").lower() == self.property_name.lower()
        ]
        assert match, (
            f"Property '{self.property_name}' not found in user_properties rows: {rows}"
        )


# ── JSON (single object) happy path ───────────────────────────────────────────

class TestUserPropertiesJSONSingle:
    """TC-UP-03 .. TC-UP-04"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.client_user_id = unique_user_id
        self.property_name  = unique_property_name

        payload = {
            "ClientUserId":      self.client_user_id,
            "PropertyName":      self.property_name,
            "PropertyValue":     "AutoTestTextValue",
        }

        self.response = api_client.post_json(SCHEMA, payload)

    # ── TC-UP-03 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.userprops
    def test_api_returns_200_with_correct_body(self):
        """TC-UP-03: POST userproperties JSON (single) → 200 OK with correct response body."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body

    # ── TC-UP-04 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.db
    def test_db_user_properties_has_record(self):
        """TC-UP-04: Uploaded JSON user property appears in profiles.user_properties."""
        rows = db_client.wait_for_user_property_in_db(self.client_user_id)
        assert rows, (
            f"No rows in profiles.user_properties for client_user_id='{self.client_user_id}'"
        )
        match = [
            r for r in rows
            if r.get("property_name", "").lower() == self.property_name.lower()
        ]
        assert match, f"Property '{self.property_name}' not found in: {rows}"


# ── JSON (array) happy path ────────────────────────────────────────────────────

class TestUserPropertiesJSONArray:
    """TC-UP-05 .. TC-UP-06"""

    @pytest.fixture(autouse=True)
    def _send(self, unique_user_id, unique_property_name):
        self.user_id_1     = f"{unique_user_id}_A"
        self.user_id_2     = f"{unique_user_id}_B"
        self.property_name = unique_property_name

        payload = [
            {
                "ClientUserId":  self.user_id_1,
                "PropertyName":  self.property_name,
                "PropertyValue": "AutoTestValue1",
            },
            {
                "ClientUserId":  self.user_id_2,
                "PropertyName":  self.property_name,
                "PropertyValue": "AutoTestValue2",
            },
        ]

        self.response = api_client.post_json(SCHEMA, payload)

    # ── TC-UP-05 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.userprops
    def test_api_returns_200_with_correct_body(self):
        """TC-UP-05: POST userproperties JSON (array) → 200 OK with correct response body."""
        body = api_client.assert_happy_response(self.response, SCHEMA, "json")
        assert body

    # ── TC-UP-06 ──────────────────────────────────────────────────────────────
    @pytest.mark.happy_path
    @pytest.mark.userprops
    @pytest.mark.db
    def test_db_both_array_users_have_records(self):
        """TC-UP-06: Both users from JSON array appear in profiles.user_properties."""
        rows_1 = db_client.wait_for_user_property_in_db(self.user_id_1)
        rows_2 = db_client.wait_for_user_property_in_db(self.user_id_2)

        assert rows_1, f"No DB rows for user '{self.user_id_1}'"
        assert rows_2, f"No DB rows for user '{self.user_id_2}'"
