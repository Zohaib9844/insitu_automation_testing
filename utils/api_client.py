"""
api_client.py
─────────────
Thin wrapper around requests for the InSitu DataReceiver API.
"""
import requests
import config


def _params(schema: str, fmt: str) -> dict:
    return {"schema": schema, "format": fmt}


def post_json(schema: str, payload: dict | list) -> requests.Response:
    """POST JSON data to the DataReceiver API."""
    return requests.post(
        config.API_BASE_URL,
        params=_params(schema, "json"),
        headers=config.API_HEADERS_JSON,
        json=payload,
        timeout=config.API_TIMEOUT,
    )


def post_csv(schema: str, csv_text: str) -> requests.Response:
    """POST CSV (plain-text) data to the DataReceiver API."""
    return requests.post(
        config.API_BASE_URL,
        params=_params(schema, "csv"),
        headers=config.API_HEADERS_CSV,
        data=csv_text.encode("utf-8"),
        timeout=config.API_TIMEOUT,
    )


def assert_happy_response(response: requests.Response, schema: str, fmt: str) -> dict:
    """
    Validate a 200 happy-path response and return the parsed body.
    Raises AssertionError with a descriptive message on any failure.
    """
    assert response.status_code == 200, (
        f"Expected 200 OK, got {response.status_code}.\n"
        f"Response body: {response.text}"
    )

    body = response.json()

    assert "filePath" in body,   f"'filePath' missing from response: {body}"
    assert "schema"   in body,   f"'schema'   missing from response: {body}"
    assert "dataFormat" in body, f"'dataFormat' missing from response: {body}"

    assert body["schema"]     == schema, (
        f"schema mismatch: expected '{schema}', got '{body['schema']}'"
    )
    assert body["dataFormat"] == fmt, (
        f"dataFormat mismatch: expected '{fmt}', got '{body['dataFormat']}'"
    )

    # filePath must start with the schema folder and end with the right extension
    assert body["filePath"].startswith(f"{schema}/"), (
        f"filePath '{body['filePath']}' should start with '{schema}/'"
    )
    assert body["filePath"].endswith(f".{fmt}"), (
        f"filePath '{body['filePath']}' should end with '.{fmt}'"
    )

    return body
