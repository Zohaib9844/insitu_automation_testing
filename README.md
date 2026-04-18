# InSitu QA Automation Framework

A pytest-based automation suite for the **DataReceiver API** — tests every
deployment end-to-end: API call → response validation → PostgreSQL DB verification → HTML report.

---

## Project Structure

```
insitu_qa/
├── config.py                     ← API URL, API key, DB credentials
├── conftest.py                   ← Shared fixtures (unique IDs per test run)
├── pytest.ini                    ← pytest settings + HTML report output
├── requirements.txt
├── utils/
│   ├── api_client.py             ← POST JSON / POST CSV helpers + response assertions
│   └── db_client.py              ← PostgreSQL query helpers with polling
└── tests/
    └── happy_path/
        ├── test_signals.py       ← 7 tests: signals CSV + JSON (single + array)
        └── test_userproperties.py← 6 tests: userproperties CSV + JSON (single + array)
```

---

## Quick Setup (Windows / macOS / Linux)

### 1 — Clone / copy the project folder

```bash
cd ~/projects          # or wherever you keep code
# (copy the insitu_qa folder here)
cd insitu_qa
```

### 2 — Create a virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### 4 — (Optional) verify DB connectivity before running

```python
python - <<'EOF'
import psycopg2, config
conn = psycopg2.connect(**config.DB_CONFIG)
print("DB connection OK:", conn.get_dsn_parameters())
conn.close()
EOF
```

---

## Running the Tests

### Run everything (all happy-path tests + HTML report)

```bash
pytest
```

Report saved to: `reports/report.html` — open it in any browser.

### Run only signals tests

```bash
pytest -m signals
```

### Run only userproperties tests

```bash
pytest -m userprops
```

### Run only API tests (skip DB checks)

```bash
pytest -m "not db"
```

### Run only DB checks

```bash
pytest -m db
```

### Run with live output (no capture)

```bash
pytest -s
```

---

## How Each Test Works

```
1. Generate a unique ClientUserId  (e.g. AT_USER_A3F21B04_1712345678)
   → guarantees we can find OUR record in the DB, not someone else's

2. POST payload to the API
   → CSV via Content-Type: text/plain
   → JSON via Content-Type: application/json

3. Assert API response
   → HTTP 200
   → JSON body contains filePath / schema / dataFormat fields
   → filePath starts with correct schema folder and ends with correct extension

4. Poll the PostgreSQL DB (up to DB_PROPAGATION_DELAY seconds)
   → signals    → profiles.client_users_data + profiles.client_user_mapping
   → userprops  → profiles.user_properties

5. Assert the expected row(s) exist with correct values
```

---

## Configuration

All settings are in `config.py`:

| Setting | Default | Description |
|---|---|---|
| `API_BASE_URL` | Azure endpoint | DataReceiver API URL |
| `API_KEY` | (set) | API key header value |
| `DB_CONFIG` | localhost:5433 | PostgreSQL connection |
| `DB_PROPAGATION_DELAY` | 5 s | Max wait for async DB writes |

---

## What's Coming Next (Phase 2)

- **Negative / regression tests** — wrong headers, missing fields, bad formats, 400 responses
- **Duplication checks** — same signal twice shouldn't duplicate rows
- **Field-level DB assertions** — verify every column value, not just row existence
- **Raw signals statistics** — validate `profiles.raw_signals` stats JSON
- **Abstract signals mapping** — verify `profiles.user_abstract_signals`
- **Intervention delivery tests** — `profiles.da_nudge_certainty`
