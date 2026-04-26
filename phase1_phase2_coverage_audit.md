# Phase 1 / Phase 2 coverage audit (signals + userproperties)

## What exists in this repo

- **Signals, Phase 1 (API):** `tests/test_signals_phase1_api.py`
  - Implemented rows: **5, 6, 7, 13, 14, 16, 17, 19, 20, 21, 23-30, 32-41, 43-49, 51-69, 71-74**.
  - Explicit skips in file comments: **18, 22, 31, 42, 50, 70**.
- **Signals, Phase 2 (DB):** `tests/test_signals_phase2_db.py`
  - DB checks exist for the exact same implemented row keys from Phase 1.
- **Userproperties, Phase 1 (API):** `tests/test_phase1_api.py`
  - Happy path + regression rows **76-119** are implemented.
- **Userproperties, Phase 2 (DB):** `tests/test_phase2_db.py`
  - DB checks exist for happy-path submissions and rows **76-119**.

## Requested-list reconciliation

### Covered (implemented in Phase 1 API and has corresponding Phase 2 DB test)

- Signal happy path API checks for CSV / JSON single / JSON array.
- Signal ingestion correctness checks (required tables, responseTime handling, optional fields, trimming, dedup, min-fields, row-level skip behavior, computed columns).
- Signal error checks for missing mandatory fields and bad format/body-type combinations.
- Userproperties happy path API checks for CSV / JSON single / JSON array.
- Userproperties regression rows 76-119 (required-field errors, row-level skip behavior, dedup rules, type columns, type-conflict behavior, value-column coverage) and DB validations.

### Not automated / not covered in code (explicitly skipped or absent)

- **Signal configured-size-limit test(s)** (sheet row 18 is documented manual-only).
- **Signal `projectName` add-user flow** (rows 22 and 50 are documented as known bug / skipped).
- **Signal typo/column-name mistake row** (row 31 documented skipped / ambiguous expectation).
- **Signal file size >1GB tests** (rows 42 and 70 documented manual-only).

### Notes

- For "other than signal schema" items in your list, the repo uses **`userproperties`** as that schema family and covers these via `tests/test_phase1_api.py` + `tests/test_phase2_db.py`.
- Coverage mapping is driven by the row-key handoff pattern (`submissions[...]` in Phase 1 and `submissions.get(...)` in Phase 2).
