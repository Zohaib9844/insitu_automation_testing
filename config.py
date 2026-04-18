# ─────────────────────────────────────────────
#  InSitu QA Automation – Central Config
# ─────────────────────────────────────────────
import os

# ── API ──────────────────────────────────────
API_BASE_URL = "http://sakerqa.southeastasia.cloudapp.azure.com/datareceiver/api/Data"
API_KEY      = "5c4b2ef056a7f4d9eeff2c39eb3f1efbb40982611e1ee95e86bf398c61c3f35d"

API_HEADERS_JSON = {
    "api-key":      API_KEY,
    "Content-Type": "application/json",
}
API_HEADERS_CSV = {
    "api-key":      API_KEY,
    "Content-Type": "text/plain",
}

API_TIMEOUT = 30  # seconds

# ── Database ──────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "da_insitu_db",
    "user":     "postgres",
    "password": "bazooka-1",
}

# How many seconds to wait for async DB writes to complete.
# Override via env var for CI: DB_PROPAGATION_DELAY=30 pytest
# Previous runs showed the slowest write taking ~14-16s end-to-end
# (including API call time). 25s gives comfortable headroom.
DB_PROPAGATION_DELAY = int(os.getenv("DB_PROPAGATION_DELAY", "25"))

# ── Test data prefixes (keep unique per run) ──
TEST_USER_PREFIX    = "AT_USER"     # AutoTest user prefix
TEST_SIGNAL_PREFIX  = "AT_SIG"
TEST_PROP_PREFIX    = "AT_PROP"