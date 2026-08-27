# ETF Factor-Series Pipeline

Database and ingestion pipeline fetching ETF & fund data from Interactive Brokers (IBKR) into a DuckDB bronze medallion layer.

---

## Setup

Requires Python `>=3.14` and [`uv`](https://github.com/astral-sh/uv).

```bash
uv sync
uv run playwright install chromium
```

---

## Usage

### 1. Authenticate (Playwright Login)
Opens a browser window to complete manual login and 2FA. Captures cookies into `data/session_state.json`:

```bash
uv run python main.py auth login
```

### 2. Product Discovery (Standalone)
Crawls the IBKR universe for ETFs and Funds and upserts them into `bronze.products`:

```bash
uv run python main.py products sync
```

### 3. Run Ingestion Pipeline
Executes the full pipeline (Product Discovery → Session Probe → Theme Taxonomy Sync → Per-product Concurrent Fetch):

```bash
# Ingest all discovered products
uv run python main.py ingest run

# Ingest a limited number of products
uv run python main.py ingest run --limit 10

# Ingest specific product IDs (comma-separated or path to a file)
uv run python main.py ingest run --product-ids "756733,8335"
uv run python main.py ingest run --product-ids docs/sample_product_ids.txt
```

---

## Development & Testing

```bash
# Run test suite
uv run pytest

# Lint and format checks
uv run ruff check .
uv run ruff format --check .
```