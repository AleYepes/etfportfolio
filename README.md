# ETF Factor-Series Pipeline

ETF factor analysis pipeline to calculate efficient frontier portfolios.

## Setup

```bash
uv sync
uv run playwright install chromium
```

## Usage

### 1. Run Ingestion Pipeline
Executes the full pipeline (Product Discovery → Session Probe → Theme Taxonomy Sync → Per-product Concurrent Fetch):

```bash
# Ingest all phases
uv run python main.py ingest

# Ingest individual phases
uv run python main.py ingest session
uv run python main.py ingest products
uv run python main.py ingest themes
uv run python main.py ingest details

# Ingest a limited number of products
uv run python main.py ingest --limit 10

# Ingest specific product IDs (comma-separated or path to a file)
uv run python main.py ingest --product-ids "756733,8335"
uv run python main.py ingest --product-ids docs/sample_product_ids.txt
```

## Development & Testing

```bash
uv run pytest
uv run ruff check -- fix
uv run ruff format
```