# ETF Factor-Series Pipeline

ETF factor analysis pipeline to calculate efficient frontier portfolios.

## Setup

```bash
uv sync
uv run playwright install chromium
```

## Usage

### Ingestion Pipeline
Executes the full pipeline (Phase 1: Products → Phase 2: Contracts → Phase 3: Prices → Phase 4: Session → Phase 5: Themes → Phase 6: Details):

```bash
# Ingest all phases
uv run python main.py ingest

# Ingest individual phases
uv run python main.py ingest session
uv run python main.py ingest products
uv run python main.py ingest contracts
uv run python main.py ingest prices
uv run python main.py ingest themes
uv run python main.py ingest details

# Limit number of products
uv run python main.py ingest --limit 10

# Specific product IDs (comma-separated string or path to a text file)
uv run python main.py ingest --product-ids "756733,8335"
uv run python main.py ingest --product-ids docs/sample_product_ids.txt

# Force refresh (bypass freshness windows and landing gates)
uv run python main.py ingest --force
```

## Development & Testing

```bash
uv run pytest
uv run ruff check --fix
uv run ruff format
uv run pyright
```