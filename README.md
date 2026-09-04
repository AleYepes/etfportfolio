# ETF Factor-Series Pipeline

ETF factor analysis pipeline to calculate efficient frontier portfolios.

## Setup

### 1. Open and Setup an [IBKR](https://www.interactivebrokers.ie/en/home.php) account 

### 2. Set up the venv

```bash
uv sync
uv run playwright install chromium
```

## Usage (WIP)

### 1. Ingestion

#### Full Ingestion Run

Products → Contracts → Prices → Themes → Details

```bash
uv run python main.py ingest                                # Can take 24h+ the first time
```

#### Partial Ingestion Run

```bash
uv run python main.py ingest products                       # Unofficial IBKR product universe
uv run python main.py ingest contracts                      # Official IBKR product contracts
uv run python main.py ingest prices                         # Official contract price series
uv run python main.py ingest themes                         # Unofficial investment themes
uv run python main.py ingest details                        # Unofficial fundamental data
```

#### Optional Ingestion **Flags**

```bash
uv run python main.py ingest --limit 10
uv run python main.py ingest --product-ids "756733,8335"    # Comma-separated string
uv run python main.py ingest --force                        # Force refresh; bypass freshness windows
```

### 2. Panel creation for downstream analysis - *coming soon to a repo near you*

## Development & Testing

```bash
uv run pytest
uv run ruff check --fix
uv run ruff format
uv run pyright
```