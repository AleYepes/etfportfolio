## CLI Logging & Terminal UX Guidelines for `etfportfolio`

**Core Rule:** `stdout` belongs to the user; `stderr` and log files belong to diagnostics.

### 1. Fundamental Principles

* **Dual-Channel Architecture:** Interactive prompts, progress indicators, and actionable results write to `stdout`. Diagnostic logs (timestamps, call stacks, external requests) write to `stderr` or a log file (`data/etfportfolio.log`).
* **Noise Suppression:** External dependencies (`httpx`, `urllib3`, `asyncio`, `playwright`) must be muted by default in interactive sessions.
* **Semantic Log Levels:** Use `logger.warning()` for state discrepancies, not raw string prefixes like `"[WARNING]"`. UI styling should be handled via a library like `rich` or standard ANSI tokens.

### 2. Output & Log Routing Matrix

| Message Type | Utility / Method | Terminal Target | Log File Target |
| --- | --- | --- | --- |
| **Interactive Prompts & Input** | `Rich.prompt` / `input()` | `stdout` | N/A |
| **User Status Updates** (e.g., "Login complete") | Console UI print / Spinner | `stdout` | `DEBUG` / `INFO` |
| **Application Diagnostics** (e.g., Session persisted) | `logger.info()` | Hidden (or `stderr` with `--verbose`) | `INFO` |
| **External HTTP Traces** | `httpx` internal logger | Hidden | `DEBUG` (or File) |
| **Warnings & Edge Cases** | `logger.warning()` + UI Prompt | `stdout` (Formatted) | `WARNING` |
| **Stack Traces & Crashes** | `logger.exception()` | `stderr` | `ERROR` |

### 3. Centralized Logging Configuration (`etfportfolio/core/logging.py`)

Implement a single initialization entry point called in `main.py` before executing any subcommands:

```python
import logging
import sys

NOISY_LOGGERS = ["httpx", "httpcore", "urllib3", "asyncio"]

def configure_logging(verbose: bool = False, log_file: str = "data/etfportfolio.log") -> None:
    # Set root logger level
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # File Handler - Full structured context for troubleshooting
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", 
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Console Handler (Diagnostics to stderr only when requested)
    if verbose:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(file_formatter)
        root_logger.addHandler(console_handler)

    # Clamp down third-party libraries
    for logger_name in NOISY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING if not verbose else logging.DEBUG)

```

### 4. Code Refactoring Checklist

* **Step 1:** Call `configure_logging()` inside `main.py` prior to invoking `typer` or `click` command handlers.
* **Step 2:** Audit `etfportfolio/ingestion/session.py`. Replace all direct `print()` calls for prompts with a unified UI console interface.
* **Step 3:** Convert inline text warnings to dedicated `logger.warning(...)` calls while keeping user confirmation prompts isolated on clean `stdout` lines.