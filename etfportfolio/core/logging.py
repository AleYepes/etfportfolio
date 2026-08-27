import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from etfportfolio.core.config import settings

NOISY_LOGGERS = ["httpx", "httpcore", "urllib3", "asyncio", "playwright"]


def configure_logging(
    verbose: bool = False,
    log_file: str | Path | None = None,
) -> None:
    """Configures centralized dual-channel logging for the application.

    - File Handler: Full structured DEBUG context stored in timestamped run log.
    - Stderr Handler: Diagnostic logs (INFO/DEBUG) routed to stderr.
    - Noisy loggers (e.g. httpx, playwright) are suppressed to WARNING unless verbose mode is enabled.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Remove existing handlers to prevent duplicates
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if log_file is None:
        log_dir = Path(settings.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        target_log_file = log_dir / f"{timestamp}.log"
    else:
        target_log_file = Path(log_file)
        target_log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(str(target_log_file), encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    for logger_name in NOISY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.DEBUG if verbose else logging.WARNING)
