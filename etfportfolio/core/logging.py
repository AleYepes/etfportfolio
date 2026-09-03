import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from etfportfolio.core.config import settings
from etfportfolio.core.progress import TqdmLoggingHandler

NOISY_LOGGERS = ["httpx", "httpcore", "urllib3", "asyncio", "playwright"]

# Dedicated logger namespace for user-facing narrative output: phase banners,
# per-product progress, and final summaries. Kept separate from the standard
# module loggers (which carry request/retry/warning/error diagnostics) so the
# two channels never mix: this one is stdout-only, plain-text, no timestamps;
# everything else is stderr + the full-DEBUG log file.
_CONSOLE_LOGGER_NAME = "etfportfolio.console"
console = logging.getLogger(_CONSOLE_LOGGER_NAME)


def configure_logging(
    verbose: bool = False,
    log_file: str | Path | None = None,
) -> None:
    """Configures centralized logging for the application, on two channels.

    - File Handler: full structured DEBUG context, stored in a timestamped run log.
    - Stderr Handler: diagnostic logs (INFO, or DEBUG with `verbose`) — request
      attempts, retries, warnings, errors. Routed through tqdm.write so an
      active progress bar is not corrupted. Noisy third-party loggers (httpx,
      playwright, etc.) are suppressed to WARNING unless `verbose` is set.
    - Stdout Handler (`etfportfolio.core.logging.console`): plain, human-facing
      narrative — phase banners, progress, results. Not affected by `verbose`
      and never duplicated into the file/stderr handlers.
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

    stderr_handler = TqdmLoggingHandler(sys.stderr)
    stderr_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    stderr_handler.setFormatter(formatter)
    root_logger.addHandler(stderr_handler)

    for logger_name in NOISY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.DEBUG if verbose else logging.WARNING)

    # Stdout console channel — separate namespace, doesn't propagate up to
    # root (so it never also lands in the stderr handler or the log file).
    console.setLevel(logging.INFO)
    console.propagate = False
    for handler in list(console.handlers):
        console.removeHandler(handler)
    stdout_handler = TqdmLoggingHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    console.addHandler(stdout_handler)
