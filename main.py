import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import fire

from etfportfolio.core.config import settings
from etfportfolio.ingestion import pipeline, products, session


def setup_logging() -> None:
    """Configures logging with console and run-timestamped file handler."""
    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{timestamp}.log"

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def main() -> None:
    setup_logging()
    fire.Fire(
        {
            "products": products.cli,
            "auth": session.cli,
            "ingest": pipeline.cli,
        }
    )


if __name__ == "__main__":
    main()
