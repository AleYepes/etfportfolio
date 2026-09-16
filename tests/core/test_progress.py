import io
import logging
import sys

from tqdm import tqdm

from etfportfolio.core.progress import (
    TqdmLoggingHandler,
    bars_disabled,
    iter_progress,
    progress_bar,
)


def test_bars_disabled(monkeypatch):
    monkeypatch.setattr(sys.stderr, "isatty", lambda: False)
    assert bars_disabled() is True

    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    assert bars_disabled() is False


def test_tqdm_logging_handler(monkeypatch):
    stream = io.StringIO()
    handler = TqdmLoggingHandler(stream)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger("test_tqdm_handler")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    try:
        logger.info("Progress log message")
        handler.flush()
        assert "Progress log message" in stream.getvalue()
    finally:
        logger.removeHandler(handler)


def test_iter_progress(monkeypatch):
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    items = [1, 2, 3, 4, 5]
    consumed = list(iter_progress(items, desc="Testing iter"))
    assert consumed == items


def test_progress_bar(monkeypatch):
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    bar = progress_bar(total=10, desc="Manual bar")
    assert isinstance(bar, tqdm)
    assert bar.total == 10
    bar.update(5)
    assert bar.n == 5
    bar.close()
