"""tqdm bars that coexist with logging."""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterable, Iterator

from tqdm import tqdm


class TqdmLoggingHandler(logging.StreamHandler):
    """StreamHandler that writes through tqdm.write so an active bar is not corrupted."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            tqdm.write(msg, file=self.stream)
            self.flush()
        except Exception:
            self.handleError(record)


def bars_disabled() -> bool:
    return not sys.stderr.isatty()


def iter_progress[T](items: Iterable[T], *, desc: str) -> Iterator[T]:
    """Iterate `items` under a tqdm bar (disabled when stderr is not a TTY)."""
    yield from tqdm(
        items,
        desc=desc,
        unit="product",
        disable=bars_disabled(),
        file=sys.stderr,
        dynamic_ncols=True,
    )


def progress_bar(total: int | None = None, *, desc: str, unit: str = "product") -> tqdm:
    """Manual bar for loops — call ``bar.update(...)`` per completion."""
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        disable=bars_disabled(),
        file=sys.stderr,
        dynamic_ncols=True,
    )
