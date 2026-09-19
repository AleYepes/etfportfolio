from __future__ import annotations

from etfportfolio.core.logging import console
from etfportfolio.prep.panel import run_panel
from etfportfolio.prep.pipeline import run_observations


class ObservationsCLI:
    """CLI surface for preprocessing: `main.py prep [--force]`.

    Runs Silver observation extraction then rebuilds `silver.monthly_panel`.
    """

    def __call__(self, force: bool = False) -> None:
        console.info("=== Starting Silver Observations Extraction ===")
        processed_count = run_observations(force=force)
        console.info(f"=== Observations Complete. Processed {processed_count} snapshots. ===")
        console.info("=== Starting Factor Panel Construction ===")
        n_rows = run_panel()
        console.info(f"=== Panel Complete. Wrote {n_rows} rows. ===")


cli = ObservationsCLI()
