from __future__ import annotations

from etfportfolio.core.logging import console
from etfportfolio.prep.panel import run_panel
from etfportfolio.prep.pipeline import run_observations


class ObservationsCLI:
    """CLI surface for preprocessing: `main.py prep`."""

    def __call__(self, force: bool = False) -> None:
        """Runs full prep pipeline: observations extraction then monthly panel."""
        self.obs(force=force)
        self.panel()

    def obs(self, force: bool = False) -> int:
        """Runs Silver observations extraction only."""
        console.info("=== Starting Silver Observations Extraction ===")
        processed_count = run_observations(force=force)
        console.info(f"=== Observations Complete. Processed {processed_count} snapshots. ===")
        return processed_count

    def panel(self) -> int:
        """Rebuilds silver.monthly_panel from current Silver observations."""
        console.info("=== Starting Factor Panel Construction ===")
        n_rows = run_panel()
        console.info(f"=== Panel Complete. Wrote {n_rows} rows. ===")
        return n_rows


cli = ObservationsCLI()
