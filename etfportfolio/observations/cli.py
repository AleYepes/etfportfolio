from etfportfolio.core.logging import console
from etfportfolio.observations.pipeline import run_observations


class ObservationsCLI:
    """CLI surface for the observations phase: `main.py observations [--force]`."""

    def __call__(self, force: bool = False) -> None:
        console.info("=== Starting Silver Observations Extraction ===")
        processed_count = run_observations(force=force)
        console.info(f"=== Observations Complete. Processed {processed_count} snapshots. ===")


cli = ObservationsCLI()
