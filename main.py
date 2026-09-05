import sys

import fire

from etfportfolio.core.logging import configure_logging
from etfportfolio.ingestion import pipeline as ingest_pipeline
from etfportfolio.observations import cli as obs_cli


def main() -> None:
    argv = sys.argv[1:]
    verbose = False
    if "-v" in argv or "--verbose" in argv:
        verbose = True
        argv = [a for a in argv if a not in ("-v", "--verbose")]

    configure_logging(verbose=verbose)
    fire.Fire(
        {
            "ingest": ingest_pipeline.cli,
            "observations": obs_cli.cli,
        },
        command=argv,
    )


if __name__ == "__main__":
    main()
