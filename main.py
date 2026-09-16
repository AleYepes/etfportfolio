import sys

import fire

from etfportfolio.core.logging import configure_logging
from etfportfolio.ingest import pipeline as ingest_pipeline
from etfportfolio.prep import cli as prep_pipeline


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
            "prep": prep_pipeline.cli,
        },
        command=argv,
    )


if __name__ == "__main__":
    main()
