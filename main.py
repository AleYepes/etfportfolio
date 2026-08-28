import sys

import fire

from etfportfolio.core.logging import configure_logging
from etfportfolio.ingestion import pipeline


def main() -> None:
    argv = sys.argv[1:]
    verbose = False
    if "-v" in argv or "--verbose" in argv:
        verbose = True
        argv = [a for a in argv if a not in ("-v", "--verbose")]

    configure_logging(verbose=verbose)
    fire.Fire({"ingest": pipeline.cli}, command=argv)


if __name__ == "__main__":
    main()
