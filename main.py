import fire

from etfportfolio.core.logging import configure_logging
from etfportfolio.ingestion import pipeline, products, session


def main() -> None:
    configure_logging()
    fire.Fire(
        {
            "products": products.cli,
            "auth": session.cli,
            "ingest": pipeline.cli,
        }
    )


if __name__ == "__main__":
    main()
