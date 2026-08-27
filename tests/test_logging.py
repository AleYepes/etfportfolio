import logging

from etfportfolio.core.logging import NOISY_LOGGERS, configure_logging


def test_configure_logging(tmp_path):
    log_file = tmp_path / "test.log"
    configure_logging(verbose=False, log_file=log_file)

    test_logger = logging.getLogger("test_logger")
    test_logger.info("Hello logging")

    # Verify log file was created and written to
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "Hello logging" in content

    # Verify noisy loggers are muted to WARNING
    for noisy in NOISY_LOGGERS:
        assert logging.getLogger(noisy).level == logging.WARNING

    # Reconfigure with verbose=True
    configure_logging(verbose=True, log_file=log_file)
    for noisy in NOISY_LOGGERS:
        assert logging.getLogger(noisy).level == logging.DEBUG
