import logging

from etfportfolio.core.logging import NOISY_LOGGERS, configure_logging


def test_configure_logging_default_mutes_info(tmp_path, capsys):
    log_file = tmp_path / "test_default.log"
    configure_logging(verbose=False, log_file=log_file)

    test_logger = logging.getLogger("test_logger")
    test_logger.info("This is an info diagnostic")
    test_logger.warning("This is a warning message")

    # Stderr handler should only capture WARNING and above
    captured = capsys.readouterr()
    assert "This is an info diagnostic" not in captured.err
    assert "This is a warning message" in captured.err

    # Log file should capture both INFO and WARNING (DEBUG level)
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "This is an info diagnostic" in content
    assert "This is a warning message" in content

    # Verify noisy loggers are muted to WARNING
    for noisy in NOISY_LOGGERS:
        assert logging.getLogger(noisy).level == logging.WARNING


def test_configure_logging_verbose_shows_info(tmp_path, capsys):
    log_file = tmp_path / "test_verbose.log"
    configure_logging(verbose=True, log_file=log_file)

    test_logger = logging.getLogger("test_logger_verbose")
    test_logger.info("Verbose info message")

    # Stderr handler should capture INFO when verbose=True
    captured = capsys.readouterr()
    assert "Verbose info message" in captured.err

    # Log file also contains it
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "Verbose info message" in content

    # Verify noisy loggers are set to DEBUG
    for noisy in NOISY_LOGGERS:
        assert logging.getLogger(noisy).level == logging.DEBUG
