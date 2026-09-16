from unittest.mock import patch

from etfportfolio.core.logging import configure_logging
from etfportfolio.prep.cli import ObservationsCLI


def test_observations_cli_dispatch(capsys, tmp_path):
    configure_logging(log_file=tmp_path / "test.log")
    cli = ObservationsCLI()

    with patch("etfportfolio.prep.cli.run_observations", return_value=42) as mock_run:
        cli(force=False)
        mock_run.assert_called_once_with(force=False)
        captured = capsys.readouterr()
        assert "=== Starting Silver Observations Extraction ===" in captured.out
        assert "Processed 42 snapshots." in captured.out

    with patch("etfportfolio.prep.cli.run_observations", return_value=100) as mock_run:
        cli(force=True)
        mock_run.assert_called_once_with(force=True)
        captured = capsys.readouterr()
        assert "Processed 100 snapshots." in captured.out
