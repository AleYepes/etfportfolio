from unittest.mock import patch

from etfportfolio.core.logging import configure_logging
from etfportfolio.prep.cli import ObservationsCLI


def test_observations_cli_dispatch(capsys, tmp_path):
    configure_logging(log_file=tmp_path / "test.log")
    cli = ObservationsCLI()

    with (
        patch("etfportfolio.prep.cli.run_observations", return_value=42) as mock_run,
        patch("etfportfolio.prep.cli.run_panel", return_value=1000) as mock_panel,
    ):
        cli(force=False)
        mock_run.assert_called_once_with(force=False)
        mock_panel.assert_called_once_with()
        captured = capsys.readouterr()
        assert "=== Starting Silver Observations Extraction ===" in captured.out
        assert "Processed 42 snapshots." in captured.out
        assert "=== Starting Factor Panel Construction ===" in captured.out
        assert "Wrote 1000 rows." in captured.out

    with (
        patch("etfportfolio.prep.cli.run_observations", return_value=100) as mock_run,
        patch("etfportfolio.prep.cli.run_panel", return_value=50) as mock_panel,
    ):
        cli(force=True)
        mock_run.assert_called_once_with(force=True)
        mock_panel.assert_called_once_with()
        captured = capsys.readouterr()
        assert "Processed 100 snapshots." in captured.out
        assert "Wrote 50 rows." in captured.out
