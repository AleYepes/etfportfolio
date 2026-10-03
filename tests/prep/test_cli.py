from unittest.mock import patch

from etfportfolio.core.logging import configure_logging
from etfportfolio.prep.cli import ObservationsCLI


def test_cli_subcommands(capsys, tmp_path):
    configure_logging(log_file=tmp_path / "test.log")
    cli = ObservationsCLI()

    with (
        patch("etfportfolio.prep.cli.run_observations", return_value=42) as mock_run,
        patch("etfportfolio.prep.cli.run_panel", return_value=1000) as mock_panel,
    ):
        # 1. obs(force=False) invokes run_observations(force=False) only
        res_obs = cli.obs(force=False)
        assert res_obs == 42
        mock_run.assert_called_once_with(force=False)
        mock_panel.assert_not_called()

    with (
        patch("etfportfolio.prep.cli.run_observations", return_value=42) as mock_run,
        patch("etfportfolio.prep.cli.run_panel", return_value=1000) as mock_panel,
    ):
        # 2. panel() invokes run_panel() only
        res_panel = cli.panel()
        assert res_panel == 1000
        mock_panel.assert_called_once_with()
        mock_run.assert_not_called()

    with (
        patch("etfportfolio.prep.cli.run_observations", return_value=100) as mock_run,
        patch("etfportfolio.prep.cli.run_panel", return_value=50) as mock_panel,
    ):
        # 3. cli(force=True) invokes both sequentially with force=True
        cli(force=True)
        mock_run.assert_called_once_with(force=True)
        mock_panel.assert_called_once_with()
