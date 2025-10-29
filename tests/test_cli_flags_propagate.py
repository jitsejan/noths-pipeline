"""Test that CLI flags correctly propagate to the pipeline functions."""

from unittest.mock import MagicMock, patch
import sys

import pytest

from pipeline.cli import main


def test_cli_run_command_propagates_flags(monkeypatch: MagicMock) -> None:
    """
    Test that CLI flags are correctly passed to run_dlt function.

    Args:
        monkeypatch: pytest's monkeypatch fixture
    """
    # Mock run_dlt to track calls
    with patch("pipeline.cli.run_dlt") as mock_run_dlt:
        # Simulate CLI args
        test_args = [
            "cli.py",
            "run",
            "--merchant-id", "test-merchant",
            "--max-pages", "5",
            "--mode", "replace",
            "--include-ratings",
            "--period-days", "30",
            "--since", "2024-01-01",
            "--until", "2024-12-31",
        ]
        monkeypatch.setattr(sys, "argv", test_args)

        # Run CLI
        main()

        # Verify run_dlt was called with correct parameters
        mock_run_dlt.assert_called_once_with(
            merchant_id="test-merchant",
            mode="replace",
            max_pages=5,
            include_ratings=True,
            period_days=30,
            since="2024-01-01",
            until="2024-12-31",
        )


def test_cli_defaults_propagate(monkeypatch: MagicMock) -> None:
    """
    Test that CLI default values are used when flags are not provided.

    Args:
        monkeypatch: pytest's monkeypatch fixture
    """
    with patch("pipeline.cli.run_dlt") as mock_run_dlt:
        # Simulate CLI args with only run command (use all defaults)
        test_args = ["cli.py", "run"]
        monkeypatch.setattr(sys, "argv", test_args)

        # Run CLI
        main()

        # Verify run_dlt was called with default parameters
        call_kwargs = mock_run_dlt.call_args.kwargs

        assert call_kwargs["merchant_id"] == "notonthehighstreet-com"
        assert call_kwargs["mode"] == "merge"
        assert call_kwargs["max_pages"] == 1
        assert call_kwargs["include_ratings"] is True
        assert call_kwargs["period_days"] is None
        assert call_kwargs["since"] is None
        assert call_kwargs["until"] is None


@pytest.mark.parametrize(
    "flags,expected_value",
    [
        (["--include-ratings"], True),
        (["--no-include-ratings"], False),
        ([], True),  # Default is True
    ],
)
def test_cli_include_ratings_flag(monkeypatch: MagicMock, flags: list[str], expected_value: bool) -> None:
    """
    Test that --include-ratings and --no-include-ratings flags work correctly.

    Args:
        monkeypatch: pytest's monkeypatch fixture
        flags: List of flags to pass (can be empty for default)
        expected_value: Expected value of include_ratings
    """
    with patch("pipeline.cli.run_dlt") as mock_run_dlt:
        test_args = ["cli.py", "run"] + flags
        monkeypatch.setattr(sys, "argv", test_args)

        # Run CLI
        main()

        # Verify run_dlt was called with correct include_ratings value
        call_kwargs = mock_run_dlt.call_args.kwargs
        assert call_kwargs["include_ratings"] is expected_value


@pytest.mark.parametrize("mode", ["merge", "replace", "append"])
def test_cli_mode_choices(monkeypatch: MagicMock, mode: str) -> None:
    """
    Test that all mode choices (merge, replace, append) work correctly.

    Args:
        monkeypatch: pytest's monkeypatch fixture
        mode: The mode to test (parametrized)
    """
    with patch("pipeline.cli.run_dlt") as mock_run_dlt:
        test_args = ["cli.py", "run", "--mode", mode]
        monkeypatch.setattr(sys, "argv", test_args)

        # Run CLI
        main()

        # Verify run_dlt was called with correct mode
        call_kwargs = mock_run_dlt.call_args.kwargs
        assert call_kwargs["mode"] == mode, f"Mode {mode} was not propagated correctly"


@pytest.mark.parametrize(
    "flag_name,flag_value,expected_value,kwarg_name",
    [
        ("--max-pages", "10", 10, "max_pages"),
        ("--period-days", "90", 90, "period_days"),
    ],
)
def test_cli_int_type_conversion(
    monkeypatch: MagicMock, flag_name: str, flag_value: str, expected_value: int, kwarg_name: str
) -> None:
    """
    Test that CLI integer arguments are correctly converted to int.

    Args:
        monkeypatch: pytest's monkeypatch fixture
        flag_name: CLI flag name
        flag_value: String value passed to CLI
        expected_value: Expected integer value
        kwarg_name: Keyword argument name in run_dlt
    """
    with patch("pipeline.cli.run_dlt") as mock_run_dlt:
        test_args = ["cli.py", "run", flag_name, flag_value]
        monkeypatch.setattr(sys, "argv", test_args)

        # Run CLI
        main()

        # Verify the argument is an integer with correct value
        call_kwargs = mock_run_dlt.call_args.kwargs
        assert isinstance(call_kwargs[kwarg_name], int)
        assert call_kwargs[kwarg_name] == expected_value
