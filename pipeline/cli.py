"""CLI for running Feefo data pipeline."""

import argparse

from pipeline.extract import run_dlt
from pipeline.settings import DEFAULT_MAX_PAGES, DEFAULT_MERCHANT_ID


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Run Feefo data pipeline")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the pipeline")
    run_parser.add_argument(
        "--merchant-id",
        type=str,
        default=DEFAULT_MERCHANT_ID,
        help=f"Merchant identifier (default: {DEFAULT_MERCHANT_ID})",
    )
    run_parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Maximum number of pages to fetch (default: {DEFAULT_MAX_PAGES})",
    )
    run_parser.add_argument(
        "--mode",
        type=str,
        default="merge",
        choices=["merge", "replace", "append"],
        help="Write mode (default: merge)",
    )
    run_parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Start date filter (optional)",
    )
    run_parser.add_argument(
        "--until",
        type=str,
        default=None,
        help="End date filter (optional)",
    )

    args = parser.parse_args()

    if args.command == "run":
        run_dlt(
            merchant_id=args.merchant_id,
            mode=args.mode,
            max_pages=args.max_pages,
            since=args.since,
            until=args.until,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
