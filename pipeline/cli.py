"""CLI for running Feefo data pipeline."""

import argparse
import logging
import sys

from pipeline.extract import run_dlt
from pipeline.settings import (
    DEFAULT_INCLUDE_RATINGS,
    DEFAULT_MAX_PAGES,
    DEFAULT_MERCHANT_ID,
    DEFAULT_PERIOD_DAYS,
)

# Configure logging
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """
    Configure logging for the application.

    Args:
        verbose: If True, set log level to DEBUG; otherwise INFO
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


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
        "--include-ratings",
        dest="include_ratings",
        action="store_true",
        default=DEFAULT_INCLUDE_RATINGS,
        help="Fetch product ratings for reviewed SKUs (default: enabled)",
    )
    run_parser.add_argument(
        "--no-include-ratings",
        dest="include_ratings",
        action="store_false",
        help="Skip fetching product ratings",
    )
    run_parser.add_argument(
        "--period-days",
        type=int,
        default=DEFAULT_PERIOD_DAYS,
        help="Filter product ratings by period (e.g., 30 for last 30 days, default: all time)",
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
    run_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    args = parser.parse_args()

    # Setup logging based on verbosity
    setup_logging(verbose=getattr(args, "verbose", False))

    if args.command == "run":
        try:
            logger.info("Starting pipeline execution")
            run_dlt(
                merchant_id=args.merchant_id,
                mode=args.mode,
                max_pages=args.max_pages,
                include_ratings=args.include_ratings,
                period_days=args.period_days,
                since=args.since,
                until=args.until,
            )
            logger.info("Pipeline execution completed successfully")
        except Exception as e:
            logger.error("Pipeline execution failed: %s", e)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
