"""Settings for Feefo API integration."""

# Feefo API configuration
FEEFO_API_BASE_URL: str = "https://api.feefo.com/api/20"

# Default merchant identifier
DEFAULT_MERCHANT_ID: str = "notonthehighstreet-com"

# Pagination defaults
DEFAULT_MAX_PAGES: int = 1
DEFAULT_PAGE_SIZE: int = 100

# Product ratings defaults
DEFAULT_INCLUDE_RATINGS: bool = True
DEFAULT_PERIOD_DAYS: int | None = None  # None = all time (API default)
