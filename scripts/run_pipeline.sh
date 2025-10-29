#!/usr/bin/env bash
# Run the DLT ingestion followed by dbt transforms inside the container.

set -euo pipefail

APP_ROOT="${APP_ROOT:-/app}"

UV_RUN=(uv run --frozen --no-dev)

cd "${APP_ROOT}"

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${APP_ROOT}/dbt}"
export DUCKDB_PATH="${DUCKDB_PATH:-${APP_ROOT}/data/feefo_pipeline.duckdb}"

mkdir -p "$(dirname "${DUCKDB_PATH}")"

MERCHANT_ID="${MERCHANT_ID:-notonthehighstreet-com}"
MAX_PAGES="${MAX_PAGES:-1}"
MODE="${MODE:-merge}"
PERIOD_DAYS="${PERIOD_DAYS:-}"
SINCE="${SINCE:-}"
UNTIL="${UNTIL:-}"
INCLUDE_RATINGS="${INCLUDE_RATINGS:-1}"

echo "Running DLT ingestion..."

DLT_CMD=(
    python -m pipeline.cli run
    --merchant-id "${MERCHANT_ID}"
    --max-pages "${MAX_PAGES}"
    --mode "${MODE}"
)

if [[ -n "${PERIOD_DAYS}" ]]; then
    DLT_CMD+=(--period-days "${PERIOD_DAYS}")
fi

if [[ -n "${SINCE}" ]]; then
    DLT_CMD+=(--since "${SINCE}")
fi

if [[ -n "${UNTIL}" ]]; then
    DLT_CMD+=(--until "${UNTIL}")
fi

if [[ "${INCLUDE_RATINGS}" == "0" ]]; then
    DLT_CMD+=(--no-include-ratings)
fi

"${UV_RUN[@]}" "${DLT_CMD[@]}"

echo "Running dbt transforms..."

cd "${APP_ROOT}/dbt"

"${UV_RUN[@]}" dbt deps
"${UV_RUN[@]}" dbt run
"${UV_RUN[@]}" dbt test

echo "Top 5 products by review count:"

"${UV_RUN[@]}" python - <<'PY'
import os
import duckdb

db_path = os.environ.get("DUCKDB_PATH", "/app/data/feefo_pipeline.duckdb")

with duckdb.connect(db_path) as conn:
    result = conn.execute(
        """
        select product_sku, product_title, review_count, avg_product_review_rating
        from gold.product_summary
        order by review_count desc, avg_product_review_rating desc
        limit 5
        """
    ).fetchall()

if not result:
    print("No data available. Did the pipeline ingest any reviews?")
else:
    for sku, title, count, rating in result:
        print(f"{sku:20} | {title[:40]:40} | reviews={count:4} | avg_rating={rating:.2f}")
PY
