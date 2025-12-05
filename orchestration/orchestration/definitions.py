from dagster import Definitions, define_asset_job, ScheduleDefinition
from orchestration.assets import (
    news_csv,
    prices_csv,
    raw_bq_loaded,
    stg_stock_prices,
    int_stock_prices_enrich,
)

# 1) Collect all assets
all_assets = [
    news_csv,
    prices_csv,
    raw_bq_loaded,
    stg_stock_prices,
    int_stock_prices_enrich,
]

# 2) Define asset job
mag7_intel_daily_job = define_asset_job(
    name="mag7_intel_daily_job",
    selection=all_assets,
)

# 3) Define schedule
daily_mag7_schedule = ScheduleDefinition(
    job=mag7_intel_daily_job,
    cron_schedule="00 21 * * *",
    execution_timezone="Asia/Singapore",
)

# 4) Final Dagster Definitions object
definitions = Definitions(
    assets=all_assets,
    jobs=[mag7_intel_daily_job],
    schedules=[daily_mag7_schedule],
)
