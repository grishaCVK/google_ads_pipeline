"""
main.py

Точка входа ETL-пайплайна Google Ads.

Режимы запуска (управляются через .env):
- BACKFILL_MODE=True  — полная выгрузка за диапазон дат (BACKFILL_START_DATE
                        по BACKFILL_END_DATE или вчера), батчами по
                        BACKFILL_BATCH_DAYS дней.
- BACKFILL_MODE=False — ежедневный запуск (дефолт): выгрузка за вчерашний
                        день, запускается по cron.

Шаги пайплайна для каждого батча:
1. fetch_and_save_raw  — выгрузка из Google Ads API в raw DB
2. raw_to_staging      — google_staging_loader читает raw,
                         трансформирует и пишет в staging DB
3. staging_to_core     — google_core_loader перекладывает в core DB
"""

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import clickhouse_db
import config
import google_ads_api
import google_staging_loader
import etl_logger
import google_core_loader


ALMATY_TZ = ZoneInfo("Asia/Almaty")


def get_yesterday() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def iter_date_batches(
    start_date: str,
    end_date: str,
    batch_days: int,
) -> list[tuple[str, str]]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    batches: list[tuple[str, str]] = []
    current_start = start

    while current_start <= end:
        current_end = min(
            current_start + timedelta(days=batch_days - 1),
            end,
        )

        batches.append(
            (
                current_start.isoformat(),
                current_end.isoformat(),
            )
        )

        current_start = current_end + timedelta(days=1)

    return batches


def insert_raw_response(
    *,
    customer_id: str,
    query_name: str,
    query_text: str,
    response_data: dict,
    request_params: dict,
) -> None:
    clickhouse_db.insert_raw_data(
        customer_id=customer_id,
        query_name=query_name,
        query_text=query_text,
        response_data=response_data,
        request_params=request_params,
    )


def get_optional_int_env(name: str) -> int | None:
    value = os.getenv(name)

    if value is None or not value.strip():
        return None

    return int(value)


def run_embeddings_after_pipeline(
    *,
    customer_id: str,
) -> None:
    """
    Запускает Google Ads embeddings после основного pipeline.

    IMAGE -> 1 embedding
    YOUTUBE_VIDEO -> 5 embeddings: 0%, 25%, 50%, 75%, 100%

    По умолчанию включено.
    Чтобы временно отключить:
    GOOGLE_EMBEDDINGS_ENABLED=0
    """
    enabled = os.getenv("GOOGLE_EMBEDDINGS_ENABLED", "1").strip().lower()

    if enabled in ("0", "false", "no", "off"):
        print(
            f"Google Ads embeddings skipped: "
            f"customer_id={customer_id}, GOOGLE_EMBEDDINGS_ENABLED={enabled}"
        )
        return

    limit = get_optional_int_env("GOOGLE_EMBEDDINGS_LIMIT")

    print(
        f"Start Google Ads embeddings after pipeline: "
        f"customer_id={customer_id}, limit={limit or 'ALL'}"
    )

    # Импорт внутри функции специально:
    # основной pipeline сначала спокойно завершается,
    # а тяжелые библиотеки embeddings грузятся только на этапе embeddings.
    import embeddings

    embeddings.run_google_embeddings(
        customer_id=customer_id,
        limit=limit,
    )

    print(
        f"Finished Google Ads embeddings after pipeline: "
        f"customer_id={customer_id}"
    )


def run_creative_assets_only(
    *,
    customer_id: str,
) -> None:
    print(
        f"Google Ads creative assets only started: "
        f"customer_id={customer_id}"
    )

    response_data = google_ads_api.fetch_creative_assets_data(
        customer_id=customer_id,
    )

    print(
        f"Fetched Google Ads creative assets data: "
        f"customer_id={customer_id}, "
        f"rows={response_data['rows_count']}"
    )

    insert_raw_response(
        customer_id=customer_id,
        query_name="creative_assets",
        query_text=(
            google_ads_api.queries.AD_GROUP_AD_ASSET_QUERY
            + "\n\n-- asset_group_asset\n\n"
            + google_ads_api.queries.ASSET_GROUP_ASSET_QUERY
        ),
        response_data=response_data,
        request_params={
            "customer_id": customer_id,
            "source": [
                "ad_group_ad_asset_view",
                "asset_group_asset",
            ],
            "granularity": "creative_asset_level",
            "mode": "creative_assets_only",
        },
    )

    print(
        f"Raw Google Ads creative assets data saved: "
        f"customer_id={customer_id}, "
        f"rows={response_data['rows_count']}"
    )

    written = google_staging_loader.load_creatives_from_raw(
        customer_id=customer_id,
    )

    print(
        f"{clickhouse_db.CREATIVE_ASSET_TABLE}: "
        f"inserted {written} creative_asset rows"
    )

    run_embeddings_after_pipeline(
        customer_id=customer_id,
    )

    print(
        f"Google Ads creative assets only finished: "
        f"customer_id={customer_id}"
    )


def run_pipeline_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
    run_id: str,
    run_type: str = "daily",
) -> None:
    print(
        f"Start Google Ads period: "
        f"customer_id={customer_id}, "
        f"{date_since} -> {date_until}"
    )

    total_raw_rows = 0
    total_staging_rows = 0

    fetch_creative_assets = config.get_bool_env(
        "GOOGLE_FETCH_CREATIVE_ASSETS",
        default=True,
    )

    # ----------------------------------------------------------
    # Шаг 1: fetch_and_save_raw — выгрузка из Google API в raw
    # ----------------------------------------------------------
    with etl_logger.etl_step(
        run_id=run_id,
        step_name="fetch_and_save_raw",
        step_order=1,
        target_database=config.CLICKHOUSE_RAW_DB,
        target_table="raw_data",
    ) as step:

        hourly_response_data = google_ads_api.fetch_ad_hourly_data(
            customer_id=customer_id,
            date_since=date_since,
            date_until=date_until,
        )

        daily_response_data = (
            google_ads_api.fetch_ad_group_ad_daily_data(
                customer_id=customer_id,
                date_since=date_since,
                date_until=date_until,
            )
        )

        daily_geo_response_data = google_ads_api.fetch_geo_daily_data(
            customer_id=customer_id,
            date_since=date_since,
            date_until=date_until,
        )

        daily_campaign_response_data = (
            google_ads_api.fetch_daily_campaign_data(
                customer_id=customer_id,
                date_since=date_since,
                date_until=date_until,
            )
        )

        daily_search_term_response_data = (
            google_ads_api.fetch_daily_search_term_data(
                customer_id=customer_id,
                date_since=date_since,
                date_until=date_until,
            )
        )

        gender_daily_response_data = (
            google_ads_api.fetch_gender_daily_data(
                customer_id=customer_id,
                date_since=date_since,
                date_until=date_until,
            )
        )

        insert_raw_response(
            customer_id=customer_id,
            query_name="ad_hourly",
            query_text=google_ads_api.queries.AD_HOURLY_QUERY,
            response_data=hourly_response_data,
            request_params={
                "customer_id": customer_id,
                "date_since": date_since,
                "date_until": date_until,
                "source": "campaign",
                "granularity": "hourly_campaign_level",
            },
        )

        insert_raw_response(
            customer_id=customer_id,
            query_name="ad_group_ad_daily",
            query_text=google_ads_api.queries.AD_GROUP_AD_DAILY_QUERY,
            response_data=daily_response_data,
            request_params={
                "customer_id": customer_id,
                "date_since": date_since,
                "date_until": date_until,
                "source": "ad_group_ad",
                "granularity": "daily_ad_level",
            },
        )

        insert_raw_response(
            customer_id=customer_id,
            query_name="geo_daily",
            query_text=google_ads_api.queries.GEO_DAILY_QUERY,
            response_data=daily_geo_response_data,
            request_params={
                "customer_id": customer_id,
                "date_since": date_since,
                "date_until": date_until,
                "source": "geographic_view",
                "granularity": "geo_daily_region_level",
            },
        )

        insert_raw_response(
            customer_id=customer_id,
            query_name="daily_campaign",
            query_text=google_ads_api.queries.DAILY_CAMPAIGN_QUERY,
            response_data=daily_campaign_response_data,
            request_params={
                "customer_id": customer_id,
                "date_since": date_since,
                "date_until": date_until,
                "source": "campaign",
                "granularity": "daily_campaign_level",
            },
        )

        insert_raw_response(
            customer_id=customer_id,
            query_name="daily_search_term",
            query_text=google_ads_api.queries.SEARCH_TERM_DAILY_QUERY,
            response_data=daily_search_term_response_data,
            request_params={
                "customer_id": customer_id,
                "date_since": date_since,
                "date_until": date_until,
                "source": "search_term_view",
                "granularity": "daily_search_term_level",
            },
        )

        insert_raw_response(
            customer_id=customer_id,
            query_name="gender_daily",
            query_text=google_ads_api.queries.GENDER_DAILY_QUERY,
            response_data=gender_daily_response_data,
            request_params={
                "customer_id": customer_id,
                "date_since": date_since,
                "date_until": date_until,
                "source": "gender_view",
                "granularity": "gender_daily_level",
            },
        )

        raw_count = (
            hourly_response_data["rows_count"]
            + daily_response_data["rows_count"]
            + daily_geo_response_data["rows_count"]
            + daily_campaign_response_data["rows_count"]
            + daily_search_term_response_data["rows_count"]
            + gender_daily_response_data["rows_count"]
        )

        if fetch_creative_assets:
            creative_assets_response_data = (
                google_ads_api.fetch_creative_assets_data(
                    customer_id=customer_id,
                )
            )
            insert_raw_response(
                customer_id=customer_id,
                query_name="creative_assets",
                query_text=(
                    google_ads_api.queries.AD_GROUP_AD_ASSET_QUERY
                    + "\n\n-- asset_group_asset\n\n"
                    + google_ads_api.queries.ASSET_GROUP_ASSET_QUERY
                ),
                response_data=creative_assets_response_data,
                request_params={
                    "customer_id": customer_id,
                    "source": [
                        "ad_group_ad_asset_view",
                        "asset_group_asset",
                    ],
                    "granularity": "creative_asset_level",
                },
            )
            raw_count += creative_assets_response_data["rows_count"]

        step["input_rows"] = raw_count
        step["output_rows"] = raw_count
        total_raw_rows = raw_count

    # ----------------------------------------------------------
    # Шаг 2: raw_to_staging — google_staging_loader читает raw,
    # трансформирует и пишет в staging
    # ----------------------------------------------------------
    total_staging_rows = google_staging_loader.run_raw_to_staging(
        run_id=run_id,
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
        load_creatives=fetch_creative_assets,
    )

    # ----------------------------------------------------------
    # Шаг 3: staging_to_core
    # ----------------------------------------------------------
    total_core_rows = google_core_loader.run_staging_to_core(
        run_id=run_id,
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
        load_creatives=fetch_creative_assets,
    )

    print(
        f"Finished Google Ads period: "
        f"customer_id={customer_id}, "
        f"{date_since} -> {date_until}"
    )

    return total_raw_rows, total_staging_rows, total_core_rows


def main() -> None:
    config.validate_config()

    customer_ids = config.GOOGLE_ADS_CUSTOMER_IDS

    only_creative_assets = config.get_bool_env(
        "GOOGLE_ONLY_CREATIVE_ASSETS",
        default=False,
    )

    if only_creative_assets:
        print("Google Ads creative assets only mode started")

        for customer_id in customer_ids:
            run_id = etl_logger.create_run(
                run_type="creative_assets_only",
                date_since="1970-01-01",
                date_until="2099-12-31",
            )
            started_at = datetime.now(ALMATY_TZ)

            try:
                run_creative_assets_only(
                    customer_id=customer_id,
                )
                etl_logger.finish_run(
                    run_id=run_id,
                    started_at=started_at,
                    status="success",
                )
            except Exception as e:
                import traceback
                etl_logger.finish_run(
                    run_id=run_id,
                    started_at=started_at,
                    status="failed",
                    error_stage="creative_assets_only",
                    error_message=str(e),
                    error_trace=traceback.format_exc(),
                )
                raise

        print("Google Ads creative assets only mode finished")
        return

    if config.BACKFILL_MODE:
        date_since = config.BACKFILL_START_DATE
        date_until = (
            config.BACKFILL_END_DATE or get_yesterday()
        )
        batch_days = config.BACKFILL_BATCH_DAYS
        run_type = "backfill"
    else:
        date_since = get_yesterday()
        date_until = get_yesterday()
        batch_days = 1
        run_type = "daily"

    batches = iter_date_batches(
        start_date=date_since,
        end_date=date_until,
        batch_days=batch_days,
    )

    print(
        f"Google Ads {run_type} started: "
        f"{date_since} -> {date_until}, "
        f"batches={len(batches)}"
    )

    for customer_id in customer_ids:
        total_raw = 0
        total_staging = 0
        total_core = 0

        # Один run_id на весь customer за все батчи
        run_id = etl_logger.create_run(
            run_type=run_type,
            date_since=date_since,
            date_until=date_until,
        )
        started_at = datetime.now(ALMATY_TZ)

        try:
            for batch_since, batch_until in batches:
                raw, staging, _ = (
                    run_pipeline_for_period(
                        customer_id=customer_id,
                        date_since=batch_since,
                        date_until=batch_until,
                        run_id=run_id,
                        run_type=run_type,
                    )
                )
                total_raw += raw
                total_staging += staging

            run_embeddings_after_pipeline(
                customer_id=customer_id,
            )

            # Считаем фактические строки в core
            total_core = google_core_loader.count_core_rows(
                date_since=date_since,
                date_until=date_until,
            )

            etl_logger.finish_run(
                run_id=run_id,
                started_at=started_at,
                status="success",
                total_raw_rows=total_raw,
                total_staging_rows=total_staging,
                total_core_rows=total_core,
                actual_min_date=date_since,
                actual_max_date=date_until,
            )

        except Exception as e:
            import traceback
            etl_logger.finish_run(
                run_id=run_id,
                started_at=started_at,
                status="failed",
                total_raw_rows=total_raw,
                total_staging_rows=total_staging,
                total_core_rows=total_core,
                error_message=str(e),
                error_trace=traceback.format_exc(),
            )
            raise

    print(f"Google Ads {run_type} finished")


if __name__ == "__main__":
    main()
