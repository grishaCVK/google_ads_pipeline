import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import clickhouse_db
import config
import google_ads_api


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


def insert_grouped_rows(
    *,
    grouped_rows: dict[str, list[list]],
    row_type: str,
    insert_func,
) -> None:
    for table_name, rows in grouped_rows.items():
        insert_func(
            table_name=table_name,
            rows=rows,
        )

        print(f"{table_name}: inserted {len(rows)} {row_type} rows")


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


def run_pipeline_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> None:
    print(
        f"Start Google Ads period: "
        f"customer_id={customer_id}, {date_since} -> {date_until}"
    )

    # 1. Hourly: campaign + hour + device + network.
    hourly_response_data, hourly_grouped_rows = (
        google_ads_api.fetch_ad_hourly_data(
            customer_id=customer_id,
            date_since=date_since,
            date_until=date_until,
        )
    )

    print(
        f"Fetched Google Ads hourly data: "
        f"customer_id={customer_id}, "
        f"rows={hourly_response_data['rows_count']}"
    )

    # 2. Daily ad: campaign + ad_group + ad + device + network.
    daily_response_data, daily_grouped_rows = (
        google_ads_api.fetch_ad_group_ad_daily_data(
            customer_id=customer_id,
            date_since=date_since,
            date_until=date_until,
        )
    )

    print(
        f"Fetched Google Ads daily ad data: "
        f"customer_id={customer_id}, "
        f"rows={daily_response_data['rows_count']}"
    )

    # 3. Daily geo: campaign + device + network + country + region + city.
    daily_geo_response_data, daily_geo_grouped_rows = (
        google_ads_api.fetch_geo_daily_data(
            customer_id=customer_id,
            date_since=date_since,
            date_until=date_until,
        )
    )

    print(
        f"Fetched Google Ads daily geo data: "
        f"customer_id={customer_id}, "
        f"rows={daily_geo_response_data['rows_count']}"
    )

    # 4. Daily campaign: campaign + date.
    daily_campaign_response_data, daily_campaign_grouped_rows = (
        google_ads_api.fetch_daily_campaign_data(
            customer_id=customer_id,
            date_since=date_since,
            date_until=date_until,
        )
    )

    print(
        f"Fetched Google Ads daily campaign data: "
        f"customer_id={customer_id}, "
        f"rows={daily_campaign_response_data['rows_count']}"
    )

    # 5. Daily search terms: campaign + ad_group + search_term + keyword + device + network.
    daily_search_term_response_data, daily_search_term_rows = (
        google_ads_api.fetch_daily_search_term_data(
            customer_id=customer_id,
            date_since=date_since,
            date_until=date_until,
        )
    )

    print(
        f"Fetched Google Ads daily search term data: "
        f"customer_id={customer_id}, "
        f"rows={daily_search_term_response_data['rows_count']}"
    )

    # 6. Creative assets: campaign/ad/ad_group/asset_group -> asset.
    creative_assets_response_data, creative_asset_rows = (
        google_ads_api.fetch_creative_assets_data(
            customer_id=customer_id,
        )
    )

    print(
        f"Fetched Google Ads creative assets data: "
        f"customer_id={customer_id}, "
        f"rows={creative_assets_response_data['rows_count']}"
    )

    # 7. raw_data append-only. Старые raw responses не удаляем.
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
            "segments": [
                "date",
                "hour",
                "device",
                "ad_network_type",
            ],
        },
    )

    print(
        f"Raw Google Ads hourly data saved: "
        f"customer_id={customer_id}, "
        f"rows={hourly_response_data['rows_count']}"
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
            "segments": [
                "date",
                "device",
                "ad_network_type",
            ],
        },
    )

    print(
        f"Raw Google Ads daily ad data saved: "
        f"customer_id={customer_id}, "
        f"rows={daily_response_data['rows_count']}"
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
            "segments": [
                "date",
                "device",
                "ad_network_type",
                "geo_target_region",
                "geo_target_city",
            ],
        },
    )

    print(
        f"Raw Google Ads daily geo data saved: "
        f"customer_id={customer_id}, "
        f"rows={daily_geo_response_data['rows_count']}"
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
            "segments": [
                "date",
            ],
        },
    )

    print(
        f"Raw Google Ads daily campaign data saved: "
        f"customer_id={customer_id}, "
        f"rows={daily_campaign_response_data['rows_count']}"
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
            "segments": [
                "date",
                "device",
                "ad_network_type",
                "keyword",
            ],
        },
    )

    print(
        f"Raw Google Ads daily search term data saved: "
        f"customer_id={customer_id}, "
        f"rows={daily_search_term_response_data['rows_count']}"
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

    print(
        f"Raw Google Ads creative assets data saved: "
        f"customer_id={customer_id}, "
        f"rows={creative_assets_response_data['rows_count']}"
    )

    # 8. Перезаписываем formatted hourly_campaign_level.
    clickhouse_db.delete_goal_tables_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    insert_grouped_rows(
        grouped_rows=hourly_grouped_rows,
        row_type="hourly",
        insert_func=clickhouse_db.insert_goal_rows,
    )

    # 9. Перезаписываем formatted daily_ad_level.
    clickhouse_db.delete_daily_tables_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    insert_grouped_rows(
        grouped_rows=daily_grouped_rows,
        row_type="daily",
        insert_func=clickhouse_db.insert_daily_rows,
    )

    # 10. Перезаписываем formatted geo_daily_region_level.
    clickhouse_db.delete_daily_geo_tables_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    insert_grouped_rows(
        grouped_rows=daily_geo_grouped_rows,
        row_type="daily_geo",
        insert_func=clickhouse_db.insert_daily_geo_rows,
    )

    # 11. Перезаписываем formatted daily_campaign_level.
    clickhouse_db.delete_daily_campaign_tables_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    insert_grouped_rows(
        grouped_rows=daily_campaign_grouped_rows,
        row_type="daily_campaign",
        insert_func=clickhouse_db.insert_daily_campaign_rows,
    )

    # 12. Перезаписываем formatted daily_search_term_level.
    clickhouse_db.delete_daily_search_term_table_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    clickhouse_db.insert_daily_search_term_rows(
        rows=daily_search_term_rows,
    )

    print(
        f"{clickhouse_db.DAILY_SEARCH_TERM_TABLE}: "
        f"inserted {len(daily_search_term_rows)} daily_search_term rows"
    )

    # 13. Перезаписываем creative assets по customer_id.
    clickhouse_db.delete_creative_assets_for_customer(
        customer_id=customer_id,
    )

    clickhouse_db.insert_creative_asset_rows(
        rows=creative_asset_rows,
    )

    print(
        f"{clickhouse_db.CREATIVE_ASSET_TABLE}: "
        f"inserted {len(creative_asset_rows)} creative_asset rows"
    )

    print(
        f"Finished Google Ads period: "
        f"customer_id={customer_id}, {date_since} -> {date_until}"
    )


def main() -> None:
    config.validate_config()

    customer_ids = config.GOOGLE_ADS_CUSTOMER_IDS

    if config.BACKFILL_MODE:
        date_since = config.BACKFILL_START_DATE
        date_until = config.BACKFILL_END_DATE or get_yesterday()
        batch_days = config.BACKFILL_BATCH_DAYS

        batches = iter_date_batches(
            start_date=date_since,
            end_date=date_until,
            batch_days=batch_days,
        )

        print(
            f"Google Ads backfill started: "
            f"{date_since} -> {date_until}, batches={len(batches)}"
        )

        for customer_id in customer_ids:
            for batch_since, batch_until in batches:
                run_pipeline_for_period(
                    customer_id=customer_id,
                    date_since=batch_since,
                    date_until=batch_until,
                )

            run_embeddings_after_pipeline(
                customer_id=customer_id,
            )

        print("Google Ads backfill finished")

    else:
        yesterday = get_yesterday()

        print(f"Google Ads daily run started: {yesterday}")

        for customer_id in customer_ids:
            run_pipeline_for_period(
                customer_id=customer_id,
                date_since=yesterday,
                date_until=yesterday,
            )

            run_embeddings_after_pipeline(
                customer_id=customer_id,
            )

        print("Google Ads daily run finished")


if __name__ == "__main__":
    main()
