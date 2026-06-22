"""
google_staging_loader.py

Шаг 2 ETL: google_ads_raw → google_ads_staging.
Читает сырые строки из raw_data, трансформирует их в строки
staging-таблиц и записывает в staging.

Единственный источник для staging — raw_data. Сами строки
парсятся теми же build_*-функциями из google_ads_api, что и при
выгрузке, но на вход им подаётся не protobuf, а dict из raw
(обёрнутый в _W, чтобы доступ row.metrics.clicks работал как
раньше). Размерные справочники (бюджеты, гео-константы,
YouTube-метаданные) подтягиваются из API в момент загрузки —
это обогащение, а не источник фактов.

Корзина (таблица) кампании определяется по
advertising_channel_type внутри build_*-функций.
"""

from typing import Any

import clickhouse_db
import config
import etl_logger
import google_ads_api as gads


STAGING_DB = config.CLICKHOUSE_STAGING_DB


# ------------------------------------------------------------
# Адаптер raw dict → protobuf-подобный доступ
# ------------------------------------------------------------

class _Missing:
    """
    Заглушка для отсутствующих в raw полей.

    MessageToDict опускает дефолтные значения protobuf, поэтому
    обращение к отсутствующему полю должно вести себя как
    protobuf-дефолт: 0 / 0.0 / '' / пустая последовательность.
    """

    def __getattr__(self, name: str) -> "Any":
        return self

    def __bool__(self) -> bool:
        return False

    def __int__(self) -> int:
        return 0

    def __float__(self) -> float:
        return 0.0

    def __str__(self) -> str:
        return ""

    def __iter__(self):
        return iter(())


_MISSING = _Missing()


def _wrap(value: Any) -> Any:
    if isinstance(value, dict):
        return _W(value)
    if isinstance(value, list):
        return [
            _W(item) if isinstance(item, dict) else item
            for item in value
        ]
    return value


class _W:
    """
    Оборачивает dict из raw так, чтобы build_*-функции
    обращались к нему как к protobuf-объекту (row.metrics.clicks).
    """

    __slots__ = ("_d",)

    def __init__(self, data: dict[str, Any]) -> None:
        self._d = data

    def __getattr__(self, name: str) -> Any:
        data = object.__getattribute__(self, "_d")

        if name not in data:
            return _MISSING

        return _wrap(data[name])


# ------------------------------------------------------------
# Загрузчики по источникам
# ------------------------------------------------------------

def _load_hourly(
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[int, int]:
    raw = clickhouse_db.read_raw(
        query_name="ad_hourly",
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    grouped: dict[str, list[list[Any]]] = {
        table: [] for table in clickhouse_db.HOURLY_TABLES
    }

    for item in raw:
        table, formatted = gads.build_clickhouse_row(_W(item))
        grouped.setdefault(table, []).append(formatted)

    clickhouse_db.delete_goal_tables_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    written = 0
    for table, rows in grouped.items():
        clickhouse_db.insert_goal_rows(table_name=table, rows=rows)
        written += len(rows)

    print(f"[staging] ad_hourly: raw={len(raw)}, written={written}")
    return len(raw), written


def _load_daily_ad(
    customer_id: str,
    date_since: str,
    date_until: str,
    budget: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    raw = clickhouse_db.read_raw(
        query_name="ad_group_ad_daily",
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    grouped: dict[str, list[list[Any]]] = {
        table: [] for table in clickhouse_db.DAILY_TABLES
    }

    for item in raw:
        table, formatted = gads.build_ad_group_ad_daily_clickhouse_row(
            _W(item),
            campaign_budget_info_by_campaign=budget,
        )
        grouped.setdefault(table, []).append(formatted)

    clickhouse_db.delete_daily_tables_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    written = 0
    for table, rows in grouped.items():
        clickhouse_db.insert_daily_rows(table_name=table, rows=rows)
        written += len(rows)

    print(
        f"[staging] ad_group_ad_daily: "
        f"raw={len(raw)}, written={written}"
    )
    return len(raw), written


def _load_geo(
    customer_id: str,
    date_since: str,
    date_until: str,
    budget: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    raw = clickhouse_db.read_raw(
        query_name="geo_daily",
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    criterion_ids: set[str] = set()
    for item in raw:
        w = _W(item)

        country = w.geographic_view.country_criterion_id
        if country:
            criterion_ids.add(str(country))

        region = gads.resource_name_to_id(w.segments.geo_target_region)
        city = gads.resource_name_to_id(w.segments.geo_target_city)

        if region:
            criterion_ids.add(region)
        if city:
            criterion_ids.add(city)

    geo_constants_map = gads.get_geo_target_constants_map(
        customer_id=customer_id,
        criterion_ids=criterion_ids,
    )
    targeted_locations_by_campaign = (
        gads.fetch_targeted_locations_by_campaign(
            customer_id=customer_id
        )
    )

    rows = [
        gads.build_geo_daily_clickhouse_row(
            _W(item),
            geo_constants_map=geo_constants_map,
            targeted_locations_by_campaign=(
                targeted_locations_by_campaign
            ),
            campaign_budget_info_by_campaign=budget,
        )
        for item in raw
    ]

    clickhouse_db.delete_daily_geo_table_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )
    clickhouse_db.insert_daily_geo_rows(rows=rows)

    print(f"[staging] geo_daily: raw={len(raw)}, written={len(rows)}")
    return len(raw), len(rows)


def _load_daily_campaign(
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[int, int]:
    raw = clickhouse_db.read_raw(
        query_name="daily_campaign",
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    grouped: dict[str, list[list[Any]]] = {
        table: [] for table in clickhouse_db.DAILY_CAMPAIGN_TABLES
    }

    for item in raw:
        table, formatted = gads.build_daily_campaign_clickhouse_row(
            _W(item),
        )
        grouped.setdefault(table, []).append(formatted)

    clickhouse_db.delete_daily_campaign_tables_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    written = 0
    for table, rows in grouped.items():
        clickhouse_db.insert_daily_campaign_rows(
            table_name=table, rows=rows
        )
        written += len(rows)

    print(
        f"[staging] daily_campaign: "
        f"raw={len(raw)}, written={written}"
    )
    return len(raw), written


def _load_search_term(
    customer_id: str,
    date_since: str,
    date_until: str,
    budget: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    raw = clickhouse_db.read_raw(
        query_name="daily_search_term",
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    rows = [
        gads.build_daily_search_term_clickhouse_row(
            _W(item),
            campaign_budget_info_by_campaign=budget,
        )
        for item in raw
    ]

    clickhouse_db.delete_daily_search_term_table_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )
    clickhouse_db.insert_daily_search_term_rows(rows=rows)

    print(
        f"[staging] daily_search_term: "
        f"raw={len(raw)}, written={len(rows)}"
    )
    return len(raw), len(rows)


def _load_gender(
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[int, int]:
    raw = clickhouse_db.read_raw(
        query_name="gender_daily",
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    rows = [
        gads.build_gender_daily_clickhouse_row(_W(item))
        for item in raw
    ]

    clickhouse_db.delete_gender_daily_table_for_period(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )
    clickhouse_db.insert_gender_daily_rows(rows=rows)

    print(
        f"[staging] gender_daily: "
        f"raw={len(raw)}, written={len(rows)}"
    )
    return len(raw), len(rows)


def _load_creatives(customer_id: str) -> tuple[int, int]:
    raw = clickhouse_db.read_raw(
        query_name="creative_assets",
        customer_id=customer_id,
    )

    rows: list[list[Any]] = []
    video_items: list[_W] = []
    video_asset_ids: set[str] = set()

    for item in raw:
        query_name = item.get("asset_query_name")
        w = _W(item.get("row", {}))

        if query_name == "ad_group_ad_assets":
            rows.append(
                gads.build_ad_group_ad_asset_clickhouse_row(w)
            )
        elif query_name == "asset_group_assets":
            rows.append(
                gads.build_asset_group_asset_clickhouse_row(w)
            )
        elif query_name == "direct_image_ads":
            rows.append(
                gads.build_direct_image_ad_clickhouse_row(w)
            )
        elif query_name == "direct_video_responsive_ads":
            video_items.append(w)
            videos = w.ad_group_ad.ad.video_responsive_ad.videos
            for video in videos:
                asset_id = gads.resource_name_to_id(video.asset)
                if asset_id:
                    video_asset_ids.add(str(asset_id))

    youtube_video_assets_by_id = gads.get_youtube_video_assets_by_ids(
        customer_id=customer_id,
        asset_ids=video_asset_ids,
    )

    for w in video_items:
        rows.extend(
            gads.build_direct_video_responsive_ad_clickhouse_rows(
                w,
                youtube_video_assets_by_id,
            )
        )

    clickhouse_db.delete_creative_assets_for_customer(
        customer_id=customer_id,
    )
    clickhouse_db.insert_creative_asset_rows(rows=rows)

    print(
        f"[staging] creative_assets: "
        f"raw={len(raw)}, written={len(rows)}"
    )
    return len(raw), len(rows)


def load_creatives_from_raw(*, customer_id: str) -> int:
    """Creative-only режим: raw → staging без логирования шага."""
    _, written = _load_creatives(customer_id)
    return written


# ------------------------------------------------------------
# Точка входа: raw → staging
# ------------------------------------------------------------

def run_raw_to_staging(
    *,
    run_id: str,
    customer_id: str,
    date_since: str,
    date_until: str,
    load_creatives: bool = True,
) -> int:
    total_input = 0
    total_output = 0

    with etl_logger.etl_step(
        run_id=run_id,
        step_name="raw_to_staging",
        step_order=2,
        target_database=STAGING_DB,
    ) as step:

        # Размерное обогащение бюджетами — кэшируется в
        # google_ads_api на customer на время запуска.
        budget = gads.fetch_campaign_budget_info_by_campaign(
            customer_id=customer_id
        )

        sources = [
            _load_hourly(customer_id, date_since, date_until),
            _load_daily_ad(
                customer_id, date_since, date_until, budget
            ),
            _load_geo(
                customer_id, date_since, date_until, budget
            ),
            _load_daily_campaign(
                customer_id, date_since, date_until
            ),
            _load_search_term(
                customer_id, date_since, date_until, budget
            ),
            _load_gender(customer_id, date_since, date_until),
        ]

        if load_creatives:
            sources.append(_load_creatives(customer_id))

        for raw_count, written_count in sources:
            total_input += raw_count
            total_output += written_count

        step["input_rows"] = total_input
        step["output_rows"] = total_output

    return total_output
