import json
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.json_format import MessageToDict

import clickhouse_db
import config
import queries


ALMATY_TZ = ZoneInfo("Asia/Almaty")
_SUFFIX = "_staging"
_HOURLY = "_hourly_campaign_level"

SALES_HOURLY_TABLE = (
    "google_ads_sales_hourly_campaign_level_staging"
)
LEADS_HOURLY_TABLE = (
    "google_ads_leads_hourly_campaign_level_staging"
)
WEBSITE_TRAFFIC_HOURLY_TABLE = (
    "google_ads_website_traffic_hourly_campaign_level_staging"
)
APP_PROMOTION_HOURLY_TABLE = (
    "google_ads_app_promotion_hourly_campaign_level_staging"
)
YOUTUBE_REACH_VIEWS_ENGAGEMENT_HOURLY_TABLE = (
    "google_ads_youtube_reach_views_engagement"
    "_hourly_campaign_level_staging"
)
STORE_VISITS_PROMOTIONS_HOURLY_TABLE = (
    "google_ads_store_visits_promotions_hourly_campaign_level_staging"
)
NO_GOAL_HOURLY_TABLE = (
    "google_ads_no_goal_hourly_campaign_level_staging"
)


GOAL_PRIORITY = {
    NO_GOAL_HOURLY_TABLE: 0,
    WEBSITE_TRAFFIC_HOURLY_TABLE: 10,
    STORE_VISITS_PROMOTIONS_HOURLY_TABLE: 20,
    APP_PROMOTION_HOURLY_TABLE: 30,
    LEADS_HOURLY_TABLE: 40,
    SALES_HOURLY_TABLE: 50,
    YOUTUBE_REACH_VIEWS_ENGAGEMENT_HOURLY_TABLE: 60,
}


_CAMPAIGN_GOAL_HINTS_CACHE: dict[
    tuple[str, str, str],
    dict[str, dict[str, Any]],
] = {}


def get_client() -> GoogleAdsClient:
    credentials = {
        "developer_token": config.GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": config.GOOGLE_ADS_CLIENT_ID,
        "client_secret": config.GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": config.GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }

    if config.GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        credentials["login_customer_id"] = int(
            config.GOOGLE_ADS_LOGIN_CUSTOMER_ID
        )

    return GoogleAdsClient.load_from_dict(credentials)


def micros_to_money(value: int | float | None) -> float | None:
    if value is None:
        return None

    try:
        return float(value) / 1_000_000
    except (TypeError, ValueError):
        return None


def safe_divide(
    numerator: float | int | None,
    denominator: float | int | None,
) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None

    try:
        return float(numerator) / float(denominator)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def enum_name(value: Any) -> str | None:
    if value is None:
        return None

    name = getattr(value, "name", None)

    if name:
        return name

    return str(value)


def enum_list_to_names(values: Any) -> list[str]:
    names: list[str] = []

    for value in values:
        name = enum_name(value)

        if name:
            names.append(name)

    return names


def detect_budget_limited(primary_status_reasons: list[str]) -> bool:
    text = " ".join(primary_status_reasons).upper()

    return "BUDGET" in text or "LIMITED" in text


def resource_name_to_id(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value)

    if not text:
        return None

    if "/" in text:
        return text.split("/")[-1]

    return text


def first_or_none(values: Any) -> str | None:
    if values is None:
        return None

    try:
        items = list(values)
    except TypeError:
        return None

    if not items:
        return None

    first_value = str(items[0]).strip()

    return first_value or None


def none_if_empty(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    return text


def positive_int_or_none(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None

    if number <= 0:
        return None

    return number


def enum_name_or_none(value: Any) -> str | None:
    name = enum_name(value)

    if not name or name == "UNSPECIFIED":
        return None

    return name


def youtube_url_from_id(youtube_video_id: str | None) -> str | None:
    if not youtube_video_id:
        return None

    return f"https://www.youtube.com/watch?v={youtube_video_id}"


def get_hourly_datetime_range(row: Any) -> tuple[datetime, datetime]:
    row_date = date.fromisoformat(str(row.segments.date))
    row_hour = int(row.segments.hour)

    date_start = datetime.combine(
        row_date,
        time(hour=row_hour),
        tzinfo=ALMATY_TZ,
    )

    date_stop = date_start + timedelta(hours=1)

    return date_start, date_stop


def get_daily_datetime_range(row: Any) -> tuple[datetime, datetime]:
    row_date = date.fromisoformat(str(row.segments.date))

    date_start = datetime.combine(
        row_date,
        time(hour=0),
        tzinfo=ALMATY_TZ,
    )

    date_stop = date_start + timedelta(days=1)

    return date_start, date_stop


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def row_has_field(row: Any, path: str) -> bool:
    current = row

    for item in path.split("."):
        if not hasattr(current, item):
            return False

        current = getattr(current, item)

    return True


def get_row_text_for_goal_detection(row: Any) -> str:
    parts = [
        enum_name(row.campaign.advertising_channel_type) or "",
        enum_name(row.campaign.advertising_channel_sub_type) or "",
        enum_name(row.campaign.bidding_strategy_type) or "",
        str(row.campaign.name or ""),
    ]

    if row_has_field(row, "segments.ad_network_type"):
        parts.append(enum_name(row.segments.ad_network_type) or "")

    if row_has_field(row, "ad_group_ad.ad.type"):
        parts.append(enum_name(row.ad_group_ad.ad.type) or "")

    return " ".join(parts).lower()


def has_youtube_or_video_signal(row: Any) -> bool:
    text = get_row_text_for_goal_detection(row)

    youtube_words = (
        "youtube",
        "video",
        "видео",
        "просмотр",
        "просмотры",
        "охват",
        "engagement",
        "взаимодейств",
        "video_responsive_ad",
        "youtube_watch",
        "youtube_search",
    )

    if any(word in text for word in youtube_words):
        return True

    if row_has_field(row, "metrics.video_trueview_views"):
        if safe_int(row.metrics.video_trueview_views) > 0:
            return True

    if row_has_field(row, "metrics.video_trueview_view_rate"):
        if safe_float(row.metrics.video_trueview_view_rate) > 0:
            return True

    if row_has_field(row, "metrics.video_quartile_p25_rate"):
        if safe_float(row.metrics.video_quartile_p25_rate) > 0:
            return True

    if row_has_field(row, "metrics.video_quartile_p50_rate"):
        if safe_float(row.metrics.video_quartile_p50_rate) > 0:
            return True

    if row_has_field(row, "metrics.video_quartile_p75_rate"):
        if safe_float(row.metrics.video_quartile_p75_rate) > 0:
            return True

    if row_has_field(row, "metrics.video_quartile_p100_rate"):
        if safe_float(row.metrics.video_quartile_p100_rate) > 0:
            return True

    if row_has_field(row, "metrics.coviewed_impressions"):
        if safe_int(row.metrics.coviewed_impressions) > 0:
            return True

    if row_has_field(row, "metrics.primary_impressions"):
        if safe_int(row.metrics.primary_impressions) > 0:
            return True

    if row_has_field(row, "metrics.unique_users"):
        reach = safe_int(row.metrics.unique_users)
        frequency = (
            safe_float(row.metrics.average_impression_frequency_per_user)
            if row_has_field(
                row,
                "metrics.average_impression_frequency_per_user",
            )
            else 0.0
        )

        if reach > 0 and frequency > 0:
            return True

    return False


def detect_target_table_from_row(row: Any) -> str:
    channel_type = enum_name(row.campaign.advertising_channel_type) or ""

    text = get_row_text_for_goal_detection(row)

    # 1. YouTube / video / reach / engagement.
    # Ставим первым, потому что часть YouTube-кампаний может приходить как DISPLAY + UNSPECIFIED.
    if has_youtube_or_video_signal(row):
        return YOUTUBE_REACH_VIEWS_ENGAGEMENT_HOURLY_TABLE

    # 2. App promotion.
    if "app" in text or "прилож" in text:
        return APP_PROMOTION_HOURLY_TABLE

    # 3. Leads.
    if (
        "lead" in text
        or "лид" in text
        or "заявк" in text
        or "форма" in text
    ):
        return LEADS_HOURLY_TABLE

    # 4. Sales.
    if (
        "sale" in text
        or "sales" in text
        or "продаж" in text
        or "purchase" in text
        or "покуп" in text
        or "shop" in text
    ):
        return SALES_HOURLY_TABLE

    # 5. Website traffic.
    if (
        "traffic" in text
        or "трафик" in text
        or "site" in text
        or "website" in text
        or "сайт" in text
        or channel_type == "SEARCH"
    ):
        return WEBSITE_TRAFFIC_HOURLY_TABLE

    # 6. Store visits / promotions.
    if (
        "store" in text
        or "магазин" in text
        or "visit" in text
        or "promotion" in text
        or "promo" in text
        or "промо" in text
    ):
        return STORE_VISITS_PROMOTIONS_HOURLY_TABLE

    # 7. Display без video/reach/engagement-сигналов считаем display traffic.
    if channel_type == "DISPLAY":
        return WEBSITE_TRAFFIC_HOURLY_TABLE

    return NO_GOAL_HOURLY_TABLE


def get_target_table(
    row: Any,
    *,
    campaign_goal_hints_by_campaign: dict[str, dict[str, Any]] | None = None,
) -> str:
    campaign_id = str(row.campaign.id)

    if campaign_goal_hints_by_campaign:
        hint = campaign_goal_hints_by_campaign.get(campaign_id)

        if hint and hint.get("table_name"):
            return str(hint["table_name"])

    return detect_target_table_from_row(row)


def get_daily_target_table(
    row,
    *,
    campaign_goal_hints_by_campaign=None,
):
    hourly_table = get_target_table(
        row,
        campaign_goal_hints_by_campaign=(
            campaign_goal_hints_by_campaign
        ),
    )
    return hourly_table.replace(
        "_hourly_campaign_level_staging",
        "_daily_ad_level_staging",
    )


def get_daily_geo_target_table(
    row,
    *,
    campaign_goal_hints_by_campaign=None,
):
    hourly_table = get_target_table(
        row,
        campaign_goal_hints_by_campaign=(
            campaign_goal_hints_by_campaign
        ),
    )
    return hourly_table.replace(
        "_hourly_campaign_level_staging",
        "_geo_daily_region_level_staging",
    )


def get_daily_campaign_target_table(
    row,
    *,
    campaign_goal_hints_by_campaign=None,
):
    hourly_table = get_target_table(
        row,
        campaign_goal_hints_by_campaign=(
            campaign_goal_hints_by_campaign
        ),
    )
    return hourly_table.replace(
        "_hourly_campaign_level_staging",
        "_daily_campaign_level_staging",
    )


def get_google_ads_goal_type(table_name: str) -> str:
    _h = _HOURLY + _SUFFIX
    mapping = {
        "google_ads_sales" + _h: "sales",
        "google_ads_leads" + _h: "leads",
        "google_ads_website_traffic" + _h: "website_traffic",
        "google_ads_app_promotion" + _h: "app_promotion",
        "google_ads_youtube_reach_views_engagement"
        + _h: "youtube_reach_views_engagement",
        "google_ads_store_visits_promotions"
        + _h: "store_visits_promotions",
        "google_ads_no_goal" + _h: "no_goal",
    }
    hourly_table_name = (
        table_name
        .replace("_geo_daily_region_level" + _SUFFIX, _h)
        .replace("_daily_campaign_level" + _SUFFIX, _h)
        .replace("_daily_ad_level" + _SUFFIX, _h)
    )
    return mapping.get(hourly_table_name, "no_goal")


def row_to_raw_dict(row: Any) -> dict[str, Any]:
    try:
        return MessageToDict(
            row._pb,
            preserving_proto_field_name=True,
            use_integers_for_enums=False,
        )
    except Exception:
        return {"row": str(row)}


def get_geo_target_constants_map(
    *,
    customer_id: str,
    criterion_ids: set[str],
) -> dict[str, dict[str, Any]]:
    ids = sorted(
        {
            str(item)
            for item in criterion_ids
            if item and str(item).isdigit()
        }
    )

    if not ids:
        return {}

    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    ids_text = ", ".join(ids)

    query = queries.GEO_TARGET_CONSTANT_QUERY_TEMPLATE.format(
        criterion_ids=ids_text
    )

    result: dict[str, dict[str, Any]] = {}

    try:
        response = google_ads_service.search(
            customer_id=customer_id,
            query=query,
        )

        for row in response:
            geo = row.geo_target_constant
            geo_id = str(geo.id)

            result[geo_id] = {
                "id": geo_id,
                "name": geo.name or None,
                "country_code": geo.country_code or None,
                "target_type": geo.target_type or None,
                "status": enum_name(geo.status),
                "parent_geo_target": (
                    resource_name_to_id(geo.parent_geo_target)
                    if geo.parent_geo_target
                    else None
                ),
            }

    except GoogleAdsException as ex:
        print("Google Ads geo target constants request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

    return result


def fetch_targeted_locations_by_campaign(
    *,
    customer_id: str,
) -> dict[str, str]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    raw_items: list[dict[str, Any]] = []
    location_ids: set[str] = set()

    try:
        response = google_ads_service.search(
            customer_id=customer_id,
            query=queries.TARGETED_LOCATIONS_QUERY,
        )

        for row in response:
            campaign_id = str(row.campaign.id)
            resource_name = (
                row.campaign_criterion.location.geo_target_constant
            )
            criterion_id = resource_name_to_id(resource_name)

            if criterion_id:
                location_ids.add(criterion_id)

            raw_items.append(
                {
                    "campaign_id": campaign_id,
                    "campaign_name": row.campaign.name,
                    "criterion_id": criterion_id,
                    "resource_name": str(resource_name)
                    if resource_name
                    else None,
                    "campaign_criterion_type": enum_name(
                        row.campaign_criterion.type
                    ),
                    "campaign_criterion_status": enum_name(
                        row.campaign_criterion.status
                    ),
                    "negative": bool(row.campaign_criterion.negative),
                }
            )

    except GoogleAdsException as ex:
        print("Google Ads targeted locations request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        return {}

    constants_map = get_geo_target_constants_map(
        customer_id=customer_id,
        criterion_ids=location_ids,
    )

    grouped: dict[str, list[dict[str, Any]]] = {}

    for item in raw_items:
        campaign_id = item["campaign_id"]
        criterion_id = item["criterion_id"]

        geo_info = constants_map.get(str(criterion_id), {})

        enriched_item = {
            **item,
            "name": geo_info.get("name"),
            "country_code": geo_info.get("country_code"),
            "target_type": geo_info.get("target_type"),
            "geo_status": geo_info.get("status"),
            "parent_geo_target": geo_info.get("parent_geo_target"),
        }

        grouped.setdefault(campaign_id, []).append(enriched_item)

    result: dict[str, str] = {}

    for campaign_id, items in grouped.items():
        result[campaign_id] = json.dumps(
            items,
            ensure_ascii=False,
            default=str,
        )

    return result


def fetch_campaign_budget_info_by_campaign(
    *,
    customer_id: str,
) -> dict[str, dict[str, Any]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    result: dict[str, dict[str, Any]] = {}

    try:
        response = google_ads_service.search(
            customer_id=customer_id,
            query=queries.CAMPAIGN_BUDGET_INFO_QUERY,
        )

        for row in response:
            campaign_id = str(row.campaign.id)

            primary_status_reasons = enum_list_to_names(
                row.campaign.primary_status_reasons
            )

            primary_status_reasons_json = (
                json.dumps(primary_status_reasons, ensure_ascii=False)
                if primary_status_reasons
                else None
            )

            budget_period = enum_name(row.campaign_budget.period)
            budget_amount = micros_to_money(
                row.campaign_budget.amount_micros
            )
            budget_total_amount = micros_to_money(
                row.campaign_budget.total_amount_micros
            )

            daily_budget = None
            lifetime_budget = None

            if budget_period == "DAILY":
                daily_budget = budget_amount
            else:
                lifetime_budget = budget_total_amount or budget_amount

            result[campaign_id] = {
                "campaign_primary_status_reasons_json": (
                    primary_status_reasons_json
                ),
                "budget_id": str(row.campaign_budget.id)
                    if row.campaign_budget.id
                    else None,
                "budget_name": row.campaign_budget.name or None,
                "budget_period": budget_period,
                "daily_budget": daily_budget,
                "lifetime_budget": lifetime_budget,
                "is_budget_limited": detect_budget_limited(
                    primary_status_reasons
                ),
                "bidding_strategy_type": enum_name(
                    row.campaign.bidding_strategy_type
                ),
                "optimization_score": float(row.campaign.optimization_score),
            }

    except GoogleAdsException as ex:
        print("Google Ads campaign budget info request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        return {}

    return result


def fetch_campaign_goal_hints_by_campaign(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> dict[str, dict[str, Any]]:
    """
    Строит единый campaign_id -> goal mapping на уровне кампании.

    Зачем:
    geo_daily / ad / hourly / search_term строки не всегда содержат все сигналы цели.
    Например кампания может быть DISPLAY + UNSPECIFIED, но по daily_campaign
    иметь reach/frequency и относиться к reach/views/engagement.
    """
    cache_key = (customer_id, date_since, date_until)

    if cache_key in _CAMPAIGN_GOAL_HINTS_CACHE:
        return _CAMPAIGN_GOAL_HINTS_CACHE[cache_key]

    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    query = queries.DAILY_CAMPAIGN_QUERY.format(
        date_since=date_since,
        date_until=date_until,
    )

    result: dict[str, dict[str, Any]] = {}

    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=query,
        )

        for batch in stream:
            for row in batch.results:
                campaign_id = str(row.campaign.id)
                table_name = detect_target_table_from_row(row)

                old_hint = result.get(campaign_id)
                old_priority = (
                    GOAL_PRIORITY.get(str(old_hint["table_name"]), 0)
                    if old_hint
                    else -1
                )
                new_priority = GOAL_PRIORITY.get(table_name, 0)

                if old_hint is None or new_priority >= old_priority:
                    result[campaign_id] = {
                        "campaign_id": campaign_id,
                        "campaign_name": row.campaign.name,
                        "table_name": table_name,
                        "goal_type": get_google_ads_goal_type(table_name),
                        "advertising_channel_type": enum_name(
                            row.campaign.advertising_channel_type
                        ),
                        "advertising_channel_sub_type": enum_name(
                            row.campaign.advertising_channel_sub_type
                        ),
                        "bidding_strategy_type": enum_name(
                            row.campaign.bidding_strategy_type
                        ),
                    }

    except GoogleAdsException as ex:
        print("Google Ads campaign goal hints request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        _CAMPAIGN_GOAL_HINTS_CACHE[cache_key] = {}
        return {}

    _CAMPAIGN_GOAL_HINTS_CACHE[cache_key] = result

    return result


def build_clickhouse_row(
    row: Any,
    *,
    campaign_goal_hints_by_campaign: dict[str, dict[str, Any]] | None = None,
) -> tuple[str, list[Any]]:
    date_start, date_stop = get_hourly_datetime_range(row)

    table_name = get_target_table(
        row,
        campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
    )

    # Micros -> money. Это не бизнес-расчет метрики,
    # а техническая конвертация из micros в нормальную валюту.
    spend = micros_to_money(row.metrics.cost_micros)
    average_cpc = micros_to_money(row.metrics.average_cpc)
    average_cpm = micros_to_money(row.metrics.average_cpm)
    average_cpv = micros_to_money(row.metrics.trueview_average_cpv)

    average_cost = micros_to_money(row.metrics.average_cost)
    average_cpe = micros_to_money(row.metrics.average_cpe)

    active_view_cpm = micros_to_money(row.metrics.active_view_cpm)
    active_view_measurable_cost = micros_to_money(
        row.metrics.active_view_measurable_cost_micros
    )

    cost_per_conversion = micros_to_money(row.metrics.cost_per_conversion)
    cost_per_all_conversions = micros_to_money(
        row.metrics.cost_per_all_conversions
    )

    budget_period = enum_name(row.campaign_budget.period)
    budget_amount = micros_to_money(row.campaign_budget.amount_micros)
    budget_total_amount = micros_to_money(
        row.campaign_budget.total_amount_micros
    )

    daily_budget = None
    lifetime_budget = None

    if budget_period == "DAILY":
        daily_budget = budget_amount
    else:
        lifetime_budget = budget_total_amount or budget_amount

    primary_status_reasons = enum_list_to_names(
        row.campaign.primary_status_reasons
    )

    primary_status_reasons_json = (
        json.dumps(primary_status_reasons, ensure_ascii=False)
        if primary_status_reasons
        else None
    )

    is_budget_limited = detect_budget_limited(primary_status_reasons)

    row_data = {
        "date_start": date_start,
        "date_stop": date_stop,

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "campaign_primary_status": enum_name(row.campaign.primary_status),
        "campaign_primary_status_reasons_json": primary_status_reasons_json,
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),
        "google_ads_goal_type": get_google_ads_goal_type(table_name),

        "device": enum_name(row.segments.device),

        "ad_network_type": enum_name(row.segments.ad_network_type),

        "budget_id": str(row.campaign_budget.id)
            if row.campaign_budget.id
            else None,
        "budget_name": row.campaign_budget.name or None,
        "budget_period": budget_period,
        "daily_budget": daily_budget,
        "lifetime_budget": lifetime_budget,
        "is_budget_limited": is_budget_limited,

        "bidding_strategy_type": enum_name(
            row.campaign.bidding_strategy_type
        ),
        "optimization_score": float(row.campaign.optimization_score),

        "impressions": int(row.metrics.impressions),
        "clicks": int(row.metrics.clicks),
        "ctr": float(row.metrics.ctr),

        "spend": spend,

        "average_cpc": average_cpc,
        "average_cpm": average_cpm,
        "average_cpv": average_cpv,
        "average_cost": average_cost,
        "average_cpe": average_cpe,

        "interactions": int(row.metrics.interactions),
        "interaction_rate": float(row.metrics.interaction_rate),

        "engagements": int(row.metrics.engagements),
        "engagement_rate": float(row.metrics.engagement_rate),

        "video_views": int(row.metrics.video_trueview_views),
        "view_rate": float(row.metrics.video_trueview_view_rate),

        "video_quartile_p25_rate": float(
            row.metrics.video_quartile_p25_rate
        ),
        "video_quartile_p50_rate": float(
            row.metrics.video_quartile_p50_rate
        ),
        "video_quartile_p75_rate": float(
            row.metrics.video_quartile_p75_rate
        ),
        "video_quartile_p100_rate": float(
            row.metrics.video_quartile_p100_rate
        ),

        "absolute_top_impression_percentage": float(
            row.metrics.absolute_top_impression_percentage
        ),
        "top_impression_percentage": float(
            row.metrics.top_impression_percentage
        ),

        "search_impression_share": float(
            row.metrics.search_impression_share
        ),
        "search_absolute_top_impression_share": float(
            row.metrics.search_absolute_top_impression_share
        ),
        "search_top_impression_share": float(
            row.metrics.search_top_impression_share
        ),

        "search_budget_lost_impression_share": float(
            row.metrics.search_budget_lost_impression_share
        ),
        "search_budget_lost_absolute_top_impression_share": float(
            row.metrics.search_budget_lost_absolute_top_impression_share
        ),
        "search_budget_lost_top_impression_share": float(
            row.metrics.search_budget_lost_top_impression_share
        ),

        "search_rank_lost_impression_share": float(
            row.metrics.search_rank_lost_impression_share
        ),
        "search_rank_lost_absolute_top_impression_share": float(
            row.metrics.search_rank_lost_absolute_top_impression_share
        ),
        "search_rank_lost_top_impression_share": float(
            row.metrics.search_rank_lost_top_impression_share
        ),

        "content_impression_share": float(
            row.metrics.content_impression_share
        ),

        "active_view_viewable_impressions": int(
            row.metrics.active_view_impressions
        ),
        "active_view_viewability": float(
            row.metrics.active_view_viewability
        ),

        "active_view_cpm": active_view_cpm,
        "active_view_ctr": float(row.metrics.active_view_ctr),
        "active_view_measurability": float(
            row.metrics.active_view_measurability
        ),
        "active_view_measurable_cost": active_view_measurable_cost,
        "active_view_measurable_impressions": int(
            row.metrics.active_view_measurable_impressions
        ),

        "active_view_audibility_measurable_impressions": int(
            row.metrics.active_view_audibility_measurable_impressions
        ),
        "active_view_audibility_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_measurable_impressions_rate
        ),

        "active_view_audibility_invalid_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_invalid_measurable_impressions_rate
        ),
        "active_view_audibility_invalid_givt_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_invalid_givt_measurable_impressions_rate
        ),

        "active_view_audible_impressions": int(
            row.metrics.active_view_audible_impressions
        ),
        "active_view_audible_impressions_rate": float(
            row.metrics.active_view_audible_impressions_rate
        ),

        "active_view_audible_quartile_p25_rate": float(
            row.metrics.active_view_audible_quartile_p25_rate
        ),
        "active_view_audible_quartile_p50_rate": float(
            row.metrics.active_view_audible_quartile_p50_rate
        ),
        "active_view_audible_quartile_p75_rate": float(
            row.metrics.active_view_audible_quartile_p75_rate
        ),
        "active_view_audible_quartile_p100_rate": float(
            row.metrics.active_view_audible_quartile_p100_rate
        ),

        "active_view_audible_two_seconds_impressions": int(
            row.metrics.active_view_audible_two_seconds_impressions
        ),
        "active_view_audible_two_seconds_impressions_rate": float(
            row.metrics.active_view_audible_two_seconds_impressions_rate
        ),

        "active_view_audible_thirty_seconds_impressions": int(
            row.metrics.active_view_audible_thirty_seconds_impressions
        ),
        "active_view_audible_thirty_seconds_impressions_rate": float(
            row.metrics.active_view_audible_thirty_seconds_impressions_rate
        ),

        "conversions": float(row.metrics.conversions),
        "conversion_rate": float(
            row.metrics.conversions_from_interactions_rate
        ),
        "cost_per_conversion": cost_per_conversion,

        "conversions_value": float(row.metrics.conversions_value),

        "all_conversions": float(row.metrics.all_conversions),
        "all_conversions_value": float(row.metrics.all_conversions_value),

        "conversions_by_conversion_date": float(
            row.metrics.conversions_by_conversion_date
        ),
        "conversions_value_by_conversion_date": float(
            row.metrics.conversions_value_by_conversion_date
        ),

        "all_conversions_by_conversion_date": float(
            row.metrics.all_conversions_by_conversion_date
        ),
        "all_conversions_value_by_conversion_date": float(
            row.metrics.all_conversions_value_by_conversion_date
        ),

        "all_conversion_rate": float(
            row.metrics.all_conversions_from_interactions_rate
        ),
        "cost_per_all_conversions": cost_per_all_conversions,

        "value_per_conversion": float(row.metrics.value_per_conversion),
        "value_per_conversions_by_conversion_date": float(
            row.metrics.value_per_conversions_by_conversion_date
        ),

        "value_per_all_conversions": float(
            row.metrics.value_per_all_conversions
        ),
        "value_per_all_conversions_by_conversion_date": float(
            row.metrics.value_per_all_conversions_by_conversion_date
        ),

        "view_through_conversions": int(
            row.metrics.view_through_conversions
        ),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return table_name, [
        row_data[column]
        for column in clickhouse_db.HOURLY_TABLE_COLUMNS
    ]


def build_ad_group_ad_daily_clickhouse_row(
    row: Any,
    *,
    campaign_budget_info_by_campaign: dict[str, dict[str, Any]],
    campaign_goal_hints_by_campaign: dict[str, dict[str, Any]] | None = None,
) -> tuple[str, list[Any]]:
    date_start, date_stop = get_daily_datetime_range(row)

    table_name = get_daily_target_table(
        row,
        campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
    )

    # Micros -> money. Это не бизнес-расчет метрики,
    # а техническая конвертация из micros в нормальную валюту.
    spend = micros_to_money(row.metrics.cost_micros)
    average_cpc = micros_to_money(row.metrics.average_cpc)
    average_cpm = micros_to_money(row.metrics.average_cpm)
    average_cpv = micros_to_money(row.metrics.trueview_average_cpv)

    average_cost = micros_to_money(row.metrics.average_cost)
    average_cpe = micros_to_money(row.metrics.average_cpe)

    active_view_cpm = micros_to_money(row.metrics.active_view_cpm)
    active_view_measurable_cost = micros_to_money(
        row.metrics.active_view_measurable_cost_micros
    )

    cost_per_conversion = micros_to_money(row.metrics.cost_per_conversion)
    cost_per_all_conversions = micros_to_money(
        row.metrics.cost_per_all_conversions
    )

    cost_per_current_model_attributed_conversion = micros_to_money(
        row.metrics.cost_per_current_model_attributed_conversion
    )

    cost_converted_currency_per_platform_comparable_conversion = (
        micros_to_money(
            row.metrics.cost_converted_currency_per_platform_comparable_conversion
        )
    )
    cost_per_platform_comparable_conversion = micros_to_money(
        row.metrics.cost_per_platform_comparable_conversion
    )

    revenue = micros_to_money(row.metrics.revenue_micros)
    average_order_value = micros_to_money(
        row.metrics.average_order_value_micros
    )
    cost_of_goods_sold = micros_to_money(
        row.metrics.cost_of_goods_sold_micros
    )
    gross_profit = micros_to_money(row.metrics.gross_profit_micros)

    cross_sell_cost_of_goods_sold = micros_to_money(
        row.metrics.cross_sell_cost_of_goods_sold_micros
    )
    cross_sell_gross_profit = micros_to_money(
        row.metrics.cross_sell_gross_profit_micros
    )
    cross_sell_revenue = micros_to_money(
        row.metrics.cross_sell_revenue_micros
    )

    lead_cost_of_goods_sold = micros_to_money(
        row.metrics.lead_cost_of_goods_sold_micros
    )
    lead_gross_profit = micros_to_money(
        row.metrics.lead_gross_profit_micros
    )
    lead_revenue = micros_to_money(row.metrics.lead_revenue_micros)

    all_revenue = micros_to_money(row.metrics.all_revenue_micros)
    all_average_order_value = micros_to_money(
        row.metrics.all_average_order_value_micros
    )
    all_cost_of_goods_sold = micros_to_money(
        row.metrics.all_cost_of_goods_sold_micros
    )
    all_gross_profit = micros_to_money(
        row.metrics.all_gross_profit_micros
    )

    all_cross_sell_cost_of_goods_sold = micros_to_money(
        row.metrics.all_cross_sell_cost_of_goods_sold_micros
    )
    all_cross_sell_gross_profit = micros_to_money(
        row.metrics.all_cross_sell_gross_profit_micros
    )
    all_cross_sell_revenue = micros_to_money(
        row.metrics.all_cross_sell_revenue_micros
    )

    all_lead_cost_of_goods_sold = micros_to_money(
        row.metrics.all_lead_cost_of_goods_sold_micros
    )
    all_lead_gross_profit = micros_to_money(
        row.metrics.all_lead_gross_profit_micros
    )
    all_lead_revenue = micros_to_money(
        row.metrics.all_lead_revenue_micros
    )

    campaign_id = str(row.campaign.id)

    campaign_budget_info = campaign_budget_info_by_campaign.get(
        campaign_id,
        {},
    )

    primary_status_reasons = enum_list_to_names(
        row.campaign.primary_status_reasons
    )

    primary_status_reasons_json = (
        json.dumps(primary_status_reasons, ensure_ascii=False)
        if primary_status_reasons
        else campaign_budget_info.get("campaign_primary_status_reasons_json")
    )

    row_data = {
        "date_start": date_start,
        "date_stop": date_stop,

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": campaign_id,
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "campaign_primary_status": enum_name(row.campaign.primary_status),
        "campaign_primary_status_reasons_json": primary_status_reasons_json,
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),
        "google_ads_goal_type": get_google_ads_goal_type(table_name),

        "ad_group_id": str(row.ad_group.id),
        "ad_group_name": row.ad_group.name or None,
        "ad_group_status": enum_name(row.ad_group.status),
        "ad_group_type": enum_name(row.ad_group.type),

        "ad_id": str(row.ad_group_ad.ad.id),
        "ad_name": row.ad_group_ad.ad.name or None,
        "ad_type": enum_name(row.ad_group_ad.ad.type),
        "ad_status": enum_name(row.ad_group_ad.status),

        "landing_page_url": (
            first_or_none(row.ad_group_ad.ad.final_urls)
            or first_or_none(row.ad_group_ad.ad.final_mobile_urls)
        ),

        "device": enum_name(row.segments.device),

        "ad_network_type": enum_name(row.segments.ad_network_type),

        "budget_id": campaign_budget_info.get("budget_id"),
        "budget_name": campaign_budget_info.get("budget_name"),
        "budget_period": campaign_budget_info.get("budget_period"),
        "daily_budget": campaign_budget_info.get("daily_budget"),
        "lifetime_budget": campaign_budget_info.get("lifetime_budget"),
        "is_budget_limited": campaign_budget_info.get("is_budget_limited"),

        "bidding_strategy_type": enum_name(
            row.campaign.bidding_strategy_type
        ),
        "optimization_score": float(row.campaign.optimization_score),

        "impressions": int(row.metrics.impressions),
        "clicks": int(row.metrics.clicks),
        "ctr": float(row.metrics.ctr),

        "spend": spend,

        "average_cpc": average_cpc,
        "average_cpm": average_cpm,
        "average_cpv": average_cpv,
        "average_cost": average_cost,
        "average_cpe": average_cpe,

        "interactions": int(row.metrics.interactions),
        "interaction_rate": float(row.metrics.interaction_rate),

        "engagements": int(row.metrics.engagements),
        "engagement_rate": float(row.metrics.engagement_rate),

        "video_views": int(row.metrics.video_trueview_views),
        "view_rate": float(row.metrics.video_trueview_view_rate),

        "video_quartile_p25_rate": float(
            row.metrics.video_quartile_p25_rate
        ),
        "video_quartile_p50_rate": float(
            row.metrics.video_quartile_p50_rate
        ),
        "video_quartile_p75_rate": float(
            row.metrics.video_quartile_p75_rate
        ),
        "video_quartile_p100_rate": float(
            row.metrics.video_quartile_p100_rate
        ),

        "video_trueview_view_rate_in_feed": float(
            row.metrics.video_trueview_view_rate_in_feed
        ),
        "video_trueview_view_rate_in_stream": float(
            row.metrics.video_trueview_view_rate_in_stream
        ),
        "video_trueview_view_rate_shorts": float(
            row.metrics.video_trueview_view_rate_shorts
        ),

        "absolute_top_impression_percentage": float(
            row.metrics.absolute_top_impression_percentage
        ),
        "top_impression_percentage": float(
            row.metrics.top_impression_percentage
        ),

        "active_view_viewable_impressions": int(
            row.metrics.active_view_impressions
        ),
        "active_view_viewability": float(
            row.metrics.active_view_viewability
        ),

        "active_view_cpm": active_view_cpm,
        "active_view_ctr": float(row.metrics.active_view_ctr),
        "active_view_measurability": float(
            row.metrics.active_view_measurability
        ),
        "active_view_measurable_cost": active_view_measurable_cost,
        "active_view_measurable_impressions": int(
            row.metrics.active_view_measurable_impressions
        ),

        "active_view_audibility_measurable_impressions": int(
            row.metrics.active_view_audibility_measurable_impressions
        ),
        "active_view_audibility_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_measurable_impressions_rate
        ),

        "active_view_audibility_invalid_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_invalid_measurable_impressions_rate
        ),
        "active_view_audibility_invalid_givt_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_invalid_givt_measurable_impressions_rate
        ),

        "active_view_audible_impressions": int(
            row.metrics.active_view_audible_impressions
        ),
        "active_view_audible_impressions_rate": float(
            row.metrics.active_view_audible_impressions_rate
        ),

        "active_view_audible_quartile_p25_rate": float(
            row.metrics.active_view_audible_quartile_p25_rate
        ),
        "active_view_audible_quartile_p50_rate": float(
            row.metrics.active_view_audible_quartile_p50_rate
        ),
        "active_view_audible_quartile_p75_rate": float(
            row.metrics.active_view_audible_quartile_p75_rate
        ),
        "active_view_audible_quartile_p100_rate": float(
            row.metrics.active_view_audible_quartile_p100_rate
        ),

        "active_view_audible_two_seconds_impressions": int(
            row.metrics.active_view_audible_two_seconds_impressions
        ),
        "active_view_audible_two_seconds_impressions_rate": float(
            row.metrics.active_view_audible_two_seconds_impressions_rate
        ),

        "active_view_audible_thirty_seconds_impressions": int(
            row.metrics.active_view_audible_thirty_seconds_impressions
        ),
        "active_view_audible_thirty_seconds_impressions_rate": float(
            row.metrics.active_view_audible_thirty_seconds_impressions_rate
        ),

        "conversions": float(row.metrics.conversions),
        "conversion_rate": float(
            row.metrics.conversions_from_interactions_rate
        ),
        "cost_per_conversion": cost_per_conversion,

        "conversions_value": float(row.metrics.conversions_value),

        "all_conversions": float(row.metrics.all_conversions),
        "all_conversions_value": float(row.metrics.all_conversions_value),

        "all_conversion_rate": float(
            row.metrics.all_conversions_from_interactions_rate
        ),
        "cost_per_all_conversions": cost_per_all_conversions,

        "conversions_by_conversion_date": float(
            row.metrics.conversions_by_conversion_date
        ),
        "conversions_value_by_conversion_date": float(
            row.metrics.conversions_value_by_conversion_date
        ),

        "all_conversions_by_conversion_date": float(
            row.metrics.all_conversions_by_conversion_date
        ),
        "all_conversions_value_by_conversion_date": float(
            row.metrics.all_conversions_value_by_conversion_date
        ),

        "value_per_conversion": float(row.metrics.value_per_conversion),
        "value_per_conversions_by_conversion_date": float(
            row.metrics.value_per_conversions_by_conversion_date
        ),

        "value_per_all_conversions": float(
            row.metrics.value_per_all_conversions
        ),
        "value_per_all_conversions_by_conversion_date": float(
            row.metrics.value_per_all_conversions_by_conversion_date
        ),

        "view_through_conversions": int(
            row.metrics.view_through_conversions
        ),
        "cross_device_conversions": float(
            row.metrics.cross_device_conversions
        ),

        "current_model_attributed_conversions": float(
            row.metrics.current_model_attributed_conversions
        ),
        "current_model_attributed_conversions_value": float(
            row.metrics.current_model_attributed_conversions_value
        ),
        "cost_per_current_model_attributed_conversion": (
            cost_per_current_model_attributed_conversion
        ),
        "value_per_current_model_attributed_conversion": float(
            row.metrics.value_per_current_model_attributed_conversion
        ),

        "platform_comparable_conversions": float(
            row.metrics.platform_comparable_conversions
        ),
        "platform_comparable_conversions_by_conversion_date": float(
            row.metrics.platform_comparable_conversions_by_conversion_date
        ),
        "platform_comparable_conversions_from_interactions_rate": float(
            row.metrics.platform_comparable_conversions_from_interactions_rate
        ),
        "platform_comparable_conversions_from_interactions_value_per_interaction": float(
            row.metrics.platform_comparable_conversions_from_interactions_value_per_interaction
        ),
        "platform_comparable_conversions_value": float(
            row.metrics.platform_comparable_conversions_value
        ),
        "platform_comparable_conversions_value_by_conversion_date": float(
            row.metrics.platform_comparable_conversions_value_by_conversion_date
        ),
        "platform_comparable_conversions_value_per_cost": float(
            row.metrics.platform_comparable_conversions_value_per_cost
        ),

        "cost_converted_currency_per_platform_comparable_conversion": (
            cost_converted_currency_per_platform_comparable_conversion
        ),
        "cost_per_platform_comparable_conversion": (
            cost_per_platform_comparable_conversion
        ),
        "value_per_platform_comparable_conversion": float(
            row.metrics.value_per_platform_comparable_conversion
        ),
        "value_per_platform_comparable_conversions_by_conversion_date": float(
            row.metrics.value_per_platform_comparable_conversions_by_conversion_date
        ),

        "orders": float(row.metrics.orders),
        "revenue": revenue,
        "units_sold": float(row.metrics.units_sold),

        "average_cart_size": float(row.metrics.average_cart_size),
        "average_order_value": average_order_value,

        "cost_of_goods_sold": cost_of_goods_sold,
        "gross_profit": gross_profit,
        "gross_profit_margin": float(row.metrics.gross_profit_margin),

        "cross_sell_cost_of_goods_sold": cross_sell_cost_of_goods_sold,
        "cross_sell_gross_profit": cross_sell_gross_profit,
        "cross_sell_revenue": cross_sell_revenue,
        "cross_sell_units_sold": float(row.metrics.cross_sell_units_sold),

        "lead_cost_of_goods_sold": lead_cost_of_goods_sold,
        "lead_gross_profit": lead_gross_profit,
        "lead_revenue": lead_revenue,
        "lead_units_sold": float(row.metrics.lead_units_sold),

        "all_orders": float(row.metrics.all_orders),
        "all_revenue": all_revenue,
        "all_units_sold": float(row.metrics.all_units_sold),

        "all_average_cart_size": float(
            row.metrics.all_average_cart_size
        ),
        "all_average_order_value": all_average_order_value,

        "all_cost_of_goods_sold": all_cost_of_goods_sold,
        "all_gross_profit": all_gross_profit,
        "all_gross_profit_margin": float(
            row.metrics.all_gross_profit_margin
        ),

        "all_cross_sell_cost_of_goods_sold": (
            all_cross_sell_cost_of_goods_sold
        ),
        "all_cross_sell_gross_profit": all_cross_sell_gross_profit,
        "all_cross_sell_revenue": all_cross_sell_revenue,
        "all_cross_sell_units_sold": float(
            row.metrics.all_cross_sell_units_sold
        ),

        "all_lead_cost_of_goods_sold": all_lead_cost_of_goods_sold,
        "all_lead_gross_profit": all_lead_gross_profit,
        "all_lead_revenue": all_lead_revenue,
        "all_lead_units_sold": float(row.metrics.all_lead_units_sold),

        "gmail_forwards": int(row.metrics.gmail_forwards),
        "gmail_saves": int(row.metrics.gmail_saves),
        "gmail_secondary_clicks": int(
            row.metrics.gmail_secondary_clicks
        ),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return table_name, [
        row_data[column]
        for column in clickhouse_db.DAILY_TABLE_COLUMNS
    ]


def build_geo_daily_clickhouse_row(
    row: Any,
    *,
    geo_constants_map: dict[str, dict[str, Any]],
    targeted_locations_by_campaign: dict[str, str],
    campaign_budget_info_by_campaign: dict[str, dict[str, Any]],
    campaign_goal_hints_by_campaign: dict[str, dict[str, Any]] | None = None,
) -> tuple[str, list[Any]]:
    date_start, date_stop = get_daily_datetime_range(row)

    table_name = get_daily_geo_target_table(
        row,
        campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
    )

    # Micros -> money. Это не бизнес-расчет метрики,
    # а техническая конвертация из micros в нормальную валюту.
    spend = micros_to_money(row.metrics.cost_micros)
    average_cpc = micros_to_money(row.metrics.average_cpc)
    average_cpm = micros_to_money(row.metrics.average_cpm)
    average_cpv = micros_to_money(row.metrics.trueview_average_cpv)
    average_cost = micros_to_money(row.metrics.average_cost)

    cost_per_conversion = micros_to_money(row.metrics.cost_per_conversion)
    cost_per_all_conversions = micros_to_money(
        row.metrics.cost_per_all_conversions
    )

    campaign_id = str(row.campaign.id)

    campaign_budget_info = campaign_budget_info_by_campaign.get(
        campaign_id,
        {},
    )

    country_criterion_id = (
        str(row.geographic_view.country_criterion_id)
        if row.geographic_view.country_criterion_id
        else None
    )

    region_criterion_id = resource_name_to_id(
        row.segments.geo_target_region
    )
    city_criterion_id = resource_name_to_id(row.segments.geo_target_city)

    country_info = geo_constants_map.get(str(country_criterion_id), {})
    region_info = geo_constants_map.get(str(region_criterion_id), {})
    city_info = geo_constants_map.get(str(city_criterion_id), {})

    row_data = {
        "date_start": date_start,
        "date_stop": date_stop,

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": campaign_id,
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "campaign_primary_status": enum_name(row.campaign.primary_status),
        "campaign_primary_status_reasons_json": (
            campaign_budget_info.get(
                "campaign_primary_status_reasons_json"
            )
        ),
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),
        "google_ads_goal_type": get_google_ads_goal_type(table_name),

        "geo_location_name": (
            city_info.get("name")
            or region_info.get("name")
            or country_info.get("name")
        ),
        "geo_country_code": country_info.get("country_code"),

        "location_type": enum_name(row.geographic_view.location_type),
        "geo_country_criterion_id": country_criterion_id,
        "geo_country_name": country_info.get("name"),
        "geo_region_criterion_id": region_criterion_id,
        "geo_region_name": region_info.get("name"),
        "geo_city_criterion_id": city_criterion_id,
        "geo_city_name": city_info.get("name"),
        "targeted_locations_json": targeted_locations_by_campaign.get(
            campaign_id
        ),

        "device": enum_name(row.segments.device),

        "ad_network_type": enum_name(row.segments.ad_network_type),

        "budget_id": campaign_budget_info.get("budget_id"),
        "budget_name": campaign_budget_info.get("budget_name"),
        "budget_period": campaign_budget_info.get("budget_period"),
        "daily_budget": campaign_budget_info.get("daily_budget"),
        "lifetime_budget": campaign_budget_info.get("lifetime_budget"),
        "is_budget_limited": campaign_budget_info.get("is_budget_limited"),

        "bidding_strategy_type": campaign_budget_info.get(
            "bidding_strategy_type"
        ),
        "optimization_score": campaign_budget_info.get(
            "optimization_score"
        ),

        "impressions": int(row.metrics.impressions),
        "clicks": int(row.metrics.clicks),
        "ctr": float(row.metrics.ctr),

        "spend": spend,

        "average_cpc": average_cpc,
        "average_cpm": average_cpm,
        "average_cpv": average_cpv,
        "average_cost": average_cost,

        "interactions": int(row.metrics.interactions),
        "interaction_rate": float(row.metrics.interaction_rate),

        "video_views": int(row.metrics.video_trueview_views),
        "view_rate": float(row.metrics.video_trueview_view_rate),

        "absolute_top_impression_percentage": float(
            row.metrics.absolute_top_impression_percentage
        ),
        "top_impression_percentage": float(
            row.metrics.top_impression_percentage
        ),

        "conversions": float(row.metrics.conversions),
        "conversion_rate": float(
            row.metrics.conversions_from_interactions_rate
        ),
        "cost_per_conversion": cost_per_conversion,

        "conversions_value": float(row.metrics.conversions_value),

        "all_conversions": float(row.metrics.all_conversions),
        "all_conversions_value": float(row.metrics.all_conversions_value),

        "all_conversion_rate": float(
            row.metrics.all_conversions_from_interactions_rate
        ),
        "cost_per_all_conversions": cost_per_all_conversions,

        "conversions_by_conversion_date": float(
            row.metrics.conversions_by_conversion_date
        ),
        "conversions_value_by_conversion_date": float(
            row.metrics.conversions_value_by_conversion_date
        ),

        "all_conversions_by_conversion_date": float(
            row.metrics.all_conversions_by_conversion_date
        ),
        "all_conversions_value_by_conversion_date": float(
            row.metrics.all_conversions_value_by_conversion_date
        ),

        "value_per_conversion": float(row.metrics.value_per_conversion),
        "value_per_conversions_by_conversion_date": float(
            row.metrics.value_per_conversions_by_conversion_date
        ),

        "value_per_all_conversions": float(
            row.metrics.value_per_all_conversions
        ),
        "value_per_all_conversions_by_conversion_date": float(
            row.metrics.value_per_all_conversions_by_conversion_date
        ),

        "cross_device_conversions": float(
            row.metrics.cross_device_conversions
        ),
        "view_through_conversions": int(
            row.metrics.view_through_conversions
        ),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return table_name, [
        row_data[column]
        for column in clickhouse_db.DAILY_GEO_TABLE_COLUMNS
    ]


def build_daily_campaign_clickhouse_row(
    row: Any,
    *,
    campaign_goal_hints_by_campaign: dict[str, dict[str, Any]] | None = None,
) -> tuple[str, list[Any]]:
    date_start, date_stop = get_daily_datetime_range(row)

    table_name = get_daily_campaign_target_table(
        row,
        campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
    )

    # Micros -> money. Это не бизнес-расчет метрики,
    # а техническая конвертация из micros в нормальную валюту.
    spend = micros_to_money(row.metrics.cost_micros)

    average_cpc = micros_to_money(row.metrics.average_cpc)
    average_cpm = micros_to_money(row.metrics.average_cpm)
    average_cpv = micros_to_money(row.metrics.trueview_average_cpv)
    average_cost = micros_to_money(row.metrics.average_cost)
    average_cpe = micros_to_money(row.metrics.average_cpe)

    active_view_cpm = micros_to_money(row.metrics.active_view_cpm)
    active_view_measurable_cost = micros_to_money(
        row.metrics.active_view_measurable_cost_micros
    )

    cost_per_conversion = micros_to_money(row.metrics.cost_per_conversion)
    cost_per_all_conversions = micros_to_money(
        row.metrics.cost_per_all_conversions
    )

    cross_device_conversions_value = micros_to_money(
        row.metrics.cross_device_conversions_value_micros
    )

    cost_per_current_model_attributed_conversion = micros_to_money(
        row.metrics.cost_per_current_model_attributed_conversion
    )

    cost_converted_currency_per_platform_comparable_conversion = (
        micros_to_money(
            row.metrics.cost_converted_currency_per_platform_comparable_conversion
        )
    )
    cost_per_platform_comparable_conversion = micros_to_money(
        row.metrics.cost_per_platform_comparable_conversion
    )

    revenue = micros_to_money(row.metrics.revenue_micros)
    average_order_value = micros_to_money(
        row.metrics.average_order_value_micros
    )
    cost_of_goods_sold = micros_to_money(
        row.metrics.cost_of_goods_sold_micros
    )
    gross_profit = micros_to_money(row.metrics.gross_profit_micros)

    cross_sell_cost_of_goods_sold = micros_to_money(
        row.metrics.cross_sell_cost_of_goods_sold_micros
    )
    cross_sell_gross_profit = micros_to_money(
        row.metrics.cross_sell_gross_profit_micros
    )
    cross_sell_revenue = micros_to_money(
        row.metrics.cross_sell_revenue_micros
    )

    lead_cost_of_goods_sold = micros_to_money(
        row.metrics.lead_cost_of_goods_sold_micros
    )
    lead_gross_profit = micros_to_money(
        row.metrics.lead_gross_profit_micros
    )
    lead_revenue = micros_to_money(row.metrics.lead_revenue_micros)

    all_revenue = micros_to_money(row.metrics.all_revenue_micros)
    all_average_order_value = micros_to_money(
        row.metrics.all_average_order_value_micros
    )
    all_cost_of_goods_sold = micros_to_money(
        row.metrics.all_cost_of_goods_sold_micros
    )
    all_gross_profit = micros_to_money(
        row.metrics.all_gross_profit_micros
    )

    all_cross_sell_cost_of_goods_sold = micros_to_money(
        row.metrics.all_cross_sell_cost_of_goods_sold_micros
    )
    all_cross_sell_gross_profit = micros_to_money(
        row.metrics.all_cross_sell_gross_profit_micros
    )
    all_cross_sell_revenue = micros_to_money(
        row.metrics.all_cross_sell_revenue_micros
    )

    all_lead_cost_of_goods_sold = micros_to_money(
        row.metrics.all_lead_cost_of_goods_sold_micros
    )
    all_lead_gross_profit = micros_to_money(
        row.metrics.all_lead_gross_profit_micros
    )
    all_lead_revenue = micros_to_money(
        row.metrics.all_lead_revenue_micros
    )

    average_target_cpa = micros_to_money(row.metrics.average_target_cpa_micros)

    budget_period = enum_name(row.campaign_budget.period)
    budget_amount = micros_to_money(row.campaign_budget.amount_micros)
    budget_total_amount = micros_to_money(
        row.campaign_budget.total_amount_micros
    )

    daily_budget = None
    lifetime_budget = None

    if budget_period == "DAILY":
        daily_budget = budget_amount
    else:
        lifetime_budget = budget_total_amount or budget_amount

    primary_status_reasons = enum_list_to_names(
        row.campaign.primary_status_reasons
    )

    primary_status_reasons_json = (
        json.dumps(primary_status_reasons, ensure_ascii=False)
        if primary_status_reasons
        else None
    )

    is_budget_limited = detect_budget_limited(primary_status_reasons)

    row_data = {
        "date_start": date_start,
        "date_stop": date_stop,

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "campaign_primary_status": enum_name(row.campaign.primary_status),
        "campaign_primary_status_reasons_json": primary_status_reasons_json,
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),
        "google_ads_goal_type": get_google_ads_goal_type(table_name),

        "budget_id": str(row.campaign_budget.id)
            if row.campaign_budget.id
            else None,
        "budget_name": row.campaign_budget.name or None,
        "budget_period": budget_period,
        "daily_budget": daily_budget,
        "lifetime_budget": lifetime_budget,
        "is_budget_limited": is_budget_limited,

        "bidding_strategy_type": enum_name(
            row.campaign.bidding_strategy_type
        ),
        "optimization_score": float(row.campaign.optimization_score),

        "reach": int(row.metrics.unique_users),
        "average_impression_frequency_per_user": float(
            row.metrics.average_impression_frequency_per_user
        ),
        "unique_users_two_plus": int(row.metrics.unique_users_two_plus),
        "unique_users_three_plus": int(row.metrics.unique_users_three_plus),
        "unique_users_four_plus": int(row.metrics.unique_users_four_plus),
        "unique_users_five_plus": int(row.metrics.unique_users_five_plus),
        "unique_users_ten_plus": int(row.metrics.unique_users_ten_plus),

        "impressions": int(row.metrics.impressions),
        "clicks": int(row.metrics.clicks),
        "ctr": float(row.metrics.ctr),

        "spend": spend,

        "average_cpc": average_cpc,
        "average_cpm": average_cpm,
        "average_cpv": average_cpv,
        "average_cost": average_cost,
        "average_cpe": average_cpe,

        "interactions": int(row.metrics.interactions),
        "interaction_rate": float(row.metrics.interaction_rate),

        "engagements": int(row.metrics.engagements),
        "engagement_rate": float(row.metrics.engagement_rate),

        "video_views": int(row.metrics.video_trueview_views),
        "view_rate": float(row.metrics.video_trueview_view_rate),

        "video_quartile_p25_rate": float(
            row.metrics.video_quartile_p25_rate
        ),
        "video_quartile_p50_rate": float(
            row.metrics.video_quartile_p50_rate
        ),
        "video_quartile_p75_rate": float(
            row.metrics.video_quartile_p75_rate
        ),
        "video_quartile_p100_rate": float(
            row.metrics.video_quartile_p100_rate
        ),

        "video_trueview_view_rate_in_feed": float(
            row.metrics.video_trueview_view_rate_in_feed
        ),
        "video_trueview_view_rate_in_stream": float(
            row.metrics.video_trueview_view_rate_in_stream
        ),
        "video_trueview_view_rate_shorts": float(
            row.metrics.video_trueview_view_rate_shorts
        ),
        "average_video_watch_time_duration_millis": int(
            row.metrics.average_video_watch_time_duration_millis
        ),

        "absolute_top_impression_percentage": float(
            row.metrics.absolute_top_impression_percentage
        ),
        "top_impression_percentage": float(
            row.metrics.top_impression_percentage
        ),

        "search_impression_share": float(
            row.metrics.search_impression_share
        ),
        "search_absolute_top_impression_share": float(
            row.metrics.search_absolute_top_impression_share
        ),
        "search_top_impression_share": float(
            row.metrics.search_top_impression_share
        ),
        "search_budget_lost_impression_share": float(
            row.metrics.search_budget_lost_impression_share
        ),
        "search_budget_lost_absolute_top_impression_share": float(
            row.metrics.search_budget_lost_absolute_top_impression_share
        ),
        "search_budget_lost_top_impression_share": float(
            row.metrics.search_budget_lost_top_impression_share
        ),
        "search_rank_lost_impression_share": float(
            row.metrics.search_rank_lost_impression_share
        ),
        "search_rank_lost_absolute_top_impression_share": float(
            row.metrics.search_rank_lost_absolute_top_impression_share
        ),
        "search_rank_lost_top_impression_share": float(
            row.metrics.search_rank_lost_top_impression_share
        ),
        "search_click_share": float(row.metrics.search_click_share),
        "search_exact_match_impression_share": float(
            row.metrics.search_exact_match_impression_share
        ),

        "content_impression_share": float(
            row.metrics.content_impression_share
        ),
        "content_budget_lost_impression_share": float(
            row.metrics.content_budget_lost_impression_share
        ),
        "content_rank_lost_impression_share": float(
            row.metrics.content_rank_lost_impression_share
        ),

        "active_view_viewable_impressions": int(
            row.metrics.active_view_impressions
        ),
        "active_view_viewability": float(
            row.metrics.active_view_viewability
        ),
        "active_view_cpm": active_view_cpm,
        "active_view_ctr": float(row.metrics.active_view_ctr),
        "active_view_measurability": float(
            row.metrics.active_view_measurability
        ),
        "active_view_measurable_cost": active_view_measurable_cost,
        "active_view_measurable_impressions": int(
            row.metrics.active_view_measurable_impressions
        ),

        "active_view_audibility_measurable_impressions": int(
            row.metrics.active_view_audibility_measurable_impressions
        ),
        "active_view_audibility_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_measurable_impressions_rate
        ),
        "active_view_audibility_invalid_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_invalid_measurable_impressions_rate
        ),
        "active_view_audibility_invalid_givt_measurable_impressions_rate": float(
            row.metrics.active_view_audibility_invalid_givt_measurable_impressions_rate
        ),
        "active_view_audible_impressions": int(
            row.metrics.active_view_audible_impressions
        ),
        "active_view_audible_impressions_rate": float(
            row.metrics.active_view_audible_impressions_rate
        ),
        "active_view_audible_quartile_p25_rate": float(
            row.metrics.active_view_audible_quartile_p25_rate
        ),
        "active_view_audible_quartile_p50_rate": float(
            row.metrics.active_view_audible_quartile_p50_rate
        ),
        "active_view_audible_quartile_p75_rate": float(
            row.metrics.active_view_audible_quartile_p75_rate
        ),
        "active_view_audible_quartile_p100_rate": float(
            row.metrics.active_view_audible_quartile_p100_rate
        ),
        "active_view_audible_two_seconds_impressions": int(
            row.metrics.active_view_audible_two_seconds_impressions
        ),
        "active_view_audible_two_seconds_impressions_rate": float(
            row.metrics.active_view_audible_two_seconds_impressions_rate
        ),
        "active_view_audible_thirty_seconds_impressions": int(
            row.metrics.active_view_audible_thirty_seconds_impressions
        ),
        "active_view_audible_thirty_seconds_impressions_rate": float(
            row.metrics.active_view_audible_thirty_seconds_impressions_rate
        ),

        "conversions": float(row.metrics.conversions),
        "conversion_rate": float(
            row.metrics.conversions_from_interactions_rate
        ),
        "cost_per_conversion": cost_per_conversion,
        "conversions_value": float(row.metrics.conversions_value),

        "conversions_by_conversion_date": float(
            row.metrics.conversions_by_conversion_date
        ),
        "conversions_value_by_conversion_date": float(
            row.metrics.conversions_value_by_conversion_date
        ),
        "conversions_unique_query_clusters": int(
            row.metrics.conversions_unique_query_clusters
        ),

        "all_conversions": float(row.metrics.all_conversions),
        "all_conversions_value": float(row.metrics.all_conversions_value),
        "all_conversion_rate": float(
            row.metrics.all_conversions_from_interactions_rate
        ),
        "cost_per_all_conversions": cost_per_all_conversions,

        "all_conversions_by_conversion_date": float(
            row.metrics.all_conversions_by_conversion_date
        ),
        "all_conversions_value_by_conversion_date": float(
            row.metrics.all_conversions_value_by_conversion_date
        ),

        "value_per_conversion": float(row.metrics.value_per_conversion),
        "value_per_conversions_by_conversion_date": float(
            row.metrics.value_per_conversions_by_conversion_date
        ),
        "value_per_all_conversions": float(
            row.metrics.value_per_all_conversions
        ),
        "value_per_all_conversions_by_conversion_date": float(
            row.metrics.value_per_all_conversions_by_conversion_date
        ),

        "cross_device_conversions": float(
            row.metrics.cross_device_conversions
        ),
        "cross_device_conversions_by_conversion_date": float(
            row.metrics.cross_device_conversions_by_conversion_date
        ),
        "cross_device_conversions_value_by_conversion_date": float(
            row.metrics.cross_device_conversions_value_by_conversion_date
        ),
        "cross_device_conversions_value": cross_device_conversions_value,

        "view_through_conversions": int(
            row.metrics.view_through_conversions
        ),

        "current_model_attributed_conversions": float(
            row.metrics.current_model_attributed_conversions
        ),
        "current_model_attributed_conversions_value": float(
            row.metrics.current_model_attributed_conversions_value
        ),
        "current_model_attributed_conversions_from_interactions_rate": float(
            row.metrics.current_model_attributed_conversions_from_interactions_rate
        ),
        "current_model_attributed_conversions_from_interactions_value_per_interaction": float(
            row.metrics.current_model_attributed_conversions_from_interactions_value_per_interaction
        ),
        "current_model_attributed_conversions_value_per_cost": float(
            row.metrics.current_model_attributed_conversions_value_per_cost
        ),
        "cost_per_current_model_attributed_conversion": (
            cost_per_current_model_attributed_conversion
        ),
        "value_per_current_model_attributed_conversion": float(
            row.metrics.value_per_current_model_attributed_conversion
        ),

        "platform_comparable_conversions": float(
            row.metrics.platform_comparable_conversions
        ),
        "platform_comparable_conversions_by_conversion_date": float(
            row.metrics.platform_comparable_conversions_by_conversion_date
        ),
        "platform_comparable_conversions_from_interactions_rate": float(
            row.metrics.platform_comparable_conversions_from_interactions_rate
        ),
        "platform_comparable_conversions_from_interactions_value_per_interaction": float(
            row.metrics.platform_comparable_conversions_from_interactions_value_per_interaction
        ),
        "platform_comparable_conversions_value": float(
            row.metrics.platform_comparable_conversions_value
        ),
        "platform_comparable_conversions_value_by_conversion_date": float(
            row.metrics.platform_comparable_conversions_value_by_conversion_date
        ),
        "platform_comparable_conversions_value_per_cost": float(
            row.metrics.platform_comparable_conversions_value_per_cost
        ),
        "cost_converted_currency_per_platform_comparable_conversion": (
            cost_converted_currency_per_platform_comparable_conversion
        ),
        "cost_per_platform_comparable_conversion": (
            cost_per_platform_comparable_conversion
        ),
        "value_per_platform_comparable_conversion": float(
            row.metrics.value_per_platform_comparable_conversion
        ),
        "value_per_platform_comparable_conversions_by_conversion_date": float(
            row.metrics.value_per_platform_comparable_conversions_by_conversion_date
        ),

        "orders": float(row.metrics.orders),
        "revenue": revenue,
        "units_sold": float(row.metrics.units_sold),
        "average_cart_size": float(row.metrics.average_cart_size),
        "average_order_value": average_order_value,

        "cost_of_goods_sold": cost_of_goods_sold,
        "gross_profit": gross_profit,
        "gross_profit_margin": float(row.metrics.gross_profit_margin),

        "cross_sell_cost_of_goods_sold": cross_sell_cost_of_goods_sold,
        "cross_sell_gross_profit": cross_sell_gross_profit,
        "cross_sell_revenue": cross_sell_revenue,
        "cross_sell_units_sold": float(row.metrics.cross_sell_units_sold),

        "lead_cost_of_goods_sold": lead_cost_of_goods_sold,
        "lead_gross_profit": lead_gross_profit,
        "lead_revenue": lead_revenue,
        "lead_units_sold": float(row.metrics.lead_units_sold),

        "all_orders": float(row.metrics.all_orders),
        "all_revenue": all_revenue,
        "all_units_sold": float(row.metrics.all_units_sold),
        "all_average_cart_size": float(
            row.metrics.all_average_cart_size
        ),
        "all_average_order_value": all_average_order_value,

        "all_cost_of_goods_sold": all_cost_of_goods_sold,
        "all_gross_profit": all_gross_profit,
        "all_gross_profit_margin": float(
            row.metrics.all_gross_profit_margin
        ),

        "all_cross_sell_cost_of_goods_sold": (
            all_cross_sell_cost_of_goods_sold
        ),
        "all_cross_sell_gross_profit": all_cross_sell_gross_profit,
        "all_cross_sell_revenue": all_cross_sell_revenue,
        "all_cross_sell_units_sold": float(
            row.metrics.all_cross_sell_units_sold
        ),

        "all_lead_cost_of_goods_sold": all_lead_cost_of_goods_sold,
        "all_lead_gross_profit": all_lead_gross_profit,
        "all_lead_revenue": all_lead_revenue,
        "all_lead_units_sold": float(row.metrics.all_lead_units_sold),

        "new_customer_lifetime_value": float(
            row.metrics.new_customer_lifetime_value
        ),
        "all_new_customer_lifetime_value": float(
            row.metrics.all_new_customer_lifetime_value
        ),

        "phone_calls": int(row.metrics.phone_calls),
        "phone_impressions": int(row.metrics.phone_impressions),
        "phone_through_rate": float(row.metrics.phone_through_rate),

        "gmail_forwards": int(row.metrics.gmail_forwards),
        "gmail_saves": int(row.metrics.gmail_saves),
        "gmail_secondary_clicks": int(
            row.metrics.gmail_secondary_clicks
        ),

        "bounce_rate": float(row.metrics.bounce_rate),
        "average_page_views": float(row.metrics.average_page_views),
        "average_time_on_site": float(row.metrics.average_time_on_site),
        "percent_new_visitors": float(row.metrics.percent_new_visitors),

        "invalid_clicks": int(row.metrics.invalid_clicks),
        "invalid_click_rate": float(row.metrics.invalid_click_rate),
        "general_invalid_clicks": int(row.metrics.general_invalid_clicks),
        "general_invalid_click_rate": float(
            row.metrics.general_invalid_click_rate
        ),

        "clicks_unique_query_clusters": int(
            row.metrics.clicks_unique_query_clusters
        ),
        "impressions_unique_query_clusters": int(
            row.metrics.impressions_unique_query_clusters
        ),

        "coviewed_impressions": int(row.metrics.coviewed_impressions),
        "primary_impressions": int(row.metrics.primary_impressions),
        "relative_ctr": float(row.metrics.relative_ctr),

        "publisher_organic_clicks": int(
            row.metrics.publisher_organic_clicks
        ),
        "publisher_purchased_clicks": int(
            row.metrics.publisher_purchased_clicks
        ),
        "publisher_unknown_clicks": int(
            row.metrics.publisher_unknown_clicks
        ),

        "sk_ad_network_installs": int(row.metrics.sk_ad_network_installs),
        "sk_ad_network_total_conversions": int(
            row.metrics.sk_ad_network_total_conversions
        ),

        "biddable_app_install_conversions": float(
            row.metrics.biddable_app_install_conversions
        ),
        "biddable_app_post_install_conversions": float(
            row.metrics.biddable_app_post_install_conversions
        ),
        "biddable_cohort_app_post_install_conversions": float(
            row.metrics.biddable_cohort_app_post_install_conversions
        ),

        "average_target_cpa": average_target_cpa,
        "average_target_roas": float(row.metrics.average_target_roas),

        "eligible_impressions_from_location_asset_store_reach": int(
            row.metrics.eligible_impressions_from_location_asset_store_reach
        ),

        "all_conversions_from_click_to_call": float(
            row.metrics.all_conversions_from_click_to_call
        ),
        "all_conversions_from_directions": float(
            row.metrics.all_conversions_from_directions
        ),
        "all_conversions_from_menu": float(
            row.metrics.all_conversions_from_menu
        ),
        "all_conversions_from_order": float(
            row.metrics.all_conversions_from_order
        ),
        "all_conversions_from_other_engagement": float(
            row.metrics.all_conversions_from_other_engagement
        ),
        "all_conversions_from_store_visit": float(
            row.metrics.all_conversions_from_store_visit
        ),
        "all_conversions_from_store_website": float(
            row.metrics.all_conversions_from_store_website
        ),

        "all_conversions_from_location_asset_click_to_call": float(
            row.metrics.all_conversions_from_location_asset_click_to_call
        ),
        "all_conversions_from_location_asset_directions": float(
            row.metrics.all_conversions_from_location_asset_directions
        ),
        "all_conversions_from_location_asset_menu": float(
            row.metrics.all_conversions_from_location_asset_menu
        ),
        "all_conversions_from_location_asset_order": float(
            row.metrics.all_conversions_from_location_asset_order
        ),
        "all_conversions_from_location_asset_other_engagement": float(
            row.metrics.all_conversions_from_location_asset_other_engagement
        ),
        "all_conversions_from_location_asset_store_visits": float(
            row.metrics.all_conversions_from_location_asset_store_visits
        ),
        "all_conversions_from_location_asset_website": float(
            row.metrics.all_conversions_from_location_asset_website
        ),

        "view_through_conversions_from_location_asset_click_to_call": float(
            row.metrics.view_through_conversions_from_location_asset_click_to_call
        ),
        "view_through_conversions_from_location_asset_directions": float(
            row.metrics.view_through_conversions_from_location_asset_directions
        ),
        "view_through_conversions_from_location_asset_menu": float(
            row.metrics.view_through_conversions_from_location_asset_menu
        ),
        "view_through_conversions_from_location_asset_order": float(
            row.metrics.view_through_conversions_from_location_asset_order
        ),
        "view_through_conversions_from_location_asset_other_engagement": float(
            row.metrics.view_through_conversions_from_location_asset_other_engagement
        ),
        "view_through_conversions_from_location_asset_store_visits": float(
            row.metrics.view_through_conversions_from_location_asset_store_visits
        ),
        "view_through_conversions_from_location_asset_website": float(
            row.metrics.view_through_conversions_from_location_asset_website
        ),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return table_name, [
        row_data[column]
        for column in clickhouse_db.DAILY_CAMPAIGN_TABLE_COLUMNS
    ]


def build_daily_search_term_clickhouse_row(
    row: Any,
    *,
    campaign_budget_info_by_campaign: dict[str, dict[str, Any]],
    campaign_goal_hints_by_campaign: dict[str, dict[str, Any]] | None = None,
) -> list[Any]:
    date_start, date_stop = get_daily_datetime_range(row)

    campaign_id = str(row.campaign.id)

    campaign_budget_info = campaign_budget_info_by_campaign.get(
        campaign_id,
        {},
    )

    primary_status_reasons = enum_list_to_names(
        row.campaign.primary_status_reasons
    )

    primary_status_reasons_json = (
        json.dumps(primary_status_reasons, ensure_ascii=False)
        if primary_status_reasons
        else campaign_budget_info.get("campaign_primary_status_reasons_json")
    )

    spend = micros_to_money(row.metrics.cost_micros)
    average_cpc = micros_to_money(row.metrics.average_cpc)
    average_cpm = micros_to_money(row.metrics.average_cpm)
    average_cost = micros_to_money(row.metrics.average_cost)

    cost_per_conversion = micros_to_money(row.metrics.cost_per_conversion)
    cost_per_all_conversions = micros_to_money(
        row.metrics.cost_per_all_conversions
    )

    row_data = {
        "date_start": date_start,
        "date_stop": date_stop,

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": campaign_id,
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "campaign_primary_status": enum_name(row.campaign.primary_status),
        "campaign_primary_status_reasons_json": primary_status_reasons_json,
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),
        "google_ads_goal_type": get_google_ads_goal_type(
            get_target_table(
                row,
                campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
            )
        ),

        "ad_group_id": str(row.ad_group.id),
        "ad_group_name": row.ad_group.name or None,
        "ad_group_status": enum_name(row.ad_group.status),
        "ad_group_type": enum_name(row.ad_group.type),

        "search_term": row.search_term_view.search_term,
        "search_term_status": enum_name(row.search_term_view.status),

        "keyword_ad_group_criterion_id": resource_name_to_id(
            row.segments.keyword.ad_group_criterion
        ),
        "keyword_text": row.segments.keyword.info.text or None,
        "keyword_match_type": enum_name(
            row.segments.keyword.info.match_type
        ),

        "device": enum_name(row.segments.device),

        "ad_network_type": enum_name(row.segments.ad_network_type),

        "budget_id": campaign_budget_info.get("budget_id"),
        "budget_name": campaign_budget_info.get("budget_name"),
        "budget_period": campaign_budget_info.get("budget_period"),
        "daily_budget": campaign_budget_info.get("daily_budget"),
        "lifetime_budget": campaign_budget_info.get("lifetime_budget"),
        "is_budget_limited": campaign_budget_info.get("is_budget_limited"),

        "bidding_strategy_type": enum_name(
            row.campaign.bidding_strategy_type
        ),
        "optimization_score": float(row.campaign.optimization_score),

        "impressions": int(row.metrics.impressions),
        "clicks": int(row.metrics.clicks),
        "ctr": float(row.metrics.ctr),

        "spend": spend,

        "average_cpc": average_cpc,
        "average_cpm": average_cpm,
        "average_cost": average_cost,

        "conversions": float(row.metrics.conversions),
        "conversion_rate": float(
            row.metrics.conversions_from_interactions_rate
        ),
        "cost_per_conversion": cost_per_conversion,

        "conversions_value": float(row.metrics.conversions_value),

        "all_conversions": float(row.metrics.all_conversions),
        "all_conversions_value": float(row.metrics.all_conversions_value),
        "all_conversion_rate": float(
            row.metrics.all_conversions_from_interactions_rate
        ),
        "cost_per_all_conversions": cost_per_all_conversions,

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return [
        row_data[column]
        for column in clickhouse_db.DAILY_SEARCH_TERM_TABLE_COLUMNS
    ]


def build_ad_group_ad_asset_clickhouse_row(row: Any) -> list[Any]:
    image_mime_type = enum_name_or_none(row.asset.image_asset.mime_type)
    youtube_video_id = none_if_empty(
        row.asset.youtube_video_asset.youtube_video_id
    )

    row_data = {
        "source_type": "ad_group_ad_asset_view",

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),

        "ad_group_id": str(row.ad_group.id) if row.ad_group.id else None,
        "ad_group_name": row.ad_group.name or None,
        "ad_group_status": enum_name(row.ad_group.status),

        "ad_id": str(row.ad_group_ad.ad.id)
        if row.ad_group_ad.ad.id
        else None,
        "ad_name": row.ad_group_ad.ad.name or None,
        "ad_type": enum_name(row.ad_group_ad.ad.type),
        "ad_status": enum_name(row.ad_group_ad.status),

        "asset_group_id": None,
        "asset_group_name": None,
        "asset_group_status": None,
        "asset_group_strength": None,
        "asset_group_asset_status": None,

        "asset_id": str(row.asset.id),
        "asset_name": row.asset.name or None,
        "asset_type": enum_name(row.asset.type),
        "asset_field_type": enum_name(
            row.ad_group_ad_asset_view.field_type
        ),

        "image_url": none_if_empty(row.asset.image_asset.full_size.url),
        "image_width": positive_int_or_none(
            row.asset.image_asset.full_size.width_pixels
        ),
        "image_height": positive_int_or_none(
            row.asset.image_asset.full_size.height_pixels
        ),
        "image_mime_type": image_mime_type,
        "image_file_size": positive_int_or_none(
            row.asset.image_asset.file_size
        ),
        "youtube_video_id": youtube_video_id,
        "youtube_video_url": youtube_url_from_id(youtube_video_id),
        "youtube_video_title": none_if_empty(
            row.asset.youtube_video_asset.youtube_video_title
        ),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return [
        row_data[column]
        for column in clickhouse_db.CREATIVE_ASSET_COLUMNS
    ]


def chunks(items: list[str], size: int) -> list[list[str]]:
    return [
        items[index:index + size]
        for index in range(0, len(items), size)
    ]


def get_youtube_video_assets_by_ids(
    *,
    customer_id: str,
    asset_ids: set[str],
) -> dict[str, dict[str, Any]]:
    ids = sorted(
        {
            str(asset_id)
            for asset_id in asset_ids
            if asset_id and str(asset_id).isdigit()
        }
    )

    if not ids:
        return {}

    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    result: dict[str, dict[str, Any]] = {}

    # Делаем чанками, чтобы не упереться в лимит IN (...)
    for ids_chunk in chunks(ids, 500):
        ids_text = ", ".join(ids_chunk)

        query = queries.YOUTUBE_VIDEO_ASSET_QUERY_TEMPLATE.format(
            asset_ids=ids_text
        )

        try:
            stream = google_ads_service.search_stream(
                customer_id=customer_id,
                query=query,
            )

            for batch in stream:
                for row in batch.results:
                    asset_id = str(row.asset.id)
                    youtube_video_id = none_if_empty(
                        row.asset.youtube_video_asset.youtube_video_id
                    )

                    result[asset_id] = {
                        "asset_id": asset_id,
                        "asset_name": row.asset.name or None,
                        "asset_type": enum_name(row.asset.type),
                        "youtube_video_id": youtube_video_id,
                        "youtube_video_url": youtube_url_from_id(
                            youtube_video_id
                        ),
                        "youtube_video_title": none_if_empty(
                            row.asset.youtube_video_asset.youtube_video_title
                        ),
                    }

        except GoogleAdsException as ex:
            print("Google Ads youtube video asset request failed")
            print(f"Request ID: {ex.request_id}")
            print(f"Status: {ex.error.code().name}")

            for error in ex.failure.errors:
                print(f"Error: {error.message}")

            raise

    return result


def build_direct_image_ad_clickhouse_row(row: Any) -> list[Any]:
    ad_id = str(row.ad_group_ad.ad.id) if row.ad_group_ad.ad.id else None
    image_ad = row.ad_group_ad.ad.image_ad

    image_url = none_if_empty(image_ad.image_url)
    preview_image_url = none_if_empty(image_ad.preview_image_url)

    row_data = {
        "source_type": "direct_image_ad",

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),

        "ad_group_id": str(row.ad_group.id) if row.ad_group.id else None,
        "ad_group_name": row.ad_group.name or None,
        "ad_group_status": enum_name(row.ad_group.status),

        "ad_id": ad_id,
        "ad_name": row.ad_group_ad.ad.name or None,
        "ad_type": enum_name(row.ad_group_ad.ad.type),
        "ad_status": enum_name(row.ad_group_ad.status),

        "asset_group_id": None,
        "asset_group_name": None,
        "asset_group_status": None,
        "asset_group_strength": None,
        "asset_group_asset_status": None,

        # У IMAGE_AD нет отдельного asset.id, поэтому делаем стабильный технический asset_id.
        "asset_id": f"direct_image_ad_{ad_id}" if ad_id else None,
        "asset_name": (
            row.ad_group_ad.ad.name
            or image_ad.name
            or None
        ),
        "asset_type": "IMAGE",
        "asset_field_type": "IMAGE_AD",

        "image_url": image_url or preview_image_url,
        "image_width": positive_int_or_none(image_ad.pixel_width),
        "image_height": positive_int_or_none(image_ad.pixel_height),
        "image_mime_type": enum_name_or_none(image_ad.mime_type),
        "image_file_size": None,

        "youtube_video_id": None,
        "youtube_video_url": None,
        "youtube_video_title": None,

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return [
        row_data[column]
        for column in clickhouse_db.CREATIVE_ASSET_COLUMNS
    ]


def build_direct_video_responsive_ad_clickhouse_rows(
    row: Any,
    youtube_video_assets_by_id: dict[str, dict[str, Any]],
) -> list[list[Any]]:
    ad_id = str(row.ad_group_ad.ad.id) if row.ad_group_ad.ad.id else None
    videos = row.ad_group_ad.ad.video_responsive_ad.videos

    rows: list[list[Any]] = []

    for position, video in enumerate(videos, start=1):
        asset_id = resource_name_to_id(video.asset)
        video_info = youtube_video_assets_by_id.get(str(asset_id), {})

        youtube_video_id = video_info.get("youtube_video_id")

        row_data = {
            "source_type": "direct_video_responsive_ad",

            "customer_id": str(row.customer.id),
            "customer_name": row.customer.descriptive_name,

            "campaign_id": str(row.campaign.id),
            "campaign_name": row.campaign.name,
            "campaign_status": enum_name(row.campaign.status),
            "advertising_channel_type": enum_name(
                row.campaign.advertising_channel_type
            ),
            "advertising_channel_sub_type": enum_name(
                row.campaign.advertising_channel_sub_type
            ),

            "ad_group_id": str(row.ad_group.id) if row.ad_group.id else None,
            "ad_group_name": row.ad_group.name or None,
            "ad_group_status": enum_name(row.ad_group.status),

            "ad_id": ad_id,
            "ad_name": row.ad_group_ad.ad.name or None,
            "ad_type": enum_name(row.ad_group_ad.ad.type),
            "ad_status": enum_name(row.ad_group_ad.status),

            "asset_group_id": None,
            "asset_group_name": None,
            "asset_group_status": None,
            "asset_group_strength": None,
            "asset_group_asset_status": None,

            "asset_id": str(asset_id) if asset_id else None,
            "asset_name": video_info.get("asset_name"),
            "asset_type": "YOUTUBE_VIDEO",
            "asset_field_type": f"VIDEO_RESPONSIVE_AD_VIDEO_{position}",

            "image_url": None,
            "image_width": None,
            "image_height": None,
            "image_mime_type": None,
            "image_file_size": None,

            "youtube_video_id": youtube_video_id,
            "youtube_video_url": video_info.get("youtube_video_url"),
            "youtube_video_title": video_info.get("youtube_video_title"),

            "loaded_at": datetime.now(ALMATY_TZ),
        }

        rows.append(
            [
                row_data[column]
                for column in clickhouse_db.CREATIVE_ASSET_COLUMNS
            ]
        )

    return rows


def build_asset_group_asset_clickhouse_row(row: Any) -> list[Any]:
    image_mime_type = enum_name_or_none(row.asset.image_asset.mime_type)
    youtube_video_id = none_if_empty(
        row.asset.youtube_video_asset.youtube_video_id
    )

    row_data = {
        "source_type": "asset_group_asset",

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),

        "ad_group_id": None,
        "ad_group_name": None,
        "ad_group_status": None,

        "ad_id": None,
        "ad_name": None,
        "ad_type": None,
        "ad_status": None,

        "asset_group_id": str(row.asset_group.id)
        if row.asset_group.id
        else None,
        "asset_group_name": row.asset_group.name or None,
        "asset_group_status": enum_name(row.asset_group.status),
        "asset_group_strength": enum_name(row.asset_group.ad_strength),
        "asset_group_asset_status": enum_name(row.asset_group_asset.status),

        "asset_id": str(row.asset.id),
        "asset_name": row.asset.name or None,
        "asset_type": enum_name(row.asset.type),
        "asset_field_type": enum_name(row.asset_group_asset.field_type),

        "image_url": none_if_empty(row.asset.image_asset.full_size.url),
        "image_width": positive_int_or_none(
            row.asset.image_asset.full_size.width_pixels
        ),
        "image_height": positive_int_or_none(
            row.asset.image_asset.full_size.height_pixels
        ),
        "image_mime_type": image_mime_type,
        "image_file_size": positive_int_or_none(
            row.asset.image_asset.file_size
        ),
        "youtube_video_id": youtube_video_id,
        "youtube_video_url": youtube_url_from_id(youtube_video_id),
        "youtube_video_title": none_if_empty(
            row.asset.youtube_video_asset.youtube_video_title
        ),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return [
        row_data[column]
        for column in clickhouse_db.CREATIVE_ASSET_COLUMNS
    ]


def build_gender_daily_clickhouse_row(
    row: Any,
    *,
    campaign_goal_hints_by_campaign: dict[str, dict[str, Any]] | None = None,
) -> list[Any]:
    date_start, date_stop = get_daily_datetime_range(row)

    hourly_table = get_target_table(
        row,
        campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
    )

    spend = micros_to_money(row.metrics.cost_micros)
    average_cpc = micros_to_money(row.metrics.average_cpc)
    average_cpm = micros_to_money(row.metrics.average_cpm)
    average_cost = micros_to_money(row.metrics.average_cost)
    cost_per_conversion = micros_to_money(row.metrics.cost_per_conversion)
    cost_per_all_conversions = micros_to_money(
        row.metrics.cost_per_all_conversions
    )

    row_data = {
        "date_start": date_start,
        "date_stop": date_stop,

        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,

        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "campaign_status": enum_name(row.campaign.status),
        "campaign_primary_status": enum_name(row.campaign.primary_status),
        "advertising_channel_type": enum_name(
            row.campaign.advertising_channel_type
        ),
        "advertising_channel_sub_type": enum_name(
            row.campaign.advertising_channel_sub_type
        ),
        "google_ads_goal_type": get_google_ads_goal_type(hourly_table),

        "ad_group_id": str(row.ad_group.id) if row.ad_group.id else None,
        "ad_group_name": row.ad_group.name or None,
        "ad_group_status": enum_name(row.ad_group.status),
        "ad_group_type": enum_name(row.ad_group.type),

        "gender_criterion_id": (
            str(row.ad_group_criterion.criterion_id)
            if row.ad_group_criterion.criterion_id
            else None
        ),
        "gender_type": enum_name(row.ad_group_criterion.gender.type),
        "gender_status": enum_name(row.ad_group_criterion.status),

        "device": enum_name(row.segments.device),
        "ad_network_type": enum_name(row.segments.ad_network_type),

        "impressions": int(row.metrics.impressions),
        "clicks": int(row.metrics.clicks),
        "ctr": float(row.metrics.ctr),

        "spend": spend,
        "average_cpc": average_cpc,
        "average_cpm": average_cpm,
        "average_cost": average_cost,

        "interactions": int(row.metrics.interactions),
        "interaction_rate": float(row.metrics.interaction_rate),

        "engagements": int(row.metrics.engagements),
        "engagement_rate": float(row.metrics.engagement_rate),

        "video_views": int(row.metrics.video_trueview_views),
        "view_rate": float(row.metrics.video_trueview_view_rate),

        "conversions": float(row.metrics.conversions),
        "conversion_rate": float(
            row.metrics.conversions_from_interactions_rate
        ),
        "cost_per_conversion": cost_per_conversion,
        "conversions_value": float(row.metrics.conversions_value),

        "all_conversions": float(row.metrics.all_conversions),
        "all_conversions_value": float(row.metrics.all_conversions_value),
        "all_conversion_rate": float(
            row.metrics.all_conversions_from_interactions_rate
        ),
        "cost_per_all_conversions": cost_per_all_conversions,

        "view_through_conversions": int(row.metrics.view_through_conversions),

        "loaded_at": datetime.now(ALMATY_TZ),
    }

    return [
        row_data[column]
        for column in clickhouse_db.GENDER_DAILY_TABLE_COLUMNS
    ]


def fetch_ad_hourly_data(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[dict[str, Any], dict[str, list[list[Any]]]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    query = queries.AD_HOURLY_QUERY.format(
        date_since=date_since,
        date_until=date_until,
    )

    grouped_rows: dict[str, list[list[Any]]] = {
        table_name: []
        for table_name in clickhouse_db.HOURLY_TABLES
    }

    raw_rows: list[dict[str, Any]] = []

    campaign_goal_hints_by_campaign = fetch_campaign_goal_hints_by_campaign(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=query,
        )

        for batch in stream:
            for row in batch.results:
                raw_rows.append(row_to_raw_dict(row))

                table_name, formatted_row = build_clickhouse_row(
                    row,
                    campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
                )
                grouped_rows[table_name].append(formatted_row)

    except GoogleAdsException as ex:
        print("Google Ads hourly request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        raise

    response_data = {
        "customer_id": customer_id,
        "date_since": date_since,
        "date_until": date_until,
        "query_name": "ad_hourly",
        "rows_count": len(raw_rows),
        "rows": raw_rows,
    }

    return response_data, grouped_rows


def fetch_ad_group_ad_daily_data(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[dict[str, Any], list[list[Any]]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    query = queries.AD_GROUP_AD_DAILY_QUERY.format(
        date_since=date_since,
        date_until=date_until,
    )

    grouped_rows: dict[str, list[list[Any]]] = {
        table_name: []
        for table_name in clickhouse_db.DAILY_TABLES
    }

    raw_rows: list[dict[str, Any]] = []

    campaign_budget_info_by_campaign = (
        fetch_campaign_budget_info_by_campaign(
            customer_id=customer_id
        )
    )

    campaign_goal_hints_by_campaign = fetch_campaign_goal_hints_by_campaign(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )


    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=query,
        )

        for batch in stream:
            for row in batch.results:
                raw_rows.append(row_to_raw_dict(row))

                table_name, formatted_row = (
                    build_ad_group_ad_daily_clickhouse_row(
                        row,
                        campaign_budget_info_by_campaign=(
                            campaign_budget_info_by_campaign
                        ),
                        campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
                    )
                )

                grouped_rows[table_name].append(formatted_row)

    except GoogleAdsException as ex:
        print("Google Ads ad group ad daily request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        raise

    response_data = {
        "customer_id": customer_id,
        "date_since": date_since,
        "date_until": date_until,
        "query_name": "ad_group_ad_daily",
        "rows_count": len(raw_rows),
        "rows": raw_rows,
        "campaign_budget_info_campaigns_count": len(
            campaign_budget_info_by_campaign
        ),
    }

    return response_data, grouped_rows


def fetch_geo_daily_data(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[dict[str, Any], list[list[Any]]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    query = queries.GEO_DAILY_QUERY.format(
        date_since=date_since,
        date_until=date_until,
    )

    rows: list[list[Any]] = []

    raw_rows: list[dict[str, Any]] = []
    raw_proto_rows: list[Any] = []
    geo_criterion_ids: set[str] = set()

    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=query,
        )

        for batch in stream:
            for row in batch.results:
                raw_rows.append(row_to_raw_dict(row))
                raw_proto_rows.append(row)

                if row.geographic_view.country_criterion_id:
                    geo_criterion_ids.add(
                        str(row.geographic_view.country_criterion_id)
                    )

                region_id = resource_name_to_id(
                    row.segments.geo_target_region
                )
                city_id = resource_name_to_id(row.segments.geo_target_city)

                if region_id:
                    geo_criterion_ids.add(region_id)

                if city_id:
                    geo_criterion_ids.add(city_id)

    except GoogleAdsException as ex:
        print("Google Ads geo daily request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        raise

    geo_constants_map = get_geo_target_constants_map(
        customer_id=customer_id,
        criterion_ids=geo_criterion_ids,
    )

    targeted_locations_by_campaign = fetch_targeted_locations_by_campaign(
        customer_id=customer_id
    )

    campaign_budget_info_by_campaign = (
        fetch_campaign_budget_info_by_campaign(
            customer_id=customer_id
        )
    )

    campaign_goal_hints_by_campaign = fetch_campaign_goal_hints_by_campaign(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    for row in raw_proto_rows:
        table_name, formatted_row = build_geo_daily_clickhouse_row(
            row,
            geo_constants_map=geo_constants_map,
            targeted_locations_by_campaign=targeted_locations_by_campaign,
            campaign_budget_info_by_campaign=(
                campaign_budget_info_by_campaign
            ),
            campaign_goal_hints_by_campaign=(
                campaign_goal_hints_by_campaign
            ),
        )
        rows.append(formatted_row)

    response_data = {
        "customer_id": customer_id,
        "date_since": date_since,
        "date_until": date_until,
        "query_name": "geo_daily",
        "rows_count": len(raw_rows),
        "rows": raw_rows,
        "geo_constants_count": len(geo_constants_map),
        "targeted_locations_campaigns_count": len(
            targeted_locations_by_campaign
        ),
        "campaign_budget_info_campaigns_count": len(
            campaign_budget_info_by_campaign
        ),
    }

    return response_data, rows


def fetch_daily_campaign_data(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[dict[str, Any], dict[str, list[list[Any]]]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    query = queries.DAILY_CAMPAIGN_QUERY.format(
        date_since=date_since,
        date_until=date_until,
    )

    grouped_rows: dict[str, list[list[Any]]] = {
        table_name: []
        for table_name in clickhouse_db.DAILY_CAMPAIGN_TABLES
    }

    raw_rows: list[dict[str, Any]] = []
    campaign_goal_hints_by_campaign = fetch_campaign_goal_hints_by_campaign(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=query,
        )

        for batch in stream:
            for row in batch.results:
                raw_rows.append(row_to_raw_dict(row))

                table_name, formatted_row = (
                    build_daily_campaign_clickhouse_row(
                        row,
                        campaign_goal_hints_by_campaign=campaign_goal_hints_by_campaign,
                    )
                )

                grouped_rows[table_name].append(formatted_row)

    except GoogleAdsException as ex:
        print("Google Ads daily campaign request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        raise

    response_data = {
        "customer_id": customer_id,
        "date_since": date_since,
        "date_until": date_until,
        "query_name": "daily_campaign",
        "rows_count": len(raw_rows),
        "rows": raw_rows,
    }

    return response_data, grouped_rows


def fetch_daily_search_term_data(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[dict[str, Any], list[list[Any]]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    query = queries.SEARCH_TERM_DAILY_QUERY.format(
        date_since=date_since,
        date_until=date_until,
    )

    rows: list[list[Any]] = []
    raw_rows: list[dict[str, Any]] = []

    campaign_budget_info_by_campaign = (
        fetch_campaign_budget_info_by_campaign(
            customer_id=customer_id
        )
    )

    campaign_goal_hints_by_campaign = fetch_campaign_goal_hints_by_campaign(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=query,
        )

        for batch in stream:
            for row in batch.results:
                raw_rows.append(row_to_raw_dict(row))

                rows.append(
                    build_daily_search_term_clickhouse_row(
                        row,
                        campaign_budget_info_by_campaign=(
                            campaign_budget_info_by_campaign
                        ),
                        campaign_goal_hints_by_campaign=(
                            campaign_goal_hints_by_campaign
                        ),
                    )
                )

    except GoogleAdsException as ex:
        print("Google Ads daily search term request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        raise

    response_data = {
        "customer_id": customer_id,
        "date_since": date_since,
        "date_until": date_until,
        "query_name": "daily_search_term",
        "rows_count": len(raw_rows),
        "rows": raw_rows,
        "campaign_budget_info_campaigns_count": len(
            campaign_budget_info_by_campaign
        ),
    }

    return response_data, rows


def fetch_creative_assets_data(
    *,
    customer_id: str,
) -> tuple[dict[str, Any], list[list[Any]]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    rows: list[list[Any]] = []
    raw_rows: list[dict[str, Any]] = []

    queries_to_run = [
        (
            "ad_group_ad_assets",
            queries.AD_GROUP_AD_ASSET_QUERY,
            build_ad_group_ad_asset_clickhouse_row,
        ),
        (
            "asset_group_assets",
            queries.ASSET_GROUP_ASSET_QUERY,
            build_asset_group_asset_clickhouse_row,
        ),
        (
            "direct_image_ads",
            queries.DIRECT_IMAGE_AD_CREATIVE_QUERY,
            build_direct_image_ad_clickhouse_row,
        ),
    ]

    for query_name, query_text, build_func in queries_to_run:
        try:
            stream = google_ads_service.search_stream(
                customer_id=customer_id,
                query=query_text,
            )

            for batch in stream:
                for row in batch.results:
                    raw_rows.append(
                        {
                            "asset_query_name": query_name,
                            "row": row_to_raw_dict(row),
                        }
                    )

                    rows.append(build_func(row))

        except GoogleAdsException as ex:
            print(f"Google Ads creative assets request failed: {query_name}")
            print(f"Request ID: {ex.request_id}")
            print(f"Status: {ex.error.code().name}")

            for error in ex.failure.errors:
                print(f"Error: {error.message}")

            raise

    # Direct VIDEO_RESPONSIVE_AD отдельно:
    # сначала собираем video asset ids, 
    # потом одним/несколькими запросами получаем YouTube metadata.
    direct_video_rows: list[Any] = []
    direct_video_asset_ids: set[str] = set()

    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=queries.DIRECT_VIDEO_RESPONSIVE_AD_CREATIVE_QUERY,
        )

        for batch in stream:
            for row in batch.results:
                direct_video_rows.append(row)

                raw_rows.append(
                    {
                        "asset_query_name": "direct_video_responsive_ads",
                        "row": row_to_raw_dict(row),
                    }
                )

                for video in row.ad_group_ad.ad.video_responsive_ad.videos:
                    asset_id = resource_name_to_id(video.asset)

                    if asset_id:
                        direct_video_asset_ids.add(str(asset_id))

    except GoogleAdsException as ex:
        print("Google Ads creative assets request failed: direct_video_responsive_ads")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        raise

    youtube_video_assets_by_id = get_youtube_video_assets_by_ids(
        customer_id=customer_id,
        asset_ids=direct_video_asset_ids,
    )

    for row in direct_video_rows:
        rows.extend(
            build_direct_video_responsive_ad_clickhouse_rows(
                row,
                youtube_video_assets_by_id=youtube_video_assets_by_id,
            )
        )

    response_data = {
        "customer_id": customer_id,
        "query_name": "creative_assets",
        "rows_count": len(raw_rows),
        "rows": raw_rows,
        "sources": [
            "ad_group_ad_asset_view",
            "asset_group_asset",
            "direct_image_ad",
            "direct_video_responsive_ad",
        ],
        "direct_video_asset_ids_count": len(direct_video_asset_ids),
        "direct_video_assets_loaded_count": len(youtube_video_assets_by_id),
        "clickhouse_rows_count": len(rows),
    }

    return response_data, rows


def fetch_gender_daily_data(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> tuple[dict[str, Any], list[list]]:
    client = get_client()
    google_ads_service = client.get_service("GoogleAdsService")

    query = queries.GENDER_DAILY_QUERY.format(
        date_since=date_since,
        date_until=date_until,
    )

    raw_rows: list[dict[str, Any]] = []
    rows: list[list] = []

    campaign_goal_hints_by_campaign = fetch_campaign_goal_hints_by_campaign(
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )

    try:
        stream = google_ads_service.search_stream(
            customer_id=customer_id,
            query=query,
        )

        for batch in stream:
            for row in batch.results:
                raw_rows.append(row_to_raw_dict(row))

                rows.append(
                    build_gender_daily_clickhouse_row(
                        row,
                        campaign_goal_hints_by_campaign=(
                            campaign_goal_hints_by_campaign
                        ),
                    )
                )

    except GoogleAdsException as ex:
        print("Google Ads gender daily request failed")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")

        for error in ex.failure.errors:
            print(f"Error: {error.message}")

        return {
            "rows_count": 0,
            "data": [],
            "error": str(ex),
        }, []

    response_data = {
        "rows_count": len(raw_rows),
        "data": raw_rows,
        "campaign_goal_hints_campaigns_count": len(
            campaign_goal_hints_by_campaign
        ),
    }

    return response_data, rows
