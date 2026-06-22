"""
clickhouse_db.py

Клиент и операции с ClickHouse:
- get_client()        — подключение к google_ads_staging
- get_raw_client()    — подключение к google_ads_raw
- insert_raw_data()   — сохранение сырых API-ответов в raw_data
- insert_*_rows()     — вставка строк в staging-таблицы
- delete_*_for_period() — очистка staging за период перед вставкой
"""

import json
import uuid
from datetime import datetime, timedelta
from typing import Any

import clickhouse_connect

import config


# ============================================================
# STAGING table names (пайплайн пишет сюда)
# ============================================================

HOURLY_TABLES = [
    "google_ads_sales_hourly_campaign_level_staging",
    "google_ads_leads_hourly_campaign_level_staging",
    "google_ads_website_traffic_hourly_campaign_level_staging",
    "google_ads_app_promotion_hourly_campaign_level_staging",
    "google_ads_youtube_reach_views_engagement_hourly_campaign_level_staging",
    "google_ads_store_visits_promotions_hourly_campaign_level_staging",
    "google_ads_no_goal_hourly_campaign_level_staging",
]

DAILY_TABLES = [
    "google_ads_sales_daily_ad_level_staging",
    "google_ads_leads_daily_ad_level_staging",
    "google_ads_website_traffic_daily_ad_level_staging",
    "google_ads_app_promotion_daily_ad_level_staging",
    "google_ads_youtube_reach_views_engagement_daily_ad_level_staging",
    "google_ads_store_visits_promotions_daily_ad_level_staging",
    "google_ads_no_goal_daily_ad_level_staging",
]

DAILY_GEO_TABLE = "google_ads_geo_daily_region_level_staging"

DAILY_CAMPAIGN_TABLES = [
    "google_ads_sales_daily_campaign_level_staging",
    "google_ads_leads_daily_campaign_level_staging",
    "google_ads_website_traffic_daily_campaign_level_staging",
    "google_ads_app_promotion_daily_campaign_level_staging",
    "google_ads_youtube_reach_views_engagement_daily_campaign_level_staging",
    "google_ads_store_visits_promotions_daily_campaign_level_staging",
    "google_ads_no_goal_daily_campaign_level_staging",
]

DAILY_SEARCH_TERM_TABLE = "google_ads_daily_search_term_level_staging"

CREATIVE_ASSET_TABLE = "google_ads_creative_assets_staging"

GENDER_DAILY_TABLE = "google_ads_gender_daily_level_staging"

# ============================================================
# Маппинг: staging_table_name -> hourly таблица (для insert func)
# ============================================================

# Маппинг hourly staging -> old hourly (для insert_goal_rows validation)
_HOURLY_TABLES_SET = set(HOURLY_TABLES)
_DAILY_TABLES_SET = set(DAILY_TABLES)
_DAILY_CAMPAIGN_TABLES_SET = set(DAILY_CAMPAIGN_TABLES)


# ============================================================
# Columns — не меняются, структура та же что в старых таблицах
# ============================================================

HOURLY_TABLE_COLUMNS = [
    "date_start",
    "date_stop",

    "customer_id",
    "customer_name",

    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_primary_status",
    "campaign_primary_status_reasons_json",
    "advertising_channel_type",
    "advertising_channel_sub_type",
    "google_ads_goal_type",

    "device",

    "ad_network_type",

    "budget_id",
    "budget_name",
    "budget_period",
    "daily_budget",
    "lifetime_budget",
    "is_budget_limited",

    "bidding_strategy_type",
    "optimization_score",

    "impressions",
    "clicks",
    "ctr",

    "spend",

    "average_cpc",
    "average_cpm",
    "average_cpv",
    "average_cost",
    "average_cpe",

    "interactions",
    "interaction_rate",

    "engagements",
    "engagement_rate",

    "video_views",
    "view_rate",

    "video_quartile_p25_rate",
    "video_quartile_p50_rate",
    "video_quartile_p75_rate",
    "video_quartile_p100_rate",

    "absolute_top_impression_percentage",
    "top_impression_percentage",

    "search_impression_share",
    "search_absolute_top_impression_share",
    "search_top_impression_share",

    "search_budget_lost_impression_share",
    "search_budget_lost_absolute_top_impression_share",
    "search_budget_lost_top_impression_share",

    "search_rank_lost_impression_share",
    "search_rank_lost_absolute_top_impression_share",
    "search_rank_lost_top_impression_share",

    "content_impression_share",

    "active_view_viewable_impressions",
    "active_view_viewability",

    "active_view_cpm",
    "active_view_ctr",
    "active_view_measurability",
    "active_view_measurable_cost",
    "active_view_measurable_impressions",

    "active_view_audibility_measurable_impressions",
    "active_view_audibility_measurable_impressions_rate",

    "active_view_audibility_invalid_measurable_impressions_rate",
    "active_view_audibility_invalid_givt_measurable_impressions_rate",

    "active_view_audible_impressions",
    "active_view_audible_impressions_rate",

    "active_view_audible_quartile_p25_rate",
    "active_view_audible_quartile_p50_rate",
    "active_view_audible_quartile_p75_rate",
    "active_view_audible_quartile_p100_rate",

    "active_view_audible_two_seconds_impressions",
    "active_view_audible_two_seconds_impressions_rate",

    "active_view_audible_thirty_seconds_impressions",
    "active_view_audible_thirty_seconds_impressions_rate",

    "conversions",
    "conversion_rate",
    "cost_per_conversion",

    "conversions_value",

    "all_conversions",
    "all_conversions_value",

    "conversions_by_conversion_date",
    "conversions_value_by_conversion_date",

    "all_conversions_by_conversion_date",
    "all_conversions_value_by_conversion_date",

    "all_conversion_rate",
    "cost_per_all_conversions",

    "value_per_conversion",
    "value_per_conversions_by_conversion_date",

    "value_per_all_conversions",
    "value_per_all_conversions_by_conversion_date",

    "view_through_conversions",

    "loaded_at",
]


DAILY_TABLE_COLUMNS = [
    "date_start",
    "date_stop",

    "customer_id",
    "customer_name",

    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_primary_status",
    "campaign_primary_status_reasons_json",
    "advertising_channel_type",
    "advertising_channel_sub_type",
    "google_ads_goal_type",

    "ad_group_id",
    "ad_group_name",
    "ad_group_status",
    "ad_group_type",

    "ad_id",
    "ad_name",
    "ad_type",
    "ad_status",

    "landing_page_url",

    "device",

    "ad_network_type",

    "budget_id",
    "budget_name",
    "budget_period",
    "daily_budget",
    "lifetime_budget",
    "is_budget_limited",

    "bidding_strategy_type",
    "optimization_score",

    "impressions",
    "clicks",
    "ctr",

    "spend",

    "average_cpc",
    "average_cpm",
    "average_cpv",
    "average_cost",
    "average_cpe",

    "interactions",
    "interaction_rate",

    "engagements",
    "engagement_rate",

    "video_views",
    "view_rate",

    "video_quartile_p25_rate",
    "video_quartile_p50_rate",
    "video_quartile_p75_rate",
    "video_quartile_p100_rate",

    "video_trueview_view_rate_in_feed",
    "video_trueview_view_rate_in_stream",
    "video_trueview_view_rate_shorts",

    "absolute_top_impression_percentage",
    "top_impression_percentage",

    "active_view_viewable_impressions",
    "active_view_viewability",

    "active_view_cpm",
    "active_view_ctr",
    "active_view_measurability",
    "active_view_measurable_cost",
    "active_view_measurable_impressions",

    "active_view_audibility_measurable_impressions",
    "active_view_audibility_measurable_impressions_rate",

    "active_view_audibility_invalid_measurable_impressions_rate",
    "active_view_audibility_invalid_givt_measurable_impressions_rate",

    "active_view_audible_impressions",
    "active_view_audible_impressions_rate",

    "active_view_audible_quartile_p25_rate",
    "active_view_audible_quartile_p50_rate",
    "active_view_audible_quartile_p75_rate",
    "active_view_audible_quartile_p100_rate",

    "active_view_audible_two_seconds_impressions",
    "active_view_audible_two_seconds_impressions_rate",

    "active_view_audible_thirty_seconds_impressions",
    "active_view_audible_thirty_seconds_impressions_rate",

    "conversions",
    "conversion_rate",
    "cost_per_conversion",

    "conversions_value",

    "all_conversions",
    "all_conversions_value",

    "all_conversion_rate",
    "cost_per_all_conversions",

    "conversions_by_conversion_date",
    "conversions_value_by_conversion_date",

    "all_conversions_by_conversion_date",
    "all_conversions_value_by_conversion_date",

    "value_per_conversion",
    "value_per_conversions_by_conversion_date",

    "value_per_all_conversions",
    "value_per_all_conversions_by_conversion_date",

    "view_through_conversions",
    "cross_device_conversions",

    "current_model_attributed_conversions",
    "current_model_attributed_conversions_value",
    "cost_per_current_model_attributed_conversion",
    "value_per_current_model_attributed_conversion",

    "platform_comparable_conversions",
    "platform_comparable_conversions_by_conversion_date",
    "platform_comparable_conversions_from_interactions_rate",
    "platform_comparable_conversions_from_interactions_value_per_interaction",
    "platform_comparable_conversions_value",
    "platform_comparable_conversions_value_by_conversion_date",
    "platform_comparable_conversions_value_per_cost",

    "cost_converted_currency_per_platform_comparable_conversion",
    "cost_per_platform_comparable_conversion",
    "value_per_platform_comparable_conversion",
    "value_per_platform_comparable_conversions_by_conversion_date",

    "orders",
    "revenue",
    "units_sold",

    "average_cart_size",
    "average_order_value",

    "cost_of_goods_sold",
    "gross_profit",
    "gross_profit_margin",

    "cross_sell_cost_of_goods_sold",
    "cross_sell_gross_profit",
    "cross_sell_revenue",
    "cross_sell_units_sold",

    "lead_cost_of_goods_sold",
    "lead_gross_profit",
    "lead_revenue",
    "lead_units_sold",

    "all_orders",
    "all_revenue",
    "all_units_sold",

    "all_average_cart_size",
    "all_average_order_value",

    "all_cost_of_goods_sold",
    "all_gross_profit",
    "all_gross_profit_margin",

    "all_cross_sell_cost_of_goods_sold",
    "all_cross_sell_gross_profit",
    "all_cross_sell_revenue",
    "all_cross_sell_units_sold",

    "all_lead_cost_of_goods_sold",
    "all_lead_gross_profit",
    "all_lead_revenue",
    "all_lead_units_sold",

    "gmail_forwards",
    "gmail_saves",
    "gmail_secondary_clicks",

    "loaded_at",
]


DAILY_GEO_TABLE_COLUMNS = [
    "date_start",
    "date_stop",

    "customer_id",
    "customer_name",

    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_primary_status",
    "campaign_primary_status_reasons_json",
    "advertising_channel_type",
    "advertising_channel_sub_type",
    "google_ads_goal_type",

    "geo_location_name",
    "geo_country_code",

    "location_type",
    "geo_country_criterion_id",
    "geo_country_name",
    "geo_region_criterion_id",
    "geo_region_name",
    "geo_city_criterion_id",
    "geo_city_name",
    "targeted_locations_json",

    "device",

    "ad_network_type",

    "budget_id",
    "budget_name",
    "budget_period",
    "daily_budget",
    "lifetime_budget",
    "is_budget_limited",

    "bidding_strategy_type",
    "optimization_score",

    "impressions",
    "clicks",
    "ctr",

    "spend",

    "average_cpc",
    "average_cpm",
    "average_cpv",
    "average_cost",

    "interactions",
    "interaction_rate",

    "video_views",
    "view_rate",

    "absolute_top_impression_percentage",
    "top_impression_percentage",

    "conversions",
    "conversion_rate",
    "cost_per_conversion",

    "conversions_value",

    "all_conversions",
    "all_conversions_value",

    "all_conversion_rate",
    "cost_per_all_conversions",

    "conversions_by_conversion_date",
    "conversions_value_by_conversion_date",

    "all_conversions_by_conversion_date",
    "all_conversions_value_by_conversion_date",

    "value_per_conversion",
    "value_per_conversions_by_conversion_date",

    "value_per_all_conversions",
    "value_per_all_conversions_by_conversion_date",

    "cross_device_conversions",
    "view_through_conversions",

    "loaded_at",
]

DAILY_CAMPAIGN_TABLE_COLUMNS = [
    "date_start",
    "date_stop",

    "customer_id",
    "customer_name",

    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_primary_status",
    "campaign_primary_status_reasons_json",
    "advertising_channel_type",
    "advertising_channel_sub_type",
    "google_ads_goal_type",

    "budget_id",
    "budget_name",
    "budget_period",
    "daily_budget",
    "lifetime_budget",
    "is_budget_limited",

    "bidding_strategy_type",
    "optimization_score",

    "reach",
    "average_impression_frequency_per_user",
    "unique_users_two_plus",
    "unique_users_three_plus",
    "unique_users_four_plus",
    "unique_users_five_plus",
    "unique_users_ten_plus",

    "impressions",
    "clicks",
    "ctr",

    "spend",

    "average_cpc",
    "average_cpm",
    "average_cpv",
    "average_cost",
    "average_cpe",

    "interactions",
    "interaction_rate",

    "engagements",
    "engagement_rate",

    "video_views",
    "view_rate",

    "video_quartile_p25_rate",
    "video_quartile_p50_rate",
    "video_quartile_p75_rate",
    "video_quartile_p100_rate",

    "video_trueview_view_rate_in_feed",
    "video_trueview_view_rate_in_stream",
    "video_trueview_view_rate_shorts",
    "average_video_watch_time_duration_millis",

    "absolute_top_impression_percentage",
    "top_impression_percentage",

    "search_impression_share",
    "search_absolute_top_impression_share",
    "search_top_impression_share",
    "search_budget_lost_impression_share",
    "search_budget_lost_absolute_top_impression_share",
    "search_budget_lost_top_impression_share",
    "search_rank_lost_impression_share",
    "search_rank_lost_absolute_top_impression_share",
    "search_rank_lost_top_impression_share",
    "search_click_share",
    "search_exact_match_impression_share",

    "content_impression_share",
    "content_budget_lost_impression_share",
    "content_rank_lost_impression_share",

    "active_view_viewable_impressions",
    "active_view_viewability",

    "active_view_cpm",
    "active_view_ctr",
    "active_view_measurability",
    "active_view_measurable_cost",
    "active_view_measurable_impressions",

    "active_view_audibility_measurable_impressions",
    "active_view_audibility_measurable_impressions_rate",

    "active_view_audibility_invalid_measurable_impressions_rate",
    "active_view_audibility_invalid_givt_measurable_impressions_rate",

    "active_view_audible_impressions",
    "active_view_audible_impressions_rate",

    "active_view_audible_quartile_p25_rate",
    "active_view_audible_quartile_p50_rate",
    "active_view_audible_quartile_p75_rate",
    "active_view_audible_quartile_p100_rate",

    "active_view_audible_two_seconds_impressions",
    "active_view_audible_two_seconds_impressions_rate",

    "active_view_audible_thirty_seconds_impressions",
    "active_view_audible_thirty_seconds_impressions_rate",

    "conversions",
    "conversion_rate",
    "cost_per_conversion",
    "conversions_value",

    "conversions_by_conversion_date",
    "conversions_value_by_conversion_date",
    "conversions_unique_query_clusters",

    "all_conversions",
    "all_conversions_value",
    "all_conversion_rate",
    "cost_per_all_conversions",

    "all_conversions_by_conversion_date",
    "all_conversions_value_by_conversion_date",

    "value_per_conversion",
    "value_per_conversions_by_conversion_date",
    "value_per_all_conversions",
    "value_per_all_conversions_by_conversion_date",

    "cross_device_conversions",
    "cross_device_conversions_by_conversion_date",
    "cross_device_conversions_value_by_conversion_date",
    "cross_device_conversions_value",

    "view_through_conversions",

    "current_model_attributed_conversions",
    "current_model_attributed_conversions_value",
    "current_model_attributed_conversions_from_interactions_rate",
    "current_model_attributed_conversions_from_interactions_value_per_interaction",  # noqa: E501
    "current_model_attributed_conversions_value_per_cost",
    "cost_per_current_model_attributed_conversion",
    "value_per_current_model_attributed_conversion",

    "platform_comparable_conversions",
    "platform_comparable_conversions_by_conversion_date",
    "platform_comparable_conversions_from_interactions_rate",
    "platform_comparable_conversions_from_interactions_value_per_interaction",
    "platform_comparable_conversions_value",
    "platform_comparable_conversions_value_by_conversion_date",
    "platform_comparable_conversions_value_per_cost",

    "cost_converted_currency_per_platform_comparable_conversion",
    "cost_per_platform_comparable_conversion",
    "value_per_platform_comparable_conversion",
    "value_per_platform_comparable_conversions_by_conversion_date",

    "orders",
    "revenue",
    "units_sold",

    "average_cart_size",
    "average_order_value",

    "cost_of_goods_sold",
    "gross_profit",
    "gross_profit_margin",

    "cross_sell_cost_of_goods_sold",
    "cross_sell_gross_profit",
    "cross_sell_revenue",
    "cross_sell_units_sold",

    "lead_cost_of_goods_sold",
    "lead_gross_profit",
    "lead_revenue",
    "lead_units_sold",

    "all_orders",
    "all_revenue",
    "all_units_sold",

    "all_average_cart_size",
    "all_average_order_value",

    "all_cost_of_goods_sold",
    "all_gross_profit",
    "all_gross_profit_margin",

    "all_cross_sell_cost_of_goods_sold",
    "all_cross_sell_gross_profit",
    "all_cross_sell_revenue",
    "all_cross_sell_units_sold",

    "all_lead_cost_of_goods_sold",
    "all_lead_gross_profit",
    "all_lead_revenue",
    "all_lead_units_sold",

    "new_customer_lifetime_value",
    "all_new_customer_lifetime_value",

    "phone_calls",
    "phone_impressions",
    "phone_through_rate",

    "gmail_forwards",
    "gmail_saves",
    "gmail_secondary_clicks",

    "bounce_rate",
    "average_page_views",
    "average_time_on_site",
    "percent_new_visitors",

    "invalid_clicks",
    "invalid_click_rate",
    "general_invalid_clicks",
    "general_invalid_click_rate",

    "clicks_unique_query_clusters",
    "impressions_unique_query_clusters",

    "coviewed_impressions",
    "primary_impressions",
    "relative_ctr",

    "publisher_organic_clicks",
    "publisher_purchased_clicks",
    "publisher_unknown_clicks",

    "sk_ad_network_installs",
    "sk_ad_network_total_conversions",

    "biddable_app_install_conversions",
    "biddable_app_post_install_conversions",
    "biddable_cohort_app_post_install_conversions",

    "average_target_cpa",
    "average_target_roas",

    "eligible_impressions_from_location_asset_store_reach",

    "all_conversions_from_click_to_call",
    "all_conversions_from_directions",
    "all_conversions_from_menu",
    "all_conversions_from_order",
    "all_conversions_from_other_engagement",
    "all_conversions_from_store_visit",
    "all_conversions_from_store_website",

    "all_conversions_from_location_asset_click_to_call",
    "all_conversions_from_location_asset_directions",
    "all_conversions_from_location_asset_menu",
    "all_conversions_from_location_asset_order",
    "all_conversions_from_location_asset_other_engagement",
    "all_conversions_from_location_asset_store_visits",
    "all_conversions_from_location_asset_website",

    "view_through_conversions_from_location_asset_click_to_call",
    "view_through_conversions_from_location_asset_directions",
    "view_through_conversions_from_location_asset_menu",
    "view_through_conversions_from_location_asset_order",
    "view_through_conversions_from_location_asset_other_engagement",
    "view_through_conversions_from_location_asset_store_visits",
    "view_through_conversions_from_location_asset_website",

    "loaded_at",
]

DAILY_SEARCH_TERM_TABLE_COLUMNS = [
    "date_start",
    "date_stop",

    "customer_id",
    "customer_name",

    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_primary_status",
    "campaign_primary_status_reasons_json",
    "advertising_channel_type",
    "advertising_channel_sub_type",
    "google_ads_goal_type",

    "ad_group_id",
    "ad_group_name",
    "ad_group_status",
    "ad_group_type",

    "search_term",
    "search_term_status",

    "keyword_ad_group_criterion_id",
    "keyword_text",
    "keyword_match_type",

    "device",

    "ad_network_type",

    "budget_id",
    "budget_name",
    "budget_period",
    "daily_budget",
    "lifetime_budget",
    "is_budget_limited",

    "bidding_strategy_type",
    "optimization_score",

    "impressions",
    "clicks",
    "ctr",

    "spend",

    "average_cpc",
    "average_cpm",
    "average_cost",

    "conversions",
    "conversion_rate",
    "cost_per_conversion",

    "conversions_value",

    "all_conversions",
    "all_conversions_value",
    "all_conversion_rate",
    "cost_per_all_conversions",

    "loaded_at",
]

CREATIVE_ASSET_COLUMNS = [
    "source_type",

    "customer_id",
    "customer_name",

    "campaign_id",
    "campaign_name",
    "campaign_status",
    "advertising_channel_type",
    "advertising_channel_sub_type",

    "ad_group_id",
    "ad_group_name",
    "ad_group_status",

    "ad_id",
    "ad_name",
    "ad_type",
    "ad_status",

    "asset_group_id",
    "asset_group_name",
    "asset_group_status",
    "asset_group_strength",
    "asset_group_asset_status",

    "asset_id",
    "asset_name",
    "asset_type",
    "asset_field_type",

    "image_url",
    "image_width",
    "image_height",
    "image_mime_type",
    "image_file_size",

    "youtube_video_id",
    "youtube_video_url",
    "youtube_video_title",

    "loaded_at",
]

GENDER_DAILY_TABLE_COLUMNS = [
    "date_start",
    "date_stop",

    "customer_id",
    "customer_name",

    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_primary_status",
    "advertising_channel_type",
    "advertising_channel_sub_type",
    "google_ads_goal_type",

    "ad_group_id",
    "ad_group_name",
    "ad_group_status",
    "ad_group_type",

    "gender_criterion_id",
    "gender_type",
    "gender_status",

    "device",
    "ad_network_type",

    "impressions",
    "clicks",
    "ctr",

    "spend",
    "average_cpc",
    "average_cpm",
    "average_cost",

    "interactions",
    "interaction_rate",

    "engagements",
    "engagement_rate",

    "video_views",
    "view_rate",

    "conversions",
    "conversion_rate",
    "cost_per_conversion",
    "conversions_value",

    "all_conversions",
    "all_conversions_value",
    "all_conversion_rate",
    "cost_per_all_conversions",

    "view_through_conversions",

    "loaded_at",
]


RAW_COLUMNS = [
    "raw_id",
    "source",
    "api_type",
    "customer_id",
    "query_name",
    "query_text",
    "response_json",
    "request_params",
    "fetched_at",
]


# ============================================================
# Client — теперь указываем STAGING БД по умолчанию
# ============================================================

def get_client(database: str | None = None):
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=database or config.CLICKHOUSE_STAGING_DB,
    )


def get_raw_client():
    """Клиент для записи raw_data — всегда в RAW БД."""
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_RAW_DB,
    )


# ============================================================
# RAW insert
# ============================================================

def insert_raw_data(
    *,
    customer_id: str,
    query_name: str,
    query_text: str,
    response_data: dict[str, Any],
    request_params: dict[str, Any],
) -> None:
    client = get_raw_client()

    row = [
        str(uuid.uuid4()),
        "google_ads",
        "google_ads_api",
        customer_id,
        query_name,
        query_text,
        json.dumps(response_data, ensure_ascii=False, default=str),
        json.dumps(request_params, ensure_ascii=False, default=str),
        datetime.now(),
    ]

    client.insert(
        "raw_data",
        [row],
        column_names=RAW_COLUMNS,
    )


# ============================================================
# RAW read — источник для staging-загрузки
# ============================================================

def read_raw(
    *,
    query_name: str,
    customer_id: str,
    date_since: str | None = None,
    date_until: str | None = None,
) -> list[dict[str, Any]]:
    """
    Читает сырые строки из raw_data.

    Каждый fetch пишет одну запись raw_data (response_json со
    списком строк под ключом "rows"). Берём самую свежую запись
    за период и возвращаем её список строк.
    """
    client = get_raw_client()

    conditions = [
        f"query_name = '{query_name}'",
        f"customer_id = '{customer_id}'",
    ]

    if date_since and date_until:
        conditions.append(
            "JSONExtractString(request_params, 'date_since') = "
            f"'{date_since}'"
        )
        conditions.append(
            "JSONExtractString(request_params, 'date_until') = "
            f"'{date_until}'"
        )

    where_sql = " AND ".join(conditions)

    sql = f"""
    SELECT response_json
    FROM raw_data
    WHERE {where_sql}
    ORDER BY fetched_at DESC
    LIMIT 1
    """

    result = client.query(sql)

    if not result.result_rows:
        return []

    payload = json.loads(result.result_rows[0][0])

    return payload.get("rows", [])


# ============================================================
# DELETE helpers — теперь используют STAGING БД
# ============================================================

def _delete_tables_for_period(
    *,
    table_names: list[str],
    customer_id: str,
    date_since: str,
    date_until: str,
    database: str | None = None,
) -> None:
    client = get_client(database)
    db = database or config.CLICKHOUSE_STAGING_DB

    date_start = f"{date_since} 00:00:00"
    date_until_exclusive = (
        datetime.fromisoformat(date_until) + timedelta(days=1)
    ).strftime("%Y-%m-%d 00:00:00")

    for table_name in table_names:
        sql = f"""
        ALTER TABLE {db}.{table_name}
        DELETE WHERE customer_id = '{customer_id}'
          AND date_start >= toDateTime('{date_start}', 'Asia/Almaty')
          AND date_start < toDateTime('{date_until_exclusive}', 'Asia/Almaty')
        """
        client.command(sql)


def delete_goal_tables_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> None:
    _delete_tables_for_period(
        table_names=HOURLY_TABLES,
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )


def delete_daily_tables_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> None:
    _delete_tables_for_period(
        table_names=DAILY_TABLES,
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )


def delete_daily_geo_table_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> None:
    _delete_tables_for_period(
        table_names=[DAILY_GEO_TABLE],
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )


def delete_daily_campaign_tables_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> None:
    _delete_tables_for_period(
        table_names=DAILY_CAMPAIGN_TABLES,
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )


def delete_daily_search_term_table_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> None:
    _delete_tables_for_period(
        table_names=[DAILY_SEARCH_TERM_TABLE],
        customer_id=customer_id,
        date_since=date_since,
        date_until=date_until,
    )


def delete_creative_assets_for_customer(
    *,
    customer_id: str,
) -> None:
    client = get_client()

    sql = f"""
    ALTER TABLE {config.CLICKHOUSE_STAGING_DB}.{CREATIVE_ASSET_TABLE}
    DELETE WHERE customer_id = '{customer_id}'
    """
    client.command(sql)


def delete_gender_daily_table_for_period(
    *,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> None:
    client = get_client()

    query = f"""
    ALTER TABLE {config.CLICKHOUSE_STAGING_DB}.{GENDER_DAILY_TABLE}
    DELETE
    WHERE customer_id = %(customer_id)s
    AND toDate(date_start) BETWEEN toDate(%(date_since)s)
    AND toDate(%(date_until)s)
    """

    client.command(
        query,
        parameters={
            "customer_id": customer_id,
            "date_since": date_since,
            "date_until": date_until,
        },
    )


# ============================================================
# INSERT helpers — теперь используют STAGING БД
# ============================================================

def insert_goal_rows(
    *,
    table_name: str,
    rows: list[list[Any]],
) -> None:
    if not rows:
        return

    if table_name not in _HOURLY_TABLES_SET:
        raise ValueError(f"Unknown hourly staging table: {table_name}")

    client = get_client()
    client.insert(
        table_name,
        rows,
        column_names=HOURLY_TABLE_COLUMNS,
    )


def insert_daily_rows(
    *,
    table_name: str,
    rows: list[list[Any]],
) -> None:
    if not rows:
        return

    if table_name not in _DAILY_TABLES_SET:
        raise ValueError(f"Unknown daily staging table: {table_name}")

    client = get_client()
    client.insert(
        table_name,
        rows,
        column_names=DAILY_TABLE_COLUMNS,
    )


def insert_daily_geo_rows(
    *,
    rows: list[list[Any]],
) -> None:
    if not rows:
        return

    client = get_client()
    client.insert(
        DAILY_GEO_TABLE,
        rows,
        column_names=DAILY_GEO_TABLE_COLUMNS,
    )


def insert_daily_campaign_rows(
    *,
    table_name: str,
    rows: list[list[Any]],
) -> None:
    if not rows:
        return

    if table_name not in _DAILY_CAMPAIGN_TABLES_SET:
        raise ValueError(f"Unknown daily_campaign staging table: {table_name}")

    client = get_client()
    client.insert(
        table_name,
        rows,
        column_names=DAILY_CAMPAIGN_TABLE_COLUMNS,
    )


def insert_daily_search_term_rows(
    *,
    rows: list[list[Any]],
) -> None:
    if not rows:
        return

    client = get_client()
    client.insert(
        DAILY_SEARCH_TERM_TABLE,
        rows,
        column_names=DAILY_SEARCH_TERM_TABLE_COLUMNS,
    )


def insert_creative_asset_rows(
    *,
    rows: list[list[Any]],
) -> None:
    if not rows:
        return

    client = get_client()
    client.insert(
        CREATIVE_ASSET_TABLE,
        rows,
        column_names=CREATIVE_ASSET_COLUMNS,
    )


def insert_gender_daily_rows(
    *,
    rows: list[list],
) -> None:
    if not rows:
        return

    client = get_client()
    client.insert(
        table=f"{config.CLICKHOUSE_STAGING_DB}.{GENDER_DAILY_TABLE}",
        data=rows,
        column_names=GENDER_DAILY_TABLE_COLUMNS,
    )
