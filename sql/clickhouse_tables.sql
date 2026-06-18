-- ============================================================
-- clickhouse_tables.sql
--
-- Создание всех баз и таблиц для Google Ads ETL-пайплайна.
--
-- Базы:
--   google_ads_raw      — сырые API-ответы (append-only)
--   google_ads_staging  — промежуточные таблицы по целям (upsert по периоду)
--   google_ads_core     — итоговые аналитические таблицы
--   etl_metadata        — метаданные запусков пайплайна
-- ============================================================


-- ============================================================
-- DATABASE: google_ads_raw
-- ============================================================

CREATE DATABASE IF NOT EXISTS google_ads_raw;

CREATE TABLE IF NOT EXISTS google_ads_raw.raw_data
(
    raw_id          String,
    source          String,
    api_type        String,
    customer_id     String,
    query_name      String,
    query_text      String,
    response_json   String,
    request_params  String,
    fetched_at      DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (fetched_at, customer_id, query_name);


-- ============================================================
-- DATABASE: google_ads_staging
-- ============================================================

CREATE DATABASE IF NOT EXISTS google_ads_staging;


-- ------------------------------------------------------------
-- 1. HOURLY CAMPAIGN LEVEL
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_website_traffic_hourly_campaign_level_staging
(
    date_start      DateTime('Asia/Almaty'),
    date_stop       DateTime('Asia/Almaty'),

    customer_id     String,
    customer_name   Nullable(String),

    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type    Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type        Nullable(String),

    device          String,
    ad_network_type String,

    budget_id       Nullable(String),
    budget_name     Nullable(String),
    budget_period   Nullable(String),
    daily_budget    Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type   Nullable(String),
    optimization_score      Nullable(Float64),

    impressions     Nullable(UInt64),
    clicks          Nullable(UInt64),
    ctr             Nullable(Float64),
    spend           Nullable(Float64),

    average_cpc     Nullable(Float64),
    average_cpm     Nullable(Float64),
    average_cpv     Nullable(Float64),
    average_cost    Nullable(Float64),
    average_cpe     Nullable(Float64),

    interactions    Nullable(UInt64),
    interaction_rate Nullable(Float64),
    engagements     Nullable(UInt64),
    engagement_rate Nullable(Float64),

    video_views     Nullable(UInt64),
    view_rate       Nullable(Float64),
    video_quartile_p25_rate  Nullable(Float64),
    video_quartile_p50_rate  Nullable(Float64),
    video_quartile_p75_rate  Nullable(Float64),
    video_quartile_p100_rate Nullable(Float64),

    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage          Nullable(Float64),

    search_impression_share                          Nullable(Float64),
    search_absolute_top_impression_share             Nullable(Float64),
    search_top_impression_share                      Nullable(Float64),
    search_budget_lost_impression_share              Nullable(Float64),
    search_budget_lost_absolute_top_impression_share Nullable(Float64),
    search_budget_lost_top_impression_share          Nullable(Float64),
    search_rank_lost_impression_share                Nullable(Float64),
    search_rank_lost_absolute_top_impression_share   Nullable(Float64),
    search_rank_lost_top_impression_share            Nullable(Float64),
    content_impression_share                         Nullable(Float64),

    active_view_viewable_impressions    Nullable(UInt64),
    active_view_viewability             Nullable(Float64),
    active_view_cpm                     Nullable(Float64),
    active_view_ctr                     Nullable(Float64),
    active_view_measurability           Nullable(Float64),
    active_view_measurable_cost         Nullable(Float64),
    active_view_measurable_impressions  Nullable(UInt64),

    active_view_audibility_measurable_impressions               Nullable(UInt64),
    active_view_audibility_measurable_impressions_rate          Nullable(Float64),
    active_view_audibility_invalid_measurable_impressions_rate  Nullable(Float64),
    active_view_audibility_invalid_givt_measurable_impressions_rate Nullable(Float64),
    active_view_audible_impressions                             Nullable(UInt64),
    active_view_audible_impressions_rate                        Nullable(Float64),
    active_view_audible_quartile_p25_rate                       Nullable(Float64),
    active_view_audible_quartile_p50_rate                       Nullable(Float64),
    active_view_audible_quartile_p75_rate                       Nullable(Float64),
    active_view_audible_quartile_p100_rate                      Nullable(Float64),
    active_view_audible_two_seconds_impressions                 Nullable(UInt64),
    active_view_audible_two_seconds_impressions_rate            Nullable(Float64),
    active_view_audible_thirty_seconds_impressions              Nullable(UInt64),
    active_view_audible_thirty_seconds_impressions_rate         Nullable(Float64),

    conversions             Nullable(Float64),
    conversion_rate         Nullable(Float64),
    cost_per_conversion     Nullable(Float64),
    conversions_value       Nullable(Float64),

    all_conversions         Nullable(Float64),
    all_conversions_value   Nullable(Float64),

    conversions_by_conversion_date              Nullable(Float64),
    conversions_value_by_conversion_date        Nullable(Float64),
    all_conversions_by_conversion_date          Nullable(Float64),
    all_conversions_value_by_conversion_date    Nullable(Float64),

    all_conversion_rate         Nullable(Float64),
    cost_per_all_conversions    Nullable(Float64),

    value_per_conversion                        Nullable(Float64),
    value_per_conversions_by_conversion_date    Nullable(Float64),
    value_per_all_conversions                   Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    view_through_conversions Nullable(UInt64),

    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, customer_id, campaign_id, device, ad_network_type);

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_sales_hourly_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_hourly_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_leads_hourly_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_hourly_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_app_promotion_hourly_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_hourly_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_youtube_reach_views_engagement_hourly_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_hourly_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_store_visits_promotions_hourly_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_hourly_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_no_goal_hourly_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_hourly_campaign_level_staging;


-- ------------------------------------------------------------
-- 2. DAILY AD LEVEL
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_website_traffic_daily_ad_level_staging
(
    date_start      DateTime('Asia/Almaty'),
    date_stop       DateTime('Asia/Almaty'),

    customer_id     String,
    customer_name   Nullable(String),

    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type     Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type         Nullable(String),

    ad_group_id     String,
    ad_group_name   Nullable(String),
    ad_group_status Nullable(String),
    ad_group_type   Nullable(String),

    ad_id           String,
    ad_name         Nullable(String),
    ad_type         Nullable(String),
    ad_status       Nullable(String),

    landing_page_url Nullable(String),

    device          String,
    ad_network_type String,

    budget_id       Nullable(String),
    budget_name     Nullable(String),
    budget_period   Nullable(String),
    daily_budget    Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type   Nullable(String),
    optimization_score      Nullable(Float64),

    impressions     Nullable(UInt64),
    clicks          Nullable(UInt64),
    ctr             Nullable(Float64),
    spend           Nullable(Float64),

    average_cpc     Nullable(Float64),
    average_cpm     Nullable(Float64),
    average_cpv     Nullable(Float64),
    average_cost    Nullable(Float64),
    average_cpe     Nullable(Float64),

    interactions    Nullable(UInt64),
    interaction_rate Nullable(Float64),
    engagements     Nullable(UInt64),
    engagement_rate Nullable(Float64),

    video_views     Nullable(UInt64),
    view_rate       Nullable(Float64),
    video_quartile_p25_rate  Nullable(Float64),
    video_quartile_p50_rate  Nullable(Float64),
    video_quartile_p75_rate  Nullable(Float64),
    video_quartile_p100_rate Nullable(Float64),

    video_trueview_view_rate_in_feed    Nullable(Float64),
    video_trueview_view_rate_in_stream  Nullable(Float64),
    video_trueview_view_rate_shorts     Nullable(Float64),

    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage          Nullable(Float64),

    active_view_viewable_impressions    Nullable(UInt64),
    active_view_viewability             Nullable(Float64),
    active_view_cpm                     Nullable(Float64),
    active_view_ctr                     Nullable(Float64),
    active_view_measurability           Nullable(Float64),
    active_view_measurable_cost         Nullable(Float64),
    active_view_measurable_impressions  Nullable(UInt64),

    active_view_audibility_measurable_impressions               Nullable(UInt64),
    active_view_audibility_measurable_impressions_rate          Nullable(Float64),
    active_view_audibility_invalid_measurable_impressions_rate  Nullable(Float64),
    active_view_audibility_invalid_givt_measurable_impressions_rate Nullable(Float64),
    active_view_audible_impressions                             Nullable(UInt64),
    active_view_audible_impressions_rate                        Nullable(Float64),
    active_view_audible_quartile_p25_rate                       Nullable(Float64),
    active_view_audible_quartile_p50_rate                       Nullable(Float64),
    active_view_audible_quartile_p75_rate                       Nullable(Float64),
    active_view_audible_quartile_p100_rate                      Nullable(Float64),
    active_view_audible_two_seconds_impressions                 Nullable(UInt64),
    active_view_audible_two_seconds_impressions_rate            Nullable(Float64),
    active_view_audible_thirty_seconds_impressions              Nullable(UInt64),
    active_view_audible_thirty_seconds_impressions_rate         Nullable(Float64),

    conversions             Nullable(Float64),
    conversion_rate         Nullable(Float64),
    cost_per_conversion     Nullable(Float64),
    conversions_value       Nullable(Float64),

    all_conversions         Nullable(Float64),
    all_conversions_value   Nullable(Float64),
    all_conversion_rate     Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    conversions_by_conversion_date              Nullable(Float64),
    conversions_value_by_conversion_date        Nullable(Float64),
    all_conversions_by_conversion_date          Nullable(Float64),
    all_conversions_value_by_conversion_date    Nullable(Float64),

    value_per_conversion                        Nullable(Float64),
    value_per_conversions_by_conversion_date    Nullable(Float64),
    value_per_all_conversions                   Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    view_through_conversions    Nullable(UInt64),
    cross_device_conversions    Nullable(Float64),

    current_model_attributed_conversions                Nullable(Float64),
    current_model_attributed_conversions_value          Nullable(Float64),
    cost_per_current_model_attributed_conversion        Nullable(Float64),
    value_per_current_model_attributed_conversion       Nullable(Float64),

    platform_comparable_conversions                                     Nullable(Float64),
    platform_comparable_conversions_by_conversion_date                  Nullable(Float64),
    platform_comparable_conversions_from_interactions_rate              Nullable(Float64),
    platform_comparable_conversions_from_interactions_value_per_interaction Nullable(Float64),
    platform_comparable_conversions_value                               Nullable(Float64),
    platform_comparable_conversions_value_by_conversion_date            Nullable(Float64),
    platform_comparable_conversions_value_per_cost                      Nullable(Float64),
    cost_converted_currency_per_platform_comparable_conversion          Nullable(Float64),
    cost_per_platform_comparable_conversion                             Nullable(Float64),
    value_per_platform_comparable_conversion                            Nullable(Float64),
    value_per_platform_comparable_conversions_by_conversion_date        Nullable(Float64),

    orders                  Nullable(Float64),
    revenue                 Nullable(Float64),
    units_sold              Nullable(Float64),
    average_cart_size       Nullable(Float64),
    average_order_value     Nullable(Float64),
    cost_of_goods_sold      Nullable(Float64),
    gross_profit            Nullable(Float64),
    gross_profit_margin     Nullable(Float64),

    cross_sell_cost_of_goods_sold   Nullable(Float64),
    cross_sell_gross_profit         Nullable(Float64),
    cross_sell_revenue              Nullable(Float64),
    cross_sell_units_sold           Nullable(Float64),

    lead_cost_of_goods_sold Nullable(Float64),
    lead_gross_profit       Nullable(Float64),
    lead_revenue            Nullable(Float64),
    lead_units_sold         Nullable(Float64),

    all_orders              Nullable(Float64),
    all_revenue             Nullable(Float64),
    all_units_sold          Nullable(Float64),
    all_average_cart_size   Nullable(Float64),
    all_average_order_value Nullable(Float64),
    all_cost_of_goods_sold  Nullable(Float64),
    all_gross_profit        Nullable(Float64),
    all_gross_profit_margin Nullable(Float64),

    all_cross_sell_cost_of_goods_sold   Nullable(Float64),
    all_cross_sell_gross_profit         Nullable(Float64),
    all_cross_sell_revenue              Nullable(Float64),
    all_cross_sell_units_sold           Nullable(Float64),

    all_lead_cost_of_goods_sold Nullable(Float64),
    all_lead_gross_profit       Nullable(Float64),
    all_lead_revenue            Nullable(Float64),
    all_lead_units_sold         Nullable(Float64),

    gmail_forwards          Nullable(UInt64),
    gmail_saves             Nullable(UInt64),
    gmail_secondary_clicks  Nullable(UInt64),

    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, customer_id, campaign_id, ad_group_id, ad_id, device, ad_network_type);

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_sales_daily_ad_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_ad_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_leads_daily_ad_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_ad_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_app_promotion_daily_ad_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_ad_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_youtube_reach_views_engagement_daily_ad_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_ad_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_store_visits_promotions_daily_ad_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_ad_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_no_goal_daily_ad_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_ad_level_staging;


-- ------------------------------------------------------------
-- 3. GEO DAILY REGION LEVEL
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_geo_daily_region_level_staging
(
    date_start      DateTime('Asia/Almaty'),
    date_stop       DateTime('Asia/Almaty'),

    customer_id     String,
    customer_name   Nullable(String),

    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type     Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type         Nullable(String),

    geo_location_name           Nullable(String),
    geo_country_code            Nullable(String),
    location_type               Nullable(String),
    geo_country_criterion_id    Nullable(String),
    geo_country_name            Nullable(String),
    geo_region_criterion_id     Nullable(String),
    geo_region_name             Nullable(String),
    geo_city_criterion_id       Nullable(String),
    geo_city_name               Nullable(String),
    targeted_locations_json     Nullable(String),

    device          String,
    ad_network_type String,

    budget_id       Nullable(String),
    budget_name     Nullable(String),
    budget_period   Nullable(String),
    daily_budget    Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type   Nullable(String),
    optimization_score      Nullable(Float64),

    impressions     Nullable(UInt64),
    clicks          Nullable(UInt64),
    ctr             Nullable(Float64),
    spend           Nullable(Float64),

    average_cpc     Nullable(Float64),
    average_cpm     Nullable(Float64),
    average_cpv     Nullable(Float64),
    average_cost    Nullable(Float64),

    interactions    Nullable(UInt64),
    interaction_rate Nullable(Float64),

    video_views     Nullable(UInt64),
    view_rate       Nullable(Float64),

    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage          Nullable(Float64),

    conversions             Nullable(Float64),
    conversion_rate         Nullable(Float64),
    cost_per_conversion     Nullable(Float64),
    conversions_value       Nullable(Float64),

    all_conversions         Nullable(Float64),
    all_conversions_value   Nullable(Float64),
    all_conversion_rate     Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    conversions_by_conversion_date              Nullable(Float64),
    conversions_value_by_conversion_date        Nullable(Float64),
    all_conversions_by_conversion_date          Nullable(Float64),
    all_conversions_value_by_conversion_date    Nullable(Float64),

    value_per_conversion                        Nullable(Float64),
    value_per_conversions_by_conversion_date    Nullable(Float64),
    value_per_all_conversions                   Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    cross_device_conversions    Nullable(Float64),
    view_through_conversions    Nullable(UInt64),

    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start, customer_id, campaign_id, device, ad_network_type,
    ifNull(geo_country_criterion_id, ''),
    ifNull(geo_region_criterion_id, ''),
    ifNull(geo_city_criterion_id, '')
);


-- ------------------------------------------------------------
-- 4. DAILY CAMPAIGN LEVEL
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_website_traffic_daily_campaign_level_staging
(
    date_start      DateTime('Asia/Almaty'),
    date_stop       DateTime('Asia/Almaty'),

    customer_id     String,
    customer_name   Nullable(String),

    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type     Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type         Nullable(String),

    budget_id       Nullable(String),
    budget_name     Nullable(String),
    budget_period   Nullable(String),
    daily_budget    Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type   Nullable(String),
    optimization_score      Nullable(Float64),

    reach                               Nullable(UInt64),
    average_impression_frequency_per_user Nullable(Float64),
    unique_users_two_plus               Nullable(UInt64),
    unique_users_three_plus             Nullable(UInt64),
    unique_users_four_plus              Nullable(UInt64),
    unique_users_five_plus              Nullable(UInt64),
    unique_users_ten_plus               Nullable(UInt64),

    impressions     Nullable(UInt64),
    clicks          Nullable(UInt64),
    ctr             Nullable(Float64),
    spend           Nullable(Float64),

    average_cpc     Nullable(Float64),
    average_cpm     Nullable(Float64),
    average_cpv     Nullable(Float64),
    average_cost    Nullable(Float64),
    average_cpe     Nullable(Float64),

    interactions    Nullable(UInt64),
    interaction_rate Nullable(Float64),
    engagements     Nullable(UInt64),
    engagement_rate Nullable(Float64),

    video_views     Nullable(UInt64),
    view_rate       Nullable(Float64),
    video_quartile_p25_rate  Nullable(Float64),
    video_quartile_p50_rate  Nullable(Float64),
    video_quartile_p75_rate  Nullable(Float64),
    video_quartile_p100_rate Nullable(Float64),
    video_trueview_view_rate_in_feed    Nullable(Float64),
    video_trueview_view_rate_in_stream  Nullable(Float64),
    video_trueview_view_rate_shorts     Nullable(Float64),
    average_video_watch_time_duration_millis Nullable(UInt64),

    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage          Nullable(Float64),

    search_impression_share                          Nullable(Float64),
    search_absolute_top_impression_share             Nullable(Float64),
    search_top_impression_share                      Nullable(Float64),
    search_budget_lost_impression_share              Nullable(Float64),
    search_budget_lost_absolute_top_impression_share Nullable(Float64),
    search_budget_lost_top_impression_share          Nullable(Float64),
    search_rank_lost_impression_share                Nullable(Float64),
    search_rank_lost_absolute_top_impression_share   Nullable(Float64),
    search_rank_lost_top_impression_share            Nullable(Float64),
    search_click_share                               Nullable(Float64),
    search_exact_match_impression_share              Nullable(Float64),

    content_impression_share            Nullable(Float64),
    content_budget_lost_impression_share Nullable(Float64),
    content_rank_lost_impression_share  Nullable(Float64),

    active_view_viewable_impressions    Nullable(UInt64),
    active_view_viewability             Nullable(Float64),
    active_view_cpm                     Nullable(Float64),
    active_view_ctr                     Nullable(Float64),
    active_view_measurability           Nullable(Float64),
    active_view_measurable_cost         Nullable(Float64),
    active_view_measurable_impressions  Nullable(UInt64),

    active_view_audibility_measurable_impressions               Nullable(UInt64),
    active_view_audibility_measurable_impressions_rate          Nullable(Float64),
    active_view_audibility_invalid_measurable_impressions_rate  Nullable(Float64),
    active_view_audibility_invalid_givt_measurable_impressions_rate Nullable(Float64),
    active_view_audible_impressions                             Nullable(UInt64),
    active_view_audible_impressions_rate                        Nullable(Float64),
    active_view_audible_quartile_p25_rate                       Nullable(Float64),
    active_view_audible_quartile_p50_rate                       Nullable(Float64),
    active_view_audible_quartile_p75_rate                       Nullable(Float64),
    active_view_audible_quartile_p100_rate                      Nullable(Float64),
    active_view_audible_two_seconds_impressions                 Nullable(UInt64),
    active_view_audible_two_seconds_impressions_rate            Nullable(Float64),
    active_view_audible_thirty_seconds_impressions              Nullable(UInt64),
    active_view_audible_thirty_seconds_impressions_rate         Nullable(Float64),

    conversions             Nullable(Float64),
    conversion_rate         Nullable(Float64),
    cost_per_conversion     Nullable(Float64),
    conversions_value       Nullable(Float64),
    conversions_by_conversion_date      Nullable(Float64),
    conversions_value_by_conversion_date Nullable(Float64),
    conversions_unique_query_clusters   Nullable(UInt64),

    all_conversions         Nullable(Float64),
    all_conversions_value   Nullable(Float64),
    all_conversion_rate     Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),
    all_conversions_by_conversion_date          Nullable(Float64),
    all_conversions_value_by_conversion_date    Nullable(Float64),

    value_per_conversion                        Nullable(Float64),
    value_per_conversions_by_conversion_date    Nullable(Float64),
    value_per_all_conversions                   Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    cross_device_conversions                    Nullable(Float64),
    cross_device_conversions_by_conversion_date Nullable(Float64),
    cross_device_conversions_value_by_conversion_date Nullable(Float64),
    cross_device_conversions_value              Nullable(Float64),

    view_through_conversions Nullable(UInt64),

    current_model_attributed_conversions                                    Nullable(Float64),
    current_model_attributed_conversions_value                              Nullable(Float64),
    current_model_attributed_conversions_from_interactions_rate             Nullable(Float64),
    current_model_attributed_conversions_from_interactions_value_per_interaction Nullable(Float64),
    current_model_attributed_conversions_value_per_cost                     Nullable(Float64),
    cost_per_current_model_attributed_conversion                            Nullable(Float64),
    value_per_current_model_attributed_conversion                           Nullable(Float64),

    platform_comparable_conversions                                     Nullable(Float64),
    platform_comparable_conversions_by_conversion_date                  Nullable(Float64),
    platform_comparable_conversions_from_interactions_rate              Nullable(Float64),
    platform_comparable_conversions_from_interactions_value_per_interaction Nullable(Float64),
    platform_comparable_conversions_value                               Nullable(Float64),
    platform_comparable_conversions_value_by_conversion_date            Nullable(Float64),
    platform_comparable_conversions_value_per_cost                      Nullable(Float64),
    cost_converted_currency_per_platform_comparable_conversion          Nullable(Float64),
    cost_per_platform_comparable_conversion                             Nullable(Float64),
    value_per_platform_comparable_conversion                            Nullable(Float64),
    value_per_platform_comparable_conversions_by_conversion_date        Nullable(Float64),

    orders                  Nullable(Float64),
    revenue                 Nullable(Float64),
    units_sold              Nullable(Float64),
    average_cart_size       Nullable(Float64),
    average_order_value     Nullable(Float64),
    cost_of_goods_sold      Nullable(Float64),
    gross_profit            Nullable(Float64),
    gross_profit_margin     Nullable(Float64),

    cross_sell_cost_of_goods_sold   Nullable(Float64),
    cross_sell_gross_profit         Nullable(Float64),
    cross_sell_revenue              Nullable(Float64),
    cross_sell_units_sold           Nullable(Float64),

    lead_cost_of_goods_sold Nullable(Float64),
    lead_gross_profit       Nullable(Float64),
    lead_revenue            Nullable(Float64),
    lead_units_sold         Nullable(Float64),

    all_orders              Nullable(Float64),
    all_revenue             Nullable(Float64),
    all_units_sold          Nullable(Float64),
    all_average_cart_size   Nullable(Float64),
    all_average_order_value Nullable(Float64),
    all_cost_of_goods_sold  Nullable(Float64),
    all_gross_profit        Nullable(Float64),
    all_gross_profit_margin Nullable(Float64),

    all_cross_sell_cost_of_goods_sold   Nullable(Float64),
    all_cross_sell_gross_profit         Nullable(Float64),
    all_cross_sell_revenue              Nullable(Float64),
    all_cross_sell_units_sold           Nullable(Float64),

    all_lead_cost_of_goods_sold Nullable(Float64),
    all_lead_gross_profit       Nullable(Float64),
    all_lead_revenue            Nullable(Float64),
    all_lead_units_sold         Nullable(Float64),

    new_customer_lifetime_value     Nullable(Float64),
    all_new_customer_lifetime_value Nullable(Float64),

    phone_calls         Nullable(UInt64),
    phone_impressions   Nullable(UInt64),
    phone_through_rate  Nullable(Float64),

    gmail_forwards          Nullable(UInt64),
    gmail_saves             Nullable(UInt64),
    gmail_secondary_clicks  Nullable(UInt64),

    bounce_rate             Nullable(Float64),
    average_page_views      Nullable(Float64),
    average_time_on_site    Nullable(Float64),
    percent_new_visitors    Nullable(Float64),

    invalid_clicks          Nullable(UInt64),
    invalid_click_rate      Nullable(Float64),
    general_invalid_clicks  Nullable(UInt64),
    general_invalid_click_rate Nullable(Float64),

    clicks_unique_query_clusters        Nullable(UInt64),
    impressions_unique_query_clusters   Nullable(UInt64),

    coviewed_impressions    Nullable(UInt64),
    primary_impressions     Nullable(UInt64),
    relative_ctr            Nullable(Float64),

    publisher_organic_clicks    Nullable(UInt64),
    publisher_purchased_clicks  Nullable(UInt64),
    publisher_unknown_clicks    Nullable(UInt64),

    sk_ad_network_installs          Nullable(UInt64),
    sk_ad_network_total_conversions Nullable(UInt64),

    biddable_app_install_conversions            Nullable(Float64),
    biddable_app_post_install_conversions       Nullable(Float64),
    biddable_cohort_app_post_install_conversions Nullable(Float64),

    average_target_cpa  Nullable(Float64),
    average_target_roas Nullable(Float64),

    eligible_impressions_from_location_asset_store_reach Nullable(UInt64),

    all_conversions_from_click_to_call  Nullable(Float64),
    all_conversions_from_directions     Nullable(Float64),
    all_conversions_from_menu           Nullable(Float64),
    all_conversions_from_order          Nullable(Float64),
    all_conversions_from_other_engagement Nullable(Float64),
    all_conversions_from_store_visit    Nullable(Float64),
    all_conversions_from_store_website  Nullable(Float64),

    all_conversions_from_location_asset_click_to_call   Nullable(Float64),
    all_conversions_from_location_asset_directions      Nullable(Float64),
    all_conversions_from_location_asset_menu            Nullable(Float64),
    all_conversions_from_location_asset_order           Nullable(Float64),
    all_conversions_from_location_asset_other_engagement Nullable(Float64),
    all_conversions_from_location_asset_store_visits    Nullable(Float64),
    all_conversions_from_location_asset_website         Nullable(Float64),

    view_through_conversions_from_location_asset_click_to_call   Nullable(Float64),
    view_through_conversions_from_location_asset_directions      Nullable(Float64),
    view_through_conversions_from_location_asset_menu            Nullable(Float64),
    view_through_conversions_from_location_asset_order           Nullable(Float64),
    view_through_conversions_from_location_asset_other_engagement Nullable(Float64),
    view_through_conversions_from_location_asset_store_visits    Nullable(Float64),
    view_through_conversions_from_location_asset_website         Nullable(Float64),

    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, customer_id, campaign_id);

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_sales_daily_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_leads_daily_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_app_promotion_daily_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_youtube_reach_views_engagement_daily_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_store_visits_promotions_daily_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_campaign_level_staging;

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_no_goal_daily_campaign_level_staging
AS google_ads_staging.google_ads_website_traffic_daily_campaign_level_staging;


-- ------------------------------------------------------------
-- 5. DAILY SEARCH TERM LEVEL
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_daily_search_term_level_staging
(
    date_start      DateTime('Asia/Almaty'),
    date_stop       DateTime('Asia/Almaty'),

    customer_id     String,
    customer_name   Nullable(String),

    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type     Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type         Nullable(String),

    ad_group_id     String,
    ad_group_name   Nullable(String),
    ad_group_status Nullable(String),
    ad_group_type   Nullable(String),

    search_term             String,
    search_term_status      Nullable(String),

    keyword_ad_group_criterion_id Nullable(String),
    keyword_text            Nullable(String),
    keyword_match_type      Nullable(String),

    device          String,
    ad_network_type String,

    budget_id       Nullable(String),
    budget_name     Nullable(String),
    budget_period   Nullable(String),
    daily_budget    Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type   Nullable(String),
    optimization_score      Nullable(Float64),

    impressions     Nullable(UInt64),
    clicks          Nullable(UInt64),
    ctr             Nullable(Float64),
    spend           Nullable(Float64),

    average_cpc     Nullable(Float64),
    average_cpm     Nullable(Float64),
    average_cost    Nullable(Float64),

    conversions             Nullable(Float64),
    conversion_rate         Nullable(Float64),
    cost_per_conversion     Nullable(Float64),
    conversions_value       Nullable(Float64),

    all_conversions         Nullable(Float64),
    all_conversions_value   Nullable(Float64),
    all_conversion_rate     Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start, customer_id, campaign_id, ad_group_id, search_term,
    ifNull(keyword_ad_group_criterion_id, ''), device, ad_network_type
);


-- ------------------------------------------------------------
-- 6. CREATIVE ASSETS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_creative_assets_staging
(
    source_type     String,

    customer_id     String,
    customer_name   Nullable(String),

    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    advertising_channel_type     Nullable(String),
    advertising_channel_sub_type Nullable(String),

    ad_group_id     Nullable(String),
    ad_group_name   Nullable(String),
    ad_group_status Nullable(String),

    ad_id           Nullable(String),
    ad_name         Nullable(String),
    ad_type         Nullable(String),
    ad_status       Nullable(String),

    asset_group_id          Nullable(String),
    asset_group_name        Nullable(String),
    asset_group_status      Nullable(String),
    asset_group_strength    Nullable(String),
    asset_group_asset_status Nullable(String),

    asset_id        String,
    asset_name      Nullable(String),
    asset_type      Nullable(String),
    asset_field_type Nullable(String),

    image_url       Nullable(String),
    image_width     Nullable(UInt32),
    image_height    Nullable(UInt32),
    image_mime_type Nullable(String),
    image_file_size Nullable(UInt64),

    youtube_video_id    Nullable(String),
    youtube_video_url   Nullable(String),
    youtube_video_title Nullable(String),

    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
ORDER BY (
    customer_id, campaign_id, source_type,
    ifNull(ad_group_id, ''), ifNull(ad_id, ''),
    ifNull(asset_group_id, ''), asset_id,
    ifNull(asset_field_type, '')
);


-- ------------------------------------------------------------
-- 7. GENDER DAILY LEVEL
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_ads_staging.google_ads_gender_daily_level_staging
(
    date_start      DateTime('Asia/Almaty'),
    date_stop       DateTime('Asia/Almaty'),

    customer_id     String,
    customer_name   Nullable(String),

    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    advertising_channel_type     Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type         Nullable(String),

    ad_group_id     Nullable(String),
    ad_group_name   Nullable(String),
    ad_group_status Nullable(String),
    ad_group_type   Nullable(String),

    gender_criterion_id Nullable(String),
    gender_type         Nullable(String),
    gender_status       Nullable(String),

    device          String,
    ad_network_type String,

    impressions     Nullable(UInt64),
    clicks          Nullable(UInt64),
    ctr             Nullable(Float64),

    spend           Nullable(Float64),
    average_cpc     Nullable(Float64),
    average_cpm     Nullable(Float64),
    average_cost    Nullable(Float64),

    interactions    Nullable(UInt64),
    interaction_rate Nullable(Float64),
    engagements     Nullable(UInt64),
    engagement_rate Nullable(Float64),

    video_views     Nullable(UInt64),
    view_rate       Nullable(Float64),

    conversions             Nullable(Float64),
    conversion_rate         Nullable(Float64),
    cost_per_conversion     Nullable(Float64),
    conversions_value       Nullable(Float64),

    all_conversions         Nullable(Float64),
    all_conversions_value   Nullable(Float64),
    all_conversion_rate     Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    view_through_conversions Nullable(UInt64),

    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start, customer_id, campaign_id,
    ifNull(ad_group_id, ''), ifNull(gender_type, ''),
    device, ad_network_type
);


-- ============================================================
-- DATABASE: google_ads_core
-- ============================================================

CREATE DATABASE IF NOT EXISTS google_ads_core;

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_daily_campaign_level
(
    date_start              DateTime('Asia/Almaty'),
    date_stop               DateTime('Asia/Almaty'),
    campaign_id             String,
    campaign_name           Nullable(String),
    campaign_status         Nullable(String),
    campaign_effective_status Nullable(String),
    objective               Nullable(String),
    media_type              Nullable(String),
    spend                   Nullable(Float64),
    impressions             Nullable(UInt64),
    reach                   Nullable(UInt64),
    frequency               Nullable(Float64),
    cpm                     Nullable(Float64),
    clicks                  Nullable(UInt64),
    ctr                     Nullable(Float64),
    cpc                     Nullable(Float64),
    cpv                     Nullable(Float64),
    true_views              Nullable(UInt64),
    view_rate               Nullable(Float64),
    daily_budget            Nullable(Float64),
    lifetime_budget         Nullable(Float64),
    loaded_at               DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, campaign_id);

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_daily_ad_level
(
    date_start              DateTime('Asia/Almaty'),
    date_stop               DateTime('Asia/Almaty'),
    campaign_id             String,
    campaign_name           Nullable(String),
    campaign_status         Nullable(String),
    campaign_effective_status Nullable(String),
    ad_group_id             Nullable(String),
    ad_group_name           Nullable(String),
    ad_id                   Nullable(String),
    ad_name                 Nullable(String),
    landing_page_url        Nullable(String),
    google_ads_goal_type    Nullable(String),
    advertising_channel_type Nullable(String),
    spend                   Nullable(Float64),
    impressions             Nullable(UInt64),
    clicks                  Nullable(UInt64),
    video_views             Nullable(UInt64),
    daily_budget            Nullable(Float64),
    lifetime_budget         Nullable(Float64),
    loaded_at               DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, campaign_id, ifNull(ad_group_id, ''), ifNull(ad_id, ''));

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_hourly_campaign_level
(
    date_start              DateTime('Asia/Almaty'),
    date_stop               DateTime('Asia/Almaty'),
    campaign_id             String,
    campaign_name           Nullable(String),
    campaign_status         Nullable(String),
    campaign_effective_status Nullable(String),
    objective               Nullable(String),
    media_type              Nullable(String),
    device                  Nullable(String),
    spend                   Nullable(Float64),
    impressions             Nullable(UInt64),
    cpm                     Nullable(Float64),
    clicks                  Nullable(UInt64),
    ctr                     Nullable(Float64),
    cpc                     Nullable(Float64),
    cpv                     Nullable(Float64),
    true_views              Nullable(UInt64),
    view_rate               Nullable(Float64),
    daily_budget            Nullable(Float64),
    lifetime_budget         Nullable(Float64),
    loaded_at               DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, campaign_id, ifNull(device, ''));

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_geo_daily_level
(
    date_start              DateTime('Asia/Almaty'),
    date_stop               DateTime('Asia/Almaty'),
    campaign_id             String,
    campaign_name           Nullable(String),
    campaign_status         Nullable(String),
    campaign_effective_status Nullable(String),
    objective               Nullable(String),
    country                 Nullable(String),
    region                  Nullable(String),
    city                    Nullable(String),
    spend                   Nullable(Float64),
    impressions             Nullable(UInt64),
    cpm                     Nullable(Float64),
    clicks                  Nullable(UInt64),
    ctr                     Nullable(Float64),
    cpc                     Nullable(Float64),
    cpv                     Nullable(Float64),
    true_views              Nullable(UInt64),
    view_rate               Nullable(Float64),
    loaded_at               DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, campaign_id, ifNull(country, ''), ifNull(region, ''), ifNull(city, ''));

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_device_daily_level
(
    date_start              DateTime('Asia/Almaty'),
    date_stop               DateTime('Asia/Almaty'),
    campaign_id             String,
    campaign_status         Nullable(String),
    campaign_effective_status Nullable(String),
    objective               Nullable(String),
    device                  Nullable(String),
    spend                   Nullable(Float64),
    impressions             Nullable(UInt64),
    cpm                     Nullable(Float64),
    clicks                  Nullable(UInt64),
    ctr                     Nullable(Float64),
    cpc                     Nullable(Float64),
    cpv                     Nullable(Float64),
    true_views              Nullable(UInt64),
    view_rate               Nullable(Float64),
    loaded_at               DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, campaign_id, ifNull(device, ''));

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_gender_daily_level
(
    date_start              DateTime('Asia/Almaty'),
    date_stop               DateTime('Asia/Almaty'),
    campaign_id             String,
    campaign_status         Nullable(String),
    campaign_effective_status Nullable(String),
    objective               Nullable(String),
    gender_type             Nullable(String),
    device                  Nullable(String),
    spend                   Nullable(Float64),
    impressions             Nullable(UInt64),
    cpm                     Nullable(Float64),
    clicks                  Nullable(UInt64),
    ctr                     Nullable(Float64),
    cpc                     Nullable(Float64),
    cpv                     Nullable(Float64),
    true_views              Nullable(UInt64),
    view_rate               Nullable(Float64),
    loaded_at               DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, campaign_id, ifNull(gender_type, ''), ifNull(device, ''));

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_search_term_daily_level
(
    date_start              DateTime('Asia/Almaty'),
    date_stop               DateTime('Asia/Almaty'),
    campaign_id             String,
    campaign_name           Nullable(String),
    campaign_status         Nullable(String),
    campaign_effective_status Nullable(String),
    search_term             String,
    keyword_text            Nullable(String),
    loaded_at               DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (date_start, campaign_id, search_term);

CREATE TABLE IF NOT EXISTS google_ads_core.google_ads_core_creative_assets
(
    source_type     String,
    customer_id     String,
    customer_name   Nullable(String),
    campaign_id     String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    advertising_channel_type     Nullable(String),
    advertising_channel_sub_type Nullable(String),
    ad_group_id     Nullable(String),
    ad_group_name   Nullable(String),
    ad_group_status Nullable(String),
    ad_id           Nullable(String),
    ad_name         Nullable(String),
    ad_type         Nullable(String),
    ad_status       Nullable(String),
    asset_group_id          Nullable(String),
    asset_group_name        Nullable(String),
    asset_group_status      Nullable(String),
    asset_group_strength    Nullable(String),
    asset_group_asset_status Nullable(String),
    asset_id        String,
    asset_name      Nullable(String),
    asset_type      Nullable(String),
    asset_field_type Nullable(String),
    image_url       Nullable(String),
    image_width     Nullable(UInt32),
    image_height    Nullable(UInt32),
    image_mime_type Nullable(String),
    image_file_size Nullable(UInt64),
    youtube_video_id    Nullable(String),
    youtube_video_url   Nullable(String),
    youtube_video_title Nullable(String),
    loaded_at       DateTime('Asia/Almaty')
)
ENGINE = MergeTree
ORDER BY (
    customer_id, campaign_id, source_type,
    ifNull(ad_group_id, ''), ifNull(ad_id, ''),
    ifNull(asset_group_id, ''), asset_id,
    ifNull(asset_field_type, '')
);


-- ============================================================
-- DATABASE: etl_metadata
-- ============================================================

CREATE DATABASE IF NOT EXISTS etl_metadata;

CREATE TABLE IF NOT EXISTS etl_metadata.etl_runs
(
    run_id              String,
    pipeline_name       String,
    source_platform     String,
    run_type            String,
    status              String,
    requested_date_since Date,
    requested_date_until Date,
    actual_min_date     Nullable(Date),
    actual_max_date     Nullable(Date),
    started_at          DateTime('Asia/Almaty'),
    finished_at         Nullable(DateTime('Asia/Almaty')),
    duration_seconds    Nullable(Int32),
    total_raw_rows      Int64,
    total_staging_rows  Int64,
    total_core_rows     Int64,
    error_stage         Nullable(String),
    error_message       Nullable(String),
    error_trace         Nullable(String)
)
ENGINE = MergeTree
ORDER BY (started_at, run_id);

CREATE TABLE IF NOT EXISTS etl_metadata.etl_step_runs
(
    run_id          String,
    pipeline_name   String,
    source_platform String,
    step_name       String,
    step_order      Int32,
    status          String,
    started_at      DateTime('Asia/Almaty'),
    finished_at     Nullable(DateTime('Asia/Almaty')),
    duration_seconds Nullable(Int32),
    input_rows      Int64,
    output_rows     Int64,
    target_database Nullable(String),
    target_table    Nullable(String),
    error_message   Nullable(String),
    error_trace     Nullable(String)
)
ENGINE = MergeTree
ORDER BY (started_at, run_id, step_order);

CREATE TABLE IF NOT EXISTS etl_metadata.etl_table_loads
(
    run_id          String,
    pipeline_name   String,
    source_platform String,
    layer           String,
    database_name   String,
    table_name      String,
    date_since      Date,
    date_until      Date,
    rows_before     Int64,
    rows_deleted    Int64,
    rows_inserted   Int64,
    rows_after      Int64,
    min_loaded_date Nullable(Date),
    max_loaded_date Nullable(Date),
    loaded_at       DateTime('Asia/Almaty')
)
ENGINE = MergeTree
ORDER BY (loaded_at, run_id, table_name);

CREATE TABLE IF NOT EXISTS etl_metadata.etl_data_quality_checks
(
    run_id          String,
    pipeline_name   String,
    source_platform String,
    database_name   String,
    table_name      String,
    date_since      Date,
    date_until      Date,
    check_name      String,
    check_level     String,
    status          String,
    failed_rows     Int64,
    details         Nullable(String),
    checked_at      DateTime('Asia/Almaty')
)
ENGINE = MergeTree
ORDER BY (checked_at, run_id, table_name, check_name);
