CREATE DATABASE IF NOT EXISTS google_ads;

USE google_ads;


CREATE TABLE IF NOT EXISTS raw_data
(
    raw_id UUID,

    source String,
    api_type String,

    customer_id String,
    query_name String,
    query_text String,

    response_json String,
    request_params String,

    fetched_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (source, customer_id, query_name, fetched_at);


-- ============================================================
-- 1. HOURLY CAMPAIGN LEVEL TABLES
-- Уровень: day + hour + campaign + device + network
-- Максимально заполняем почасовые данные.
-- Все дополнительные metrics.* ниже проверены с:
-- segments.date + segments.hour + segments.device + segments.ad_network_type
-- FROM campaign.
-- Без geo, без ad/ad_group, потому что API не отдаёт это по часам.
-- ============================================================

CREATE TABLE IF NOT EXISTS google_ads_website_traffic_hourly_campaign_level
(
    -- time
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    -- account
    customer_id String,
    customer_name Nullable(String),

    -- campaign
    campaign_id String,
    campaign_name Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type Nullable(String),

    -- device
    device String,

    -- network
    ad_network_type String,

    -- budget / bidding
    budget_id Nullable(String),
    budget_name Nullable(String),
    budget_period Nullable(String),
    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type Nullable(String),
    optimization_score Nullable(Float64),

    -- main metrics
    impressions Nullable(UInt64),
    clicks Nullable(UInt64),
    ctr Nullable(Float64),

    spend Nullable(Float64),

    average_cpc Nullable(Float64),
    average_cpm Nullable(Float64),
    average_cpv Nullable(Float64),
    average_cost Nullable(Float64),
    average_cpe Nullable(Float64),

    -- interactions / engagements
    interactions Nullable(UInt64),
    interaction_rate Nullable(Float64),

    engagements Nullable(UInt64),
    engagement_rate Nullable(Float64),

    -- video / YouTube
    video_views Nullable(UInt64),
    view_rate Nullable(Float64),

    video_quartile_p25_rate Nullable(Float64),
    video_quartile_p50_rate Nullable(Float64),
    video_quartile_p75_rate Nullable(Float64),
    video_quartile_p100_rate Nullable(Float64),

    -- position / impression share
    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage Nullable(Float64),

    search_impression_share Nullable(Float64),
    search_absolute_top_impression_share Nullable(Float64),
    search_top_impression_share Nullable(Float64),

    search_budget_lost_impression_share Nullable(Float64),
    search_budget_lost_absolute_top_impression_share Nullable(Float64),
    search_budget_lost_top_impression_share Nullable(Float64),

    search_rank_lost_impression_share Nullable(Float64),
    search_rank_lost_absolute_top_impression_share Nullable(Float64),
    search_rank_lost_top_impression_share Nullable(Float64),

    content_impression_share Nullable(Float64),

    -- active view
    active_view_viewable_impressions Nullable(UInt64),
    active_view_viewability Nullable(Float64),

    active_view_cpm Nullable(Float64),
    active_view_ctr Nullable(Float64),
    active_view_measurability Nullable(Float64),
    active_view_measurable_cost Nullable(Float64),
    active_view_measurable_impressions Nullable(UInt64),

    -- active view audibility
    active_view_audibility_measurable_impressions Nullable(UInt64),
    active_view_audibility_measurable_impressions_rate Nullable(Float64),

    active_view_audibility_invalid_measurable_impressions_rate Nullable(Float64),
    active_view_audibility_invalid_givt_measurable_impressions_rate Nullable(Float64),

    active_view_audible_impressions Nullable(UInt64),
    active_view_audible_impressions_rate Nullable(Float64),

    active_view_audible_quartile_p25_rate Nullable(Float64),
    active_view_audible_quartile_p50_rate Nullable(Float64),
    active_view_audible_quartile_p75_rate Nullable(Float64),
    active_view_audible_quartile_p100_rate Nullable(Float64),

    active_view_audible_two_seconds_impressions Nullable(UInt64),
    active_view_audible_two_seconds_impressions_rate Nullable(Float64),

    active_view_audible_thirty_seconds_impressions Nullable(UInt64),
    active_view_audible_thirty_seconds_impressions_rate Nullable(Float64),

    -- conversions
    conversions Nullable(Float64),
    conversion_rate Nullable(Float64),
    cost_per_conversion Nullable(Float64),

    conversions_value Nullable(Float64),

    all_conversions Nullable(Float64),
    all_conversions_value Nullable(Float64),

    -- conversions by conversion date
    conversions_by_conversion_date Nullable(Float64),
    conversions_value_by_conversion_date Nullable(Float64),

    all_conversions_by_conversion_date Nullable(Float64),
    all_conversions_value_by_conversion_date Nullable(Float64),

    -- additional conversion metrics
    all_conversion_rate Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    value_per_conversion Nullable(Float64),
    value_per_conversions_by_conversion_date Nullable(Float64),

    value_per_all_conversions Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    view_through_conversions Nullable(UInt64),

    -- service
    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start,
    customer_id,
    campaign_id,
    device,
    ad_network_type
);


CREATE TABLE IF NOT EXISTS google_ads_sales_hourly_campaign_level
AS google_ads_website_traffic_hourly_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_leads_hourly_campaign_level
AS google_ads_website_traffic_hourly_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_app_promotion_hourly_campaign_level
AS google_ads_website_traffic_hourly_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_youtube_reach_views_engagement_hourly_campaign_level
AS google_ads_website_traffic_hourly_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_store_visits_promotions_hourly_campaign_level
AS google_ads_website_traffic_hourly_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_no_goal_hourly_campaign_level
AS google_ads_website_traffic_hourly_campaign_level;


-- ============================================================
-- 2. DAILY AD LEVEL TABLES
-- Уровень: day + campaign + ad_group + ad + device + network
-- Без geo.
-- Главная задача: получать daily-данные по каждой рекламе.
-- Если в кампании 5 реклам, здесь должны быть строки по каждому ad_id.
-- Одна реклама может иметь несколько строк, если есть разные device/network.
-- Все дополнительные metrics.* ниже проверены с:
-- segments.date + segments.device + segments.ad_network_type
-- FROM ad_group_ad.
--
-- Не включаем:
-- reach / average_impression_frequency_per_user
-- search_impression_share / search_* lost share
-- content_impression_share
-- conversion_value_per_cost / all_conversions_value_per_cost
-- потому что они не проходят на уровне FROM ad_group_ad.
-- ============================================================

CREATE TABLE IF NOT EXISTS google_ads_website_traffic_daily_ad_level
(
    -- time
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    -- account
    customer_id String,
    customer_name Nullable(String),

    -- campaign
    campaign_id String,
    campaign_name Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type Nullable(String),

    -- ad group
    ad_group_id String,
    ad_group_name Nullable(String),
    ad_group_status Nullable(String),
    ad_group_type Nullable(String),

    -- ad
    ad_id String,
    ad_name Nullable(String),
    ad_type Nullable(String),
    ad_status Nullable(String),

    -- landing page
    landing_page_url Nullable(String),

    -- device
    device String,

    -- network
    ad_network_type String,

    -- budget / bidding
    budget_id Nullable(String),
    budget_name Nullable(String),
    budget_period Nullable(String),
    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type Nullable(String),
    optimization_score Nullable(Float64),

    -- main metrics
    impressions Nullable(UInt64),
    clicks Nullable(UInt64),
    ctr Nullable(Float64),

    spend Nullable(Float64),

    average_cpc Nullable(Float64),
    average_cpm Nullable(Float64),
    average_cpv Nullable(Float64),
    average_cost Nullable(Float64),
    average_cpe Nullable(Float64),

    -- interactions / engagements
    interactions Nullable(UInt64),
    interaction_rate Nullable(Float64),

    engagements Nullable(UInt64),
    engagement_rate Nullable(Float64),

    -- video / YouTube
    video_views Nullable(UInt64),
    view_rate Nullable(Float64),

    video_quartile_p25_rate Nullable(Float64),
    video_quartile_p50_rate Nullable(Float64),
    video_quartile_p75_rate Nullable(Float64),
    video_quartile_p100_rate Nullable(Float64),

    video_trueview_view_rate_in_feed Nullable(Float64),
    video_trueview_view_rate_in_stream Nullable(Float64),
    video_trueview_view_rate_shorts Nullable(Float64),

    -- position
    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage Nullable(Float64),

    -- active view
    active_view_viewable_impressions Nullable(UInt64),
    active_view_viewability Nullable(Float64),

    active_view_cpm Nullable(Float64),
    active_view_ctr Nullable(Float64),
    active_view_measurability Nullable(Float64),
    active_view_measurable_cost Nullable(Float64),
    active_view_measurable_impressions Nullable(UInt64),

    -- active view audibility
    active_view_audibility_measurable_impressions Nullable(UInt64),
    active_view_audibility_measurable_impressions_rate Nullable(Float64),

    active_view_audibility_invalid_measurable_impressions_rate Nullable(Float64),
    active_view_audibility_invalid_givt_measurable_impressions_rate Nullable(Float64),

    active_view_audible_impressions Nullable(UInt64),
    active_view_audible_impressions_rate Nullable(Float64),

    active_view_audible_quartile_p25_rate Nullable(Float64),
    active_view_audible_quartile_p50_rate Nullable(Float64),
    active_view_audible_quartile_p75_rate Nullable(Float64),
    active_view_audible_quartile_p100_rate Nullable(Float64),

    active_view_audible_two_seconds_impressions Nullable(UInt64),
    active_view_audible_two_seconds_impressions_rate Nullable(Float64),

    active_view_audible_thirty_seconds_impressions Nullable(UInt64),
    active_view_audible_thirty_seconds_impressions_rate Nullable(Float64),

    -- conversions
    conversions Nullable(Float64),
    conversion_rate Nullable(Float64),
    cost_per_conversion Nullable(Float64),

    conversions_value Nullable(Float64),

    all_conversions Nullable(Float64),
    all_conversions_value Nullable(Float64),

    all_conversion_rate Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    -- conversions by conversion date
    conversions_by_conversion_date Nullable(Float64),
    conversions_value_by_conversion_date Nullable(Float64),

    all_conversions_by_conversion_date Nullable(Float64),
    all_conversions_value_by_conversion_date Nullable(Float64),

    -- conversion value metrics
    value_per_conversion Nullable(Float64),
    value_per_conversions_by_conversion_date Nullable(Float64),

    value_per_all_conversions Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    view_through_conversions Nullable(UInt64),
    cross_device_conversions Nullable(Float64),

    -- current model attribution
    current_model_attributed_conversions Nullable(Float64),
    current_model_attributed_conversions_value Nullable(Float64),
    cost_per_current_model_attributed_conversion Nullable(Float64),
    value_per_current_model_attributed_conversion Nullable(Float64),

    -- platform comparable conversions
    platform_comparable_conversions Nullable(Float64),
    platform_comparable_conversions_by_conversion_date Nullable(Float64),
    platform_comparable_conversions_from_interactions_rate Nullable(Float64),
    platform_comparable_conversions_from_interactions_value_per_interaction Nullable(Float64),
    platform_comparable_conversions_value Nullable(Float64),
    platform_comparable_conversions_value_by_conversion_date Nullable(Float64),
    platform_comparable_conversions_value_per_cost Nullable(Float64),

    cost_converted_currency_per_platform_comparable_conversion Nullable(Float64),
    cost_per_platform_comparable_conversion Nullable(Float64),
    value_per_platform_comparable_conversion Nullable(Float64),
    value_per_platform_comparable_conversions_by_conversion_date Nullable(Float64),

    -- sales / ecommerce
    orders Nullable(Float64),
    revenue Nullable(Float64),
    units_sold Nullable(Float64),

    average_cart_size Nullable(Float64),
    average_order_value Nullable(Float64),

    cost_of_goods_sold Nullable(Float64),
    gross_profit Nullable(Float64),
    gross_profit_margin Nullable(Float64),

    -- cross-sell ecommerce
    cross_sell_cost_of_goods_sold Nullable(Float64),
    cross_sell_gross_profit Nullable(Float64),
    cross_sell_revenue Nullable(Float64),
    cross_sell_units_sold Nullable(Float64),

    -- lead ecommerce
    lead_cost_of_goods_sold Nullable(Float64),
    lead_gross_profit Nullable(Float64),
    lead_revenue Nullable(Float64),
    lead_units_sold Nullable(Float64),

    -- all sales / ecommerce
    all_orders Nullable(Float64),
    all_revenue Nullable(Float64),
    all_units_sold Nullable(Float64),

    all_average_cart_size Nullable(Float64),
    all_average_order_value Nullable(Float64),

    all_cost_of_goods_sold Nullable(Float64),
    all_gross_profit Nullable(Float64),
    all_gross_profit_margin Nullable(Float64),

    -- all cross-sell ecommerce
    all_cross_sell_cost_of_goods_sold Nullable(Float64),
    all_cross_sell_gross_profit Nullable(Float64),
    all_cross_sell_revenue Nullable(Float64),
    all_cross_sell_units_sold Nullable(Float64),

    -- all lead ecommerce
    all_lead_cost_of_goods_sold Nullable(Float64),
    all_lead_gross_profit Nullable(Float64),
    all_lead_revenue Nullable(Float64),
    all_lead_units_sold Nullable(Float64),

    -- Gmail
    gmail_forwards Nullable(UInt64),
    gmail_saves Nullable(UInt64),
    gmail_secondary_clicks Nullable(UInt64),

    -- service
    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start,
    customer_id,
    campaign_id,
    ad_group_id,
    ad_id,
    device,
    ad_network_type
);


CREATE TABLE IF NOT EXISTS google_ads_sales_daily_ad_level
AS google_ads_website_traffic_daily_ad_level;


CREATE TABLE IF NOT EXISTS google_ads_leads_daily_ad_level
AS google_ads_website_traffic_daily_ad_level;


CREATE TABLE IF NOT EXISTS google_ads_app_promotion_daily_ad_level
AS google_ads_website_traffic_daily_ad_level;


CREATE TABLE IF NOT EXISTS google_ads_youtube_reach_views_engagement_daily_ad_level
AS google_ads_website_traffic_daily_ad_level;


CREATE TABLE IF NOT EXISTS google_ads_store_visits_promotions_daily_ad_level
AS google_ads_website_traffic_daily_ad_level;


CREATE TABLE IF NOT EXISTS google_ads_no_goal_daily_ad_level
AS google_ads_website_traffic_daily_ad_level;

-- ============================================================
-- 3. GEO DAILY REGION LEVEL TABLES
-- Уровень: day + campaign + device + network + country + region + city
-- Без ad/ad_group.
-- Сюда идет geographic_view.
--
-- Не включаем:
-- reach / unique_users
-- average_impression_frequency_per_user
-- active_view_*
-- engagements
-- ecommerce / revenue / orders
-- video_quartile_*
-- conversion_value_per_cost / all_conversions_value_per_cost
-- потому что они не проходят на уровне FROM geographic_view.
-- ============================================================

CREATE TABLE IF NOT EXISTS google_ads_website_traffic_geo_daily_region_level
(
    -- time
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    -- account
    customer_id String,
    customer_name Nullable(String),

    -- campaign
    campaign_id String,
    campaign_name Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type Nullable(String),

    -- geo base
    geo_location_name Nullable(String),
    geo_country_code Nullable(String),

    -- geo details
    location_type Nullable(String),
    geo_country_criterion_id Nullable(String),
    geo_country_name Nullable(String),
    geo_region_criterion_id Nullable(String),
    geo_region_name Nullable(String),
    geo_city_criterion_id Nullable(String),
    geo_city_name Nullable(String),
    targeted_locations_json Nullable(String),

    -- device
    device String,

    -- network
    ad_network_type String,

    -- budget / bidding
    budget_id Nullable(String),
    budget_name Nullable(String),
    budget_period Nullable(String),
    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type Nullable(String),
    optimization_score Nullable(Float64),

    -- main metrics
    impressions Nullable(UInt64),
    clicks Nullable(UInt64),
    ctr Nullable(Float64),

    spend Nullable(Float64),

    average_cpc Nullable(Float64),
    average_cpm Nullable(Float64),
    average_cpv Nullable(Float64),
    average_cost Nullable(Float64),

    -- interactions
    interactions Nullable(UInt64),
    interaction_rate Nullable(Float64),

    -- video / YouTube
    video_views Nullable(UInt64),
    view_rate Nullable(Float64),

    -- position
    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage Nullable(Float64),

    -- conversions
    conversions Nullable(Float64),
    conversion_rate Nullable(Float64),
    cost_per_conversion Nullable(Float64),

    conversions_value Nullable(Float64),

    all_conversions Nullable(Float64),
    all_conversions_value Nullable(Float64),

    all_conversion_rate Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    -- conversions by conversion date
    conversions_by_conversion_date Nullable(Float64),
    conversions_value_by_conversion_date Nullable(Float64),

    all_conversions_by_conversion_date Nullable(Float64),
    all_conversions_value_by_conversion_date Nullable(Float64),

    -- conversion value metrics
    value_per_conversion Nullable(Float64),
    value_per_conversions_by_conversion_date Nullable(Float64),

    value_per_all_conversions Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    cross_device_conversions Nullable(Float64),
    view_through_conversions Nullable(UInt64),

    -- service
    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start,
    customer_id,
    campaign_id,
    device,
    ad_network_type,
    ifNull(geo_country_criterion_id, ''),
    ifNull(geo_region_criterion_id, ''),
    ifNull(geo_city_criterion_id, '')
);


CREATE TABLE IF NOT EXISTS google_ads_sales_geo_daily_region_level
AS google_ads_website_traffic_geo_daily_region_level;


CREATE TABLE IF NOT EXISTS google_ads_leads_geo_daily_region_level
AS google_ads_website_traffic_geo_daily_region_level;


CREATE TABLE IF NOT EXISTS google_ads_app_promotion_geo_daily_region_level
AS google_ads_website_traffic_geo_daily_region_level;


CREATE TABLE IF NOT EXISTS google_ads_youtube_reach_views_engagement_geo_daily_region_level
AS google_ads_website_traffic_geo_daily_region_level;


CREATE TABLE IF NOT EXISTS google_ads_store_visits_promotions_geo_daily_region_level
AS google_ads_website_traffic_geo_daily_region_level;


CREATE TABLE IF NOT EXISTS google_ads_no_goal_geo_daily_region_level
AS google_ads_website_traffic_geo_daily_region_level;

-- ============================================================
-- 4. DAILY CAMPAIGN LEVEL TABLES
-- Уровень: day + campaign
-- Без hour, device, network, geo, ad_group, ad.
--
-- Главная задача:
-- получить campaign-level daily метрики, включая reach/frequency.
--
-- Здесь можно получать:
-- reach = metrics.unique_users
-- average_impression_frequency_per_user
--
-- Не включаем:
-- conversions_value_per_cost
-- all_conversions_value_per_cost
-- auction_insight_*
-- asset_pinned_*
-- organic_*
-- hotel_*
-- message_*
-- потому что аудит показал, что они не проходят на уровне FROM campaign + segments.date.
-- ============================================================

CREATE TABLE IF NOT EXISTS google_ads_website_traffic_daily_campaign_level
(
    -- time
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    -- account
    customer_id String,
    customer_name Nullable(String),

    -- campaign
    campaign_id String,
    campaign_name Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type Nullable(String),

    -- budget / bidding
    budget_id Nullable(String),
    budget_name Nullable(String),
    budget_period Nullable(String),
    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type Nullable(String),
    optimization_score Nullable(Float64),

    -- reach / frequency
    reach Nullable(UInt64),
    average_impression_frequency_per_user Nullable(Float64),
    unique_users_two_plus Nullable(UInt64),
    unique_users_three_plus Nullable(UInt64),
    unique_users_four_plus Nullable(UInt64),
    unique_users_five_plus Nullable(UInt64),
    unique_users_ten_plus Nullable(UInt64),

    -- main metrics
    impressions Nullable(UInt64),
    clicks Nullable(UInt64),
    ctr Nullable(Float64),

    spend Nullable(Float64),

    average_cpc Nullable(Float64),
    average_cpm Nullable(Float64),
    average_cpv Nullable(Float64),
    average_cost Nullable(Float64),
    average_cpe Nullable(Float64),

    -- interactions / engagements
    interactions Nullable(UInt64),
    interaction_rate Nullable(Float64),

    engagements Nullable(UInt64),
    engagement_rate Nullable(Float64),

    -- video / YouTube
    video_views Nullable(UInt64),
    view_rate Nullable(Float64),

    video_quartile_p25_rate Nullable(Float64),
    video_quartile_p50_rate Nullable(Float64),
    video_quartile_p75_rate Nullable(Float64),
    video_quartile_p100_rate Nullable(Float64),

    video_trueview_view_rate_in_feed Nullable(Float64),
    video_trueview_view_rate_in_stream Nullable(Float64),
    video_trueview_view_rate_shorts Nullable(Float64),
    average_video_watch_time_duration_millis Nullable(UInt64),

    -- position / impression share
    absolute_top_impression_percentage Nullable(Float64),
    top_impression_percentage Nullable(Float64),

    search_impression_share Nullable(Float64),
    search_absolute_top_impression_share Nullable(Float64),
    search_top_impression_share Nullable(Float64),
    search_budget_lost_impression_share Nullable(Float64),
    search_budget_lost_absolute_top_impression_share Nullable(Float64),
    search_budget_lost_top_impression_share Nullable(Float64),
    search_rank_lost_impression_share Nullable(Float64),
    search_rank_lost_absolute_top_impression_share Nullable(Float64),
    search_rank_lost_top_impression_share Nullable(Float64),
    search_click_share Nullable(Float64),
    search_exact_match_impression_share Nullable(Float64),

    content_impression_share Nullable(Float64),
    content_budget_lost_impression_share Nullable(Float64),
    content_rank_lost_impression_share Nullable(Float64),

    -- active view
    active_view_viewable_impressions Nullable(UInt64),
    active_view_viewability Nullable(Float64),

    active_view_cpm Nullable(Float64),
    active_view_ctr Nullable(Float64),
    active_view_measurability Nullable(Float64),
    active_view_measurable_cost Nullable(Float64),
    active_view_measurable_impressions Nullable(UInt64),

    -- active view audibility
    active_view_audibility_measurable_impressions Nullable(UInt64),
    active_view_audibility_measurable_impressions_rate Nullable(Float64),

    active_view_audibility_invalid_measurable_impressions_rate Nullable(Float64),
    active_view_audibility_invalid_givt_measurable_impressions_rate Nullable(Float64),

    active_view_audible_impressions Nullable(UInt64),
    active_view_audible_impressions_rate Nullable(Float64),

    active_view_audible_quartile_p25_rate Nullable(Float64),
    active_view_audible_quartile_p50_rate Nullable(Float64),
    active_view_audible_quartile_p75_rate Nullable(Float64),
    active_view_audible_quartile_p100_rate Nullable(Float64),

    active_view_audible_two_seconds_impressions Nullable(UInt64),
    active_view_audible_two_seconds_impressions_rate Nullable(Float64),

    active_view_audible_thirty_seconds_impressions Nullable(UInt64),
    active_view_audible_thirty_seconds_impressions_rate Nullable(Float64),

    -- conversions
    conversions Nullable(Float64),
    conversion_rate Nullable(Float64),
    cost_per_conversion Nullable(Float64),
    conversions_value Nullable(Float64),

    conversions_by_conversion_date Nullable(Float64),
    conversions_value_by_conversion_date Nullable(Float64),
    conversions_unique_query_clusters Nullable(UInt64),

    all_conversions Nullable(Float64),
    all_conversions_value Nullable(Float64),
    all_conversion_rate Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    all_conversions_by_conversion_date Nullable(Float64),
    all_conversions_value_by_conversion_date Nullable(Float64),

    value_per_conversion Nullable(Float64),
    value_per_conversions_by_conversion_date Nullable(Float64),
    value_per_all_conversions Nullable(Float64),
    value_per_all_conversions_by_conversion_date Nullable(Float64),

    -- cross-device conversions
    cross_device_conversions Nullable(Float64),
    cross_device_conversions_by_conversion_date Nullable(Float64),
    cross_device_conversions_value_by_conversion_date Nullable(Float64),
    cross_device_conversions_value Nullable(Float64),

    -- view-through conversions
    view_through_conversions Nullable(UInt64),

    -- current model attribution
    current_model_attributed_conversions Nullable(Float64),
    current_model_attributed_conversions_value Nullable(Float64),
    current_model_attributed_conversions_from_interactions_rate Nullable(Float64),
    current_model_attributed_conversions_from_interactions_value_per_interaction Nullable(Float64),
    current_model_attributed_conversions_value_per_cost Nullable(Float64),
    cost_per_current_model_attributed_conversion Nullable(Float64),
    value_per_current_model_attributed_conversion Nullable(Float64),

    -- platform comparable conversions
    platform_comparable_conversions Nullable(Float64),
    platform_comparable_conversions_by_conversion_date Nullable(Float64),
    platform_comparable_conversions_from_interactions_rate Nullable(Float64),
    platform_comparable_conversions_from_interactions_value_per_interaction Nullable(Float64),
    platform_comparable_conversions_value Nullable(Float64),
    platform_comparable_conversions_value_by_conversion_date Nullable(Float64),
    platform_comparable_conversions_value_per_cost Nullable(Float64),

    cost_converted_currency_per_platform_comparable_conversion Nullable(Float64),
    cost_per_platform_comparable_conversion Nullable(Float64),
    value_per_platform_comparable_conversion Nullable(Float64),
    value_per_platform_comparable_conversions_by_conversion_date Nullable(Float64),

    -- sales / ecommerce
    orders Nullable(Float64),
    revenue Nullable(Float64),
    units_sold Nullable(Float64),

    average_cart_size Nullable(Float64),
    average_order_value Nullable(Float64),

    cost_of_goods_sold Nullable(Float64),
    gross_profit Nullable(Float64),
    gross_profit_margin Nullable(Float64),

    -- cross-sell ecommerce
    cross_sell_cost_of_goods_sold Nullable(Float64),
    cross_sell_gross_profit Nullable(Float64),
    cross_sell_revenue Nullable(Float64),
    cross_sell_units_sold Nullable(Float64),

    -- lead ecommerce
    lead_cost_of_goods_sold Nullable(Float64),
    lead_gross_profit Nullable(Float64),
    lead_revenue Nullable(Float64),
    lead_units_sold Nullable(Float64),

    -- all sales / ecommerce
    all_orders Nullable(Float64),
    all_revenue Nullable(Float64),
    all_units_sold Nullable(Float64),

    all_average_cart_size Nullable(Float64),
    all_average_order_value Nullable(Float64),

    all_cost_of_goods_sold Nullable(Float64),
    all_gross_profit Nullable(Float64),
    all_gross_profit_margin Nullable(Float64),

    -- all cross-sell ecommerce
    all_cross_sell_cost_of_goods_sold Nullable(Float64),
    all_cross_sell_gross_profit Nullable(Float64),
    all_cross_sell_revenue Nullable(Float64),
    all_cross_sell_units_sold Nullable(Float64),

    -- all lead ecommerce
    all_lead_cost_of_goods_sold Nullable(Float64),
    all_lead_gross_profit Nullable(Float64),
    all_lead_revenue Nullable(Float64),
    all_lead_units_sold Nullable(Float64),

    -- customer value
    new_customer_lifetime_value Nullable(Float64),
    all_new_customer_lifetime_value Nullable(Float64),

    -- phone
    phone_calls Nullable(UInt64),
    phone_impressions Nullable(UInt64),
    phone_through_rate Nullable(Float64),

    -- Gmail
    gmail_forwards Nullable(UInt64),
    gmail_saves Nullable(UInt64),
    gmail_secondary_clicks Nullable(UInt64),

    -- landing page / site behaviour
    bounce_rate Nullable(Float64),
    average_page_views Nullable(Float64),
    average_time_on_site Nullable(Float64),
    percent_new_visitors Nullable(Float64),

    -- invalid clicks
    invalid_clicks Nullable(UInt64),
    invalid_click_rate Nullable(Float64),
    general_invalid_clicks Nullable(UInt64),
    general_invalid_click_rate Nullable(Float64),

    -- query clusters
    clicks_unique_query_clusters Nullable(UInt64),
    impressions_unique_query_clusters Nullable(UInt64),

    -- additional impression / click metrics
    coviewed_impressions Nullable(UInt64),
    primary_impressions Nullable(UInt64),
    relative_ctr Nullable(Float64),

    -- publisher
    publisher_organic_clicks Nullable(UInt64),
    publisher_purchased_clicks Nullable(UInt64),
    publisher_unknown_clicks Nullable(UInt64),

    -- app / SKAdNetwork
    sk_ad_network_installs Nullable(UInt64),
    sk_ad_network_total_conversions Nullable(UInt64),

    biddable_app_install_conversions Nullable(Float64),
    biddable_app_post_install_conversions Nullable(Float64),
    biddable_cohort_app_post_install_conversions Nullable(Float64),

    -- target strategy
    average_target_cpa Nullable(Float64),
    average_target_roas Nullable(Float64),

    -- location asset reach
    eligible_impressions_from_location_asset_store_reach Nullable(UInt64),

    -- all conversions by interaction type
    all_conversions_from_click_to_call Nullable(Float64),
    all_conversions_from_directions Nullable(Float64),
    all_conversions_from_menu Nullable(Float64),
    all_conversions_from_order Nullable(Float64),
    all_conversions_from_other_engagement Nullable(Float64),
    all_conversions_from_store_visit Nullable(Float64),
    all_conversions_from_store_website Nullable(Float64),

    -- all conversions from location asset
    all_conversions_from_location_asset_click_to_call Nullable(Float64),
    all_conversions_from_location_asset_directions Nullable(Float64),
    all_conversions_from_location_asset_menu Nullable(Float64),
    all_conversions_from_location_asset_order Nullable(Float64),
    all_conversions_from_location_asset_other_engagement Nullable(Float64),
    all_conversions_from_location_asset_store_visits Nullable(Float64),
    all_conversions_from_location_asset_website Nullable(Float64),

    -- view-through conversions from location asset
    view_through_conversions_from_location_asset_click_to_call Nullable(Float64),
    view_through_conversions_from_location_asset_directions Nullable(Float64),
    view_through_conversions_from_location_asset_menu Nullable(Float64),
    view_through_conversions_from_location_asset_order Nullable(Float64),
    view_through_conversions_from_location_asset_other_engagement Nullable(Float64),
    view_through_conversions_from_location_asset_store_visits Nullable(Float64),
    view_through_conversions_from_location_asset_website Nullable(Float64),

    -- service
    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start,
    customer_id,
    campaign_id
);


CREATE TABLE IF NOT EXISTS google_ads_sales_daily_campaign_level
AS google_ads_website_traffic_daily_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_leads_daily_campaign_level
AS google_ads_website_traffic_daily_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_app_promotion_daily_campaign_level
AS google_ads_website_traffic_daily_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_youtube_reach_views_engagement_daily_campaign_level
AS google_ads_website_traffic_daily_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_store_visits_promotions_daily_campaign_level
AS google_ads_website_traffic_daily_campaign_level;


CREATE TABLE IF NOT EXISTS google_ads_no_goal_daily_campaign_level
AS google_ads_website_traffic_daily_campaign_level;

-- ============================================================
-- 5. DAILY SEARCH TERM LEVEL TABLE
-- Уровень: day + campaign + ad_group + search_term + keyword + device + network
-- Без ad_id, потому что search term нельзя честно привязать к конкретной рекламе.
-- Источник: search_term_view.
--
-- Это одна общая таблица, а не 7 таблиц по целям.
-- Тип цели хранится в google_ads_goal_type.
-- ============================================================

CREATE TABLE IF NOT EXISTS google_ads_daily_search_term_level
(
    -- time
    date_start DateTime('Asia/Almaty'),
    date_stop DateTime('Asia/Almaty'),

    -- account
    customer_id String,
    customer_name Nullable(String),

    -- campaign
    campaign_id String,
    campaign_name Nullable(String),
    campaign_status Nullable(String),
    campaign_primary_status Nullable(String),
    campaign_primary_status_reasons_json Nullable(String),
    advertising_channel_type Nullable(String),
    advertising_channel_sub_type Nullable(String),
    google_ads_goal_type Nullable(String),

    -- ad group
    ad_group_id String,
    ad_group_name Nullable(String),
    ad_group_status Nullable(String),
    ad_group_type Nullable(String),

    -- search term / keyword
    search_term String,
    search_term_status Nullable(String),

    keyword_ad_group_criterion_id Nullable(String),
    keyword_text Nullable(String),
    keyword_match_type Nullable(String),

    -- device
    device String,

    -- network
    ad_network_type String,

    -- budget / bidding
    budget_id Nullable(String),
    budget_name Nullable(String),
    budget_period Nullable(String),
    daily_budget Nullable(Float64),
    lifetime_budget Nullable(Float64),
    is_budget_limited Nullable(Bool),

    bidding_strategy_type Nullable(String),
    optimization_score Nullable(Float64),

    -- main metrics
    impressions Nullable(UInt64),
    clicks Nullable(UInt64),
    ctr Nullable(Float64),

    spend Nullable(Float64),

    average_cpc Nullable(Float64),
    average_cpm Nullable(Float64),
    average_cost Nullable(Float64),

    -- conversions
    conversions Nullable(Float64),
    conversion_rate Nullable(Float64),
    cost_per_conversion Nullable(Float64),

    conversions_value Nullable(Float64),

    all_conversions Nullable(Float64),
    all_conversions_value Nullable(Float64),
    all_conversion_rate Nullable(Float64),
    cost_per_all_conversions Nullable(Float64),

    -- service
    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_start)
ORDER BY (
    date_start,
    customer_id,
    campaign_id,
    ad_group_id,
    search_term,
    ifNull(keyword_ad_group_criterion_id, ''),
    device,
    ad_network_type
);


-- ============================================================
-- 6. CREATIVE ASSETS TABLE
-- Уровень: связь campaign/ad/ad_group/asset_group -> asset.
-- Источники:
-- 1. ad_group_ad_asset_view — assets обычных объявлений.
-- 2. asset_group_asset — assets Performance Max.
--
-- Это не performance-таблица, а справочник/связь креативов.
-- Здесь есть campaign_id, чтобы понимать, к какой кампании относится asset.
-- ============================================================

CREATE TABLE IF NOT EXISTS google_ads_creative_assets
(
    -- source
    source_type String,

    -- account
    customer_id String,
    customer_name Nullable(String),

    -- campaign
    campaign_id String,
    campaign_name Nullable(String),
    campaign_status Nullable(String),
    advertising_channel_type Nullable(String),
    advertising_channel_sub_type Nullable(String),

    -- ad group / ad, для обычных объявлений
    ad_group_id Nullable(String),
    ad_group_name Nullable(String),
    ad_group_status Nullable(String),

    ad_id Nullable(String),
    ad_name Nullable(String),
    ad_type Nullable(String),
    ad_status Nullable(String),

    -- asset group, для Performance Max
    asset_group_id Nullable(String),
    asset_group_name Nullable(String),
    asset_group_status Nullable(String),
    asset_group_strength Nullable(String),
    asset_group_asset_status Nullable(String),

    -- asset
    asset_id String,
    asset_name Nullable(String),
    asset_type Nullable(String),
    asset_field_type Nullable(String),

    -- image asset
    image_url Nullable(String),
    image_width Nullable(UInt32),
    image_height Nullable(UInt32),
    image_mime_type Nullable(String),
    image_file_size Nullable(UInt64),

    -- YouTube video asset
    youtube_video_id Nullable(String),
    youtube_video_url Nullable(String),
    youtube_video_title Nullable(String),

    -- service
    loaded_at DateTime('Asia/Almaty')
)
ENGINE = MergeTree
ORDER BY (
    customer_id,
    campaign_id,
    source_type,
    ifNull(ad_group_id, ''),
    ifNull(ad_id, ''),
    ifNull(asset_group_id, ''),
    asset_id,
    ifNull(asset_field_type, '')
);
