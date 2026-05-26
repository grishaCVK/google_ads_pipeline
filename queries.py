AD_HOURLY_QUERY = """
SELECT
  segments.date,
  segments.hour,
  segments.device,
  segments.ad_network_type,

  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.primary_status,
  campaign.primary_status_reasons,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  campaign.bidding_strategy_type,
  campaign.optimization_score,

  campaign_budget.id,
  campaign_budget.name,
  campaign_budget.period,
  campaign_budget.amount_micros,
  campaign_budget.total_amount_micros,

  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.cost_micros,

  metrics.average_cpc,
  metrics.average_cpm,
  metrics.trueview_average_cpv,
  metrics.average_cost,
  metrics.average_cpe,

  metrics.interactions,
  metrics.interaction_rate,

  metrics.engagements,
  metrics.engagement_rate,

  metrics.video_trueview_views,
  metrics.video_trueview_view_rate,
  metrics.video_quartile_p25_rate,
  metrics.video_quartile_p50_rate,
  metrics.video_quartile_p75_rate,
  metrics.video_quartile_p100_rate,

  metrics.absolute_top_impression_percentage,
  metrics.top_impression_percentage,

  metrics.search_impression_share,
  metrics.search_absolute_top_impression_share,
  metrics.search_top_impression_share,

  metrics.search_budget_lost_impression_share,
  metrics.search_budget_lost_absolute_top_impression_share,
  metrics.search_budget_lost_top_impression_share,

  metrics.search_rank_lost_impression_share,
  metrics.search_rank_lost_absolute_top_impression_share,
  metrics.search_rank_lost_top_impression_share,

  metrics.content_impression_share,

  metrics.active_view_impressions,
  metrics.active_view_viewability,

  metrics.active_view_cpm,
  metrics.active_view_ctr,
  metrics.active_view_measurability,
  metrics.active_view_measurable_cost_micros,
  metrics.active_view_measurable_impressions,

  metrics.active_view_audibility_measurable_impressions,
  metrics.active_view_audibility_measurable_impressions_rate,

  metrics.active_view_audibility_invalid_measurable_impressions_rate,
  metrics.active_view_audibility_invalid_givt_measurable_impressions_rate,

  metrics.active_view_audible_impressions,
  metrics.active_view_audible_impressions_rate,

  metrics.active_view_audible_quartile_p25_rate,
  metrics.active_view_audible_quartile_p50_rate,
  metrics.active_view_audible_quartile_p75_rate,
  metrics.active_view_audible_quartile_p100_rate,

  metrics.active_view_audible_two_seconds_impressions,
  metrics.active_view_audible_two_seconds_impressions_rate,

  metrics.active_view_audible_thirty_seconds_impressions,
  metrics.active_view_audible_thirty_seconds_impressions_rate,

  metrics.conversions,
  metrics.conversions_from_interactions_rate,
  metrics.cost_per_conversion,
  metrics.conversions_value,

  metrics.all_conversions,
  metrics.all_conversions_value,

  metrics.conversions_by_conversion_date,
  metrics.conversions_value_by_conversion_date,

  metrics.all_conversions_by_conversion_date,
  metrics.all_conversions_value_by_conversion_date,

  metrics.all_conversions_from_interactions_rate,
  metrics.cost_per_all_conversions,

  metrics.value_per_conversion,
  metrics.value_per_conversions_by_conversion_date,

  metrics.value_per_all_conversions,
  metrics.value_per_all_conversions_by_conversion_date,

  metrics.view_through_conversions
FROM campaign
WHERE segments.date BETWEEN '{date_since}' AND '{date_until}'
"""


GEO_DAILY_QUERY = """
SELECT
  segments.date,
  segments.device,
  segments.ad_network_type,
  segments.geo_target_region,
  segments.geo_target_city,

  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.primary_status,
  campaign.primary_status_reasons,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  campaign.bidding_strategy_type,
  campaign.optimization_score,

  geographic_view.country_criterion_id,
  geographic_view.location_type,

  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.cost_micros,

  metrics.average_cpc,
  metrics.average_cpm,
  metrics.trueview_average_cpv,
  metrics.average_cost,

  metrics.interactions,
  metrics.interaction_rate,

  metrics.video_trueview_views,
  metrics.video_trueview_view_rate,

  metrics.absolute_top_impression_percentage,
  metrics.top_impression_percentage,

  metrics.conversions,
  metrics.conversions_from_interactions_rate,
  metrics.cost_per_conversion,
  metrics.conversions_value,

  metrics.all_conversions,
  metrics.all_conversions_value,
  metrics.all_conversions_from_interactions_rate,
  metrics.cost_per_all_conversions,

  metrics.conversions_by_conversion_date,
  metrics.conversions_value_by_conversion_date,

  metrics.all_conversions_by_conversion_date,
  metrics.all_conversions_value_by_conversion_date,

  metrics.value_per_conversion,
  metrics.value_per_conversions_by_conversion_date,

  metrics.value_per_all_conversions,
  metrics.value_per_all_conversions_by_conversion_date,

  metrics.cross_device_conversions,
  metrics.view_through_conversions
FROM geographic_view
WHERE segments.date BETWEEN '{date_since}' AND '{date_until}'
"""


GEO_TARGET_CONSTANT_QUERY_TEMPLATE = """
SELECT
  geo_target_constant.id,
  geo_target_constant.name,
  geo_target_constant.country_code,
  geo_target_constant.target_type,
  geo_target_constant.status,
  geo_target_constant.parent_geo_target
FROM geo_target_constant
WHERE geo_target_constant.id IN ({criterion_ids})
"""


TARGETED_LOCATIONS_QUERY = """
SELECT
  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,

  campaign_criterion.criterion_id,
  campaign_criterion.type,
  campaign_criterion.status,
  campaign_criterion.negative,
  campaign_criterion.location.geo_target_constant
FROM campaign_criterion
WHERE campaign_criterion.type = LOCATION
"""


CAMPAIGN_BUDGET_INFO_QUERY = """
SELECT
  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.primary_status_reasons,
  campaign.bidding_strategy_type,
  campaign.optimization_score,

  campaign_budget.id,
  campaign_budget.name,
  campaign_budget.period,
  campaign_budget.amount_micros,
  campaign_budget.total_amount_micros
FROM campaign
WHERE campaign.status != 'REMOVED'
"""

AD_GROUP_AD_DAILY_QUERY = """
SELECT
  segments.date,
  segments.device,
  segments.ad_network_type,

  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.primary_status,
  campaign.primary_status_reasons,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  campaign.bidding_strategy_type,
  campaign.optimization_score,

  ad_group.id,
  ad_group.name,
  ad_group.status,
  ad_group.type,

  ad_group_ad.ad.id,
  ad_group_ad.ad.name,
  ad_group_ad.ad.type,
  ad_group_ad.status,
  ad_group_ad.ad.final_urls,
  ad_group_ad.ad.final_mobile_urls,

  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.cost_micros,

  metrics.average_cpc,
  metrics.average_cpm,
  metrics.trueview_average_cpv,
  metrics.average_cost,
  metrics.average_cpe,

  metrics.interactions,
  metrics.interaction_rate,

  metrics.engagements,
  metrics.engagement_rate,

  metrics.video_trueview_views,
  metrics.video_trueview_view_rate,
  metrics.video_quartile_p25_rate,
  metrics.video_quartile_p50_rate,
  metrics.video_quartile_p75_rate,
  metrics.video_quartile_p100_rate,

  metrics.video_trueview_view_rate_in_feed,
  metrics.video_trueview_view_rate_in_stream,
  metrics.video_trueview_view_rate_shorts,

  metrics.absolute_top_impression_percentage,
  metrics.top_impression_percentage,

  metrics.active_view_impressions,
  metrics.active_view_viewability,

  metrics.active_view_cpm,
  metrics.active_view_ctr,
  metrics.active_view_measurability,
  metrics.active_view_measurable_cost_micros,
  metrics.active_view_measurable_impressions,

  metrics.active_view_audibility_measurable_impressions,
  metrics.active_view_audibility_measurable_impressions_rate,
  metrics.active_view_audibility_invalid_measurable_impressions_rate,
  metrics.active_view_audibility_invalid_givt_measurable_impressions_rate,

  metrics.active_view_audible_impressions,
  metrics.active_view_audible_impressions_rate,
  metrics.active_view_audible_quartile_p25_rate,
  metrics.active_view_audible_quartile_p50_rate,
  metrics.active_view_audible_quartile_p75_rate,
  metrics.active_view_audible_quartile_p100_rate,
  metrics.active_view_audible_two_seconds_impressions,
  metrics.active_view_audible_two_seconds_impressions_rate,
  metrics.active_view_audible_thirty_seconds_impressions,
  metrics.active_view_audible_thirty_seconds_impressions_rate,

  metrics.conversions,
  metrics.conversions_from_interactions_rate,
  metrics.cost_per_conversion,
  metrics.conversions_value,

  metrics.all_conversions,
  metrics.all_conversions_value,
  metrics.all_conversions_from_interactions_rate,
  metrics.cost_per_all_conversions,

  metrics.conversions_by_conversion_date,
  metrics.conversions_value_by_conversion_date,
  metrics.all_conversions_by_conversion_date,
  metrics.all_conversions_value_by_conversion_date,

  metrics.value_per_conversion,
  metrics.value_per_conversions_by_conversion_date,
  metrics.value_per_all_conversions,
  metrics.value_per_all_conversions_by_conversion_date,

  metrics.view_through_conversions,
  metrics.cross_device_conversions,

  metrics.current_model_attributed_conversions,
  metrics.current_model_attributed_conversions_value,
  metrics.cost_per_current_model_attributed_conversion,
  metrics.value_per_current_model_attributed_conversion,

  metrics.platform_comparable_conversions,
  metrics.platform_comparable_conversions_by_conversion_date,
  metrics.platform_comparable_conversions_from_interactions_rate,
  metrics.platform_comparable_conversions_from_interactions_value_per_interaction,
  metrics.platform_comparable_conversions_value,
  metrics.platform_comparable_conversions_value_by_conversion_date,
  metrics.platform_comparable_conversions_value_per_cost,

  metrics.cost_converted_currency_per_platform_comparable_conversion,
  metrics.cost_per_platform_comparable_conversion,
  metrics.value_per_platform_comparable_conversion,
  metrics.value_per_platform_comparable_conversions_by_conversion_date,

  metrics.orders,
  metrics.revenue_micros,
  metrics.units_sold,
  metrics.average_cart_size,
  metrics.average_order_value_micros,

  metrics.cost_of_goods_sold_micros,
  metrics.gross_profit_micros,
  metrics.gross_profit_margin,

  metrics.cross_sell_cost_of_goods_sold_micros,
  metrics.cross_sell_gross_profit_micros,
  metrics.cross_sell_revenue_micros,
  metrics.cross_sell_units_sold,

  metrics.lead_cost_of_goods_sold_micros,
  metrics.lead_gross_profit_micros,
  metrics.lead_revenue_micros,
  metrics.lead_units_sold,

  metrics.all_orders,
  metrics.all_revenue_micros,
  metrics.all_units_sold,
  metrics.all_average_cart_size,
  metrics.all_average_order_value_micros,

  metrics.all_cost_of_goods_sold_micros,
  metrics.all_gross_profit_micros,
  metrics.all_gross_profit_margin,

  metrics.all_cross_sell_cost_of_goods_sold_micros,
  metrics.all_cross_sell_gross_profit_micros,
  metrics.all_cross_sell_revenue_micros,
  metrics.all_cross_sell_units_sold,

  metrics.all_lead_cost_of_goods_sold_micros,
  metrics.all_lead_gross_profit_micros,
  metrics.all_lead_revenue_micros,
  metrics.all_lead_units_sold,

  metrics.gmail_forwards,
  metrics.gmail_saves,
  metrics.gmail_secondary_clicks
FROM ad_group_ad
WHERE segments.date BETWEEN '{date_since}' AND '{date_until}'
"""

DAILY_CAMPAIGN_QUERY = """
SELECT
  segments.date,

  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.primary_status,
  campaign.primary_status_reasons,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  campaign.bidding_strategy_type,
  campaign.optimization_score,

  campaign_budget.id,
  campaign_budget.name,
  campaign_budget.period,
  campaign_budget.amount_micros,
  campaign_budget.total_amount_micros,

  metrics.unique_users,
  metrics.average_impression_frequency_per_user,
  metrics.unique_users_two_plus,
  metrics.unique_users_three_plus,
  metrics.unique_users_four_plus,
  metrics.unique_users_five_plus,
  metrics.unique_users_ten_plus,

  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.cost_micros,

  metrics.average_cpc,
  metrics.average_cpm,
  metrics.trueview_average_cpv,
  metrics.average_cost,
  metrics.average_cpe,

  metrics.interactions,
  metrics.interaction_rate,

  metrics.engagements,
  metrics.engagement_rate,

  metrics.video_trueview_views,
  metrics.video_trueview_view_rate,
  metrics.video_quartile_p25_rate,
  metrics.video_quartile_p50_rate,
  metrics.video_quartile_p75_rate,
  metrics.video_quartile_p100_rate,

  metrics.video_trueview_view_rate_in_feed,
  metrics.video_trueview_view_rate_in_stream,
  metrics.video_trueview_view_rate_shorts,
  metrics.average_video_watch_time_duration_millis,

  metrics.absolute_top_impression_percentage,
  metrics.top_impression_percentage,

  metrics.search_impression_share,
  metrics.search_absolute_top_impression_share,
  metrics.search_top_impression_share,
  metrics.search_budget_lost_impression_share,
  metrics.search_budget_lost_absolute_top_impression_share,
  metrics.search_budget_lost_top_impression_share,
  metrics.search_rank_lost_impression_share,
  metrics.search_rank_lost_absolute_top_impression_share,
  metrics.search_rank_lost_top_impression_share,
  metrics.search_click_share,
  metrics.search_exact_match_impression_share,

  metrics.content_impression_share,
  metrics.content_budget_lost_impression_share,
  metrics.content_rank_lost_impression_share,

  metrics.active_view_impressions,
  metrics.active_view_viewability,
  metrics.active_view_cpm,
  metrics.active_view_ctr,
  metrics.active_view_measurability,
  metrics.active_view_measurable_cost_micros,
  metrics.active_view_measurable_impressions,

  metrics.active_view_audibility_measurable_impressions,
  metrics.active_view_audibility_measurable_impressions_rate,
  metrics.active_view_audibility_invalid_measurable_impressions_rate,
  metrics.active_view_audibility_invalid_givt_measurable_impressions_rate,

  metrics.active_view_audible_impressions,
  metrics.active_view_audible_impressions_rate,
  metrics.active_view_audible_quartile_p25_rate,
  metrics.active_view_audible_quartile_p50_rate,
  metrics.active_view_audible_quartile_p75_rate,
  metrics.active_view_audible_quartile_p100_rate,
  metrics.active_view_audible_two_seconds_impressions,
  metrics.active_view_audible_two_seconds_impressions_rate,
  metrics.active_view_audible_thirty_seconds_impressions,
  metrics.active_view_audible_thirty_seconds_impressions_rate,

  metrics.conversions,
  metrics.conversions_from_interactions_rate,
  metrics.cost_per_conversion,
  metrics.conversions_value,
  metrics.conversions_by_conversion_date,
  metrics.conversions_value_by_conversion_date,
  metrics.conversions_unique_query_clusters,

  metrics.all_conversions,
  metrics.all_conversions_value,
  metrics.all_conversions_from_interactions_rate,
  metrics.cost_per_all_conversions,
  metrics.all_conversions_by_conversion_date,
  metrics.all_conversions_value_by_conversion_date,

  metrics.value_per_conversion,
  metrics.value_per_conversions_by_conversion_date,
  metrics.value_per_all_conversions,
  metrics.value_per_all_conversions_by_conversion_date,

  metrics.cross_device_conversions,
  metrics.cross_device_conversions_by_conversion_date,
  metrics.cross_device_conversions_value_by_conversion_date,
  metrics.cross_device_conversions_value_micros,

  metrics.view_through_conversions,

  metrics.current_model_attributed_conversions,
  metrics.current_model_attributed_conversions_value,
  metrics.current_model_attributed_conversions_from_interactions_rate,
  metrics.current_model_attributed_conversions_from_interactions_value_per_interaction,
  metrics.current_model_attributed_conversions_value_per_cost,
  metrics.cost_per_current_model_attributed_conversion,
  metrics.value_per_current_model_attributed_conversion,

  metrics.platform_comparable_conversions,
  metrics.platform_comparable_conversions_by_conversion_date,
  metrics.platform_comparable_conversions_from_interactions_rate,
  metrics.platform_comparable_conversions_from_interactions_value_per_interaction,
  metrics.platform_comparable_conversions_value,
  metrics.platform_comparable_conversions_value_by_conversion_date,
  metrics.platform_comparable_conversions_value_per_cost,
  metrics.cost_converted_currency_per_platform_comparable_conversion,
  metrics.cost_per_platform_comparable_conversion,
  metrics.value_per_platform_comparable_conversion,
  metrics.value_per_platform_comparable_conversions_by_conversion_date,

  metrics.orders,
  metrics.revenue_micros,
  metrics.units_sold,
  metrics.average_cart_size,
  metrics.average_order_value_micros,

  metrics.cost_of_goods_sold_micros,
  metrics.gross_profit_micros,
  metrics.gross_profit_margin,

  metrics.cross_sell_cost_of_goods_sold_micros,
  metrics.cross_sell_gross_profit_micros,
  metrics.cross_sell_revenue_micros,
  metrics.cross_sell_units_sold,

  metrics.lead_cost_of_goods_sold_micros,
  metrics.lead_gross_profit_micros,
  metrics.lead_revenue_micros,
  metrics.lead_units_sold,

  metrics.all_orders,
  metrics.all_revenue_micros,
  metrics.all_units_sold,
  metrics.all_average_cart_size,
  metrics.all_average_order_value_micros,

  metrics.all_cost_of_goods_sold_micros,
  metrics.all_gross_profit_micros,
  metrics.all_gross_profit_margin,

  metrics.all_cross_sell_cost_of_goods_sold_micros,
  metrics.all_cross_sell_gross_profit_micros,
  metrics.all_cross_sell_revenue_micros,
  metrics.all_cross_sell_units_sold,

  metrics.all_lead_cost_of_goods_sold_micros,
  metrics.all_lead_gross_profit_micros,
  metrics.all_lead_revenue_micros,
  metrics.all_lead_units_sold,

  metrics.new_customer_lifetime_value,
  metrics.all_new_customer_lifetime_value,

  metrics.phone_calls,
  metrics.phone_impressions,
  metrics.phone_through_rate,

  metrics.gmail_forwards,
  metrics.gmail_saves,
  metrics.gmail_secondary_clicks,

  metrics.bounce_rate,
  metrics.average_page_views,
  metrics.average_time_on_site,
  metrics.percent_new_visitors,

  metrics.invalid_clicks,
  metrics.invalid_click_rate,
  metrics.general_invalid_clicks,
  metrics.general_invalid_click_rate,

  metrics.clicks_unique_query_clusters,
  metrics.impressions_unique_query_clusters,

  metrics.coviewed_impressions,
  metrics.primary_impressions,
  metrics.relative_ctr,

  metrics.publisher_organic_clicks,
  metrics.publisher_purchased_clicks,
  metrics.publisher_unknown_clicks,

  metrics.sk_ad_network_installs,
  metrics.sk_ad_network_total_conversions,

  metrics.biddable_app_install_conversions,
  metrics.biddable_app_post_install_conversions,
  metrics.biddable_cohort_app_post_install_conversions,

  metrics.average_target_cpa_micros,
  metrics.average_target_roas,

  metrics.eligible_impressions_from_location_asset_store_reach,

  metrics.all_conversions_from_click_to_call,
  metrics.all_conversions_from_directions,
  metrics.all_conversions_from_menu,
  metrics.all_conversions_from_order,
  metrics.all_conversions_from_other_engagement,
  metrics.all_conversions_from_store_visit,
  metrics.all_conversions_from_store_website,

  metrics.all_conversions_from_location_asset_click_to_call,
  metrics.all_conversions_from_location_asset_directions,
  metrics.all_conversions_from_location_asset_menu,
  metrics.all_conversions_from_location_asset_order,
  metrics.all_conversions_from_location_asset_other_engagement,
  metrics.all_conversions_from_location_asset_store_visits,
  metrics.all_conversions_from_location_asset_website,

  metrics.view_through_conversions_from_location_asset_click_to_call,
  metrics.view_through_conversions_from_location_asset_directions,
  metrics.view_through_conversions_from_location_asset_menu,
  metrics.view_through_conversions_from_location_asset_order,
  metrics.view_through_conversions_from_location_asset_other_engagement,
  metrics.view_through_conversions_from_location_asset_store_visits,
  metrics.view_through_conversions_from_location_asset_website
FROM campaign
WHERE segments.date BETWEEN '{date_since}' AND '{date_until}'
"""

SEARCH_TERM_DAILY_QUERY = """
SELECT
  segments.date,
  segments.device,
  segments.ad_network_type,

  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.primary_status,
  campaign.primary_status_reasons,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  campaign.bidding_strategy_type,
  campaign.optimization_score,

  ad_group.id,
  ad_group.name,
  ad_group.status,
  ad_group.type,

  search_term_view.search_term,
  search_term_view.status,

  segments.keyword.ad_group_criterion,
  segments.keyword.info.text,
  segments.keyword.info.match_type,

  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.cost_micros,

  metrics.average_cpc,
  metrics.average_cpm,
  metrics.average_cost,

  metrics.conversions,
  metrics.conversions_from_interactions_rate,
  metrics.cost_per_conversion,
  metrics.conversions_value,

  metrics.all_conversions,
  metrics.all_conversions_value,
  metrics.all_conversions_from_interactions_rate,
  metrics.cost_per_all_conversions
FROM search_term_view
WHERE segments.date BETWEEN '{date_since}' AND '{date_until}'
"""

AD_GROUP_AD_ASSET_QUERY = """
SELECT
  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,

  ad_group.id,
  ad_group.name,
  ad_group.status,

  ad_group_ad.ad.id,
  ad_group_ad.ad.name,
  ad_group_ad.ad.type,
  ad_group_ad.status,

  ad_group_ad_asset_view.field_type,

  asset.id,
  asset.name,
  asset.type,
  asset.youtube_video_asset.youtube_video_id,
  asset.youtube_video_asset.youtube_video_title,
  asset.image_asset.full_size.url,
  asset.image_asset.full_size.width_pixels,
  asset.image_asset.full_size.height_pixels,
  asset.image_asset.mime_type,
  asset.image_asset.file_size
FROM ad_group_ad_asset_view
WHERE campaign.status != 'REMOVED'
"""

ASSET_GROUP_ASSET_QUERY = """
SELECT
  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,

  asset_group.id,
  asset_group.name,
  asset_group.status,
  asset_group.ad_strength,

  asset_group_asset.field_type,
  asset_group_asset.status,

  asset.id,
  asset.name,
  asset.type,
  asset.youtube_video_asset.youtube_video_id,
  asset.youtube_video_asset.youtube_video_title,
  asset.image_asset.full_size.url,
  asset.image_asset.full_size.width_pixels,
  asset.image_asset.full_size.height_pixels,
  asset.image_asset.mime_type,
  asset.image_asset.file_size
FROM asset_group_asset
WHERE campaign.status != 'REMOVED'
"""

GENDER_DAILY_QUERY = """
SELECT
  segments.date,
  segments.device,
  segments.ad_network_type,

  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.primary_status,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  campaign.bidding_strategy_type,

  ad_group.id,
  ad_group.name,
  ad_group.status,
  ad_group.type,

  ad_group_criterion.criterion_id,
  ad_group_criterion.gender.type,
  ad_group_criterion.status,

  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.cost_micros,

  metrics.average_cpc,
  metrics.average_cpm,
  metrics.average_cost,

  metrics.interactions,
  metrics.interaction_rate,

  metrics.engagements,
  metrics.engagement_rate,

  metrics.video_trueview_views,
  metrics.video_trueview_view_rate,

  metrics.conversions,
  metrics.conversions_from_interactions_rate,
  metrics.cost_per_conversion,
  metrics.conversions_value,

  metrics.all_conversions,
  metrics.all_conversions_value,
  metrics.all_conversions_from_interactions_rate,
  metrics.cost_per_all_conversions,

  metrics.view_through_conversions
FROM gender_view
WHERE segments.date BETWEEN '{date_since}' AND '{date_until}'
"""

DIRECT_IMAGE_AD_CREATIVE_QUERY = """
SELECT
  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,

  ad_group.id,
  ad_group.name,
  ad_group.status,

  ad_group_ad.ad.id,
  ad_group_ad.ad.name,
  ad_group_ad.ad.type,
  ad_group_ad.status,

  ad_group_ad.ad.image_ad.name,
  ad_group_ad.ad.image_ad.image_url,
  ad_group_ad.ad.image_ad.preview_image_url,
  ad_group_ad.ad.image_ad.mime_type,
  ad_group_ad.ad.image_ad.pixel_width,
  ad_group_ad.ad.image_ad.pixel_height,
  ad_group_ad.ad.image_ad.preview_pixel_width,
  ad_group_ad.ad.image_ad.preview_pixel_height

FROM ad_group_ad
WHERE campaign.status != 'REMOVED'
AND ad_group_ad.ad.type = 'IMAGE_AD'
"""


DIRECT_VIDEO_RESPONSIVE_AD_CREATIVE_QUERY = """
SELECT
  customer.id,
  customer.descriptive_name,

  campaign.id,
  campaign.name,
  campaign.status,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,

  ad_group.id,
  ad_group.name,
  ad_group.status,

  ad_group_ad.ad.id,
  ad_group_ad.ad.name,
  ad_group_ad.ad.type,
  ad_group_ad.status,

  ad_group_ad.ad.video_responsive_ad.videos

FROM ad_group_ad
WHERE campaign.status != 'REMOVED'
AND ad_group_ad.ad.type = 'VIDEO_RESPONSIVE_AD'
"""


YOUTUBE_VIDEO_ASSET_QUERY_TEMPLATE = """
SELECT
  asset.id,
  asset.name,
  asset.type,
  asset.youtube_video_asset.youtube_video_id,
  asset.youtube_video_asset.youtube_video_title
FROM asset
WHERE asset.id IN ({asset_ids})
"""