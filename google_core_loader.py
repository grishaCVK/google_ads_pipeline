"""
google_core_loader.py

Загрузка данных из staging в core.
Вызывается из main.py после staging загрузки.

Логика для каждой таблицы:
1. Считаем rows_before
2. DELETE за период
3. INSERT SELECT из staging
4. Считаем rows_after
5. Логируем в etl_table_loads
6. Quality checks
"""

from zoneinfo import ZoneInfo

import clickhouse_connect

import config
import etl_logger

ALMATY_TZ = ZoneInfo("Asia/Almaty")

STAGING_DB = config.CLICKHOUSE_STAGING_DB
CORE_DB = config.CLICKHOUSE_CORE_DB

# Все staging таблицы по типу
DAILY_CAMPAIGN_STAGING_TABLES = [
    "google_ads_sales_daily_campaign_level_staging",
    "google_ads_leads_daily_campaign_level_staging",
    "google_ads_website_traffic_daily_campaign_level_staging",
    "google_ads_app_promotion_daily_campaign_level_staging",
    (
        "google_ads_youtube_reach_views_engagement"
        "_daily_campaign_level_staging"
    ),
    (
        "google_ads_store_visits_promotions"
        "_daily_campaign_level_staging"
    ),
    "google_ads_no_goal_daily_campaign_level_staging",
]

DAILY_AD_STAGING_TABLES = [
    "google_ads_sales_daily_ad_level_staging",
    "google_ads_leads_daily_ad_level_staging",
    "google_ads_website_traffic_daily_ad_level_staging",
    "google_ads_app_promotion_daily_ad_level_staging",
    (
        "google_ads_youtube_reach_views_engagement"
        "_daily_ad_level_staging"
    ),
    (
        "google_ads_store_visits_promotions"
        "_daily_ad_level_staging"
    ),
    "google_ads_no_goal_daily_ad_level_staging",
]

HOURLY_STAGING_TABLES = [
    "google_ads_sales_hourly_campaign_level_staging",
    "google_ads_leads_hourly_campaign_level_staging",
    (
        "google_ads_website_traffic"
        "_hourly_campaign_level_staging"
    ),
    (
        "google_ads_app_promotion"
        "_hourly_campaign_level_staging"
    ),
    (
        "google_ads_youtube_reach_views_engagement"
        "_hourly_campaign_level_staging"
    ),
    (
        "google_ads_store_visits_promotions"
        "_hourly_campaign_level_staging"
    ),
    "google_ads_no_goal_hourly_campaign_level_staging",
]


def _get_client(database: str):
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=database,
    )


def _count(
    client,
    database: str,
    table: str,
    date_since: str,
    date_until: str,
) -> int:
    sql = f"""
    SELECT count()
    FROM {database}.{table}
    WHERE toDate(date_start)
          BETWEEN toDate('{date_since}')
          AND toDate('{date_until}')
    """
    return client.query(sql).first_row[0]


def _delete(
    client,
    database: str,
    table: str,
    date_since: str,
    date_until: str,
) -> None:
    sql = f"""
    ALTER TABLE {database}.{table}
    DELETE
    WHERE toDate(date_start)
          BETWEEN toDate('{date_since}')
          AND toDate('{date_until}')
    """
    client.command(sql)


def _union_select(
    staging_tables: list[str],
    select_sql: str,
    date_since: str,
    date_until: str,
) -> str:
    """
    Строит UNION ALL из нескольких staging таблиц.
    select_sql — шаблон SELECT с {table} и {db}.
    """
    parts = []

    for table in staging_tables:
        part = select_sql.format(
            db=STAGING_DB,
            table=table,
            date_since=date_since,
            date_until=date_until,
        )
        parts.append(part)

    return "\nUNION ALL\n".join(parts)


# ============================================================
# 1. core_daily_campaign_level
# ============================================================

_DAILY_CAMPAIGN_SELECT = """
SELECT
    date_start,
    date_stop,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_primary_status         AS campaign_effective_status,
    google_ads_goal_type            AS objective,
    advertising_channel_type        AS media_type,
    spend,
    impressions,
    reach,
    average_impression_frequency_per_user AS frequency,
    average_cpm                     AS cpm,
    clicks,
    ctr,
    average_cpc                     AS cpc,
    average_cpv                     AS cpv,
    video_views                     AS true_views,
    view_rate,
    daily_budget,
    lifetime_budget,
    now()                           AS loaded_at
FROM {db}.{table}
WHERE toDate(date_start)
      BETWEEN toDate('{date_since}')
      AND toDate('{date_until}')"""


def load_daily_campaign(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "google_ads_core_daily_campaign_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    union_sql = _union_select(
        DAILY_CAMPAIGN_STAGING_TABLES,
        _DAILY_CAMPAIGN_SELECT,
        date_since,
        date_until,
    )

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        + union_sql
    )
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    rows_inserted = rows_after

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_inserted,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    etl_logger.run_quality_checks(
        run_id=run_id,
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        key_columns=["campaign_id", "date_start"],
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, "
        f"inserted={rows_inserted}, "
        f"after={rows_after}"
    )
    return rows_inserted


# ============================================================
# 2. core_daily_ad_level
# ============================================================

_DAILY_AD_SELECT = """
SELECT
    date_start,
    date_stop,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_primary_status,
    ad_group_id,
    ad_group_name,
    ad_id,
    ad_name,
    landing_page_url,
    google_ads_goal_type,
    advertising_channel_type,
    spend,
    impressions,
    clicks,
    video_views,
    daily_budget,
    lifetime_budget
FROM {db}.{table}
WHERE toDate(date_start)
      BETWEEN toDate('{date_since}')
      AND toDate('{date_until}')"""


def load_daily_ad(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "google_ads_core_daily_ad_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    union_sql = _union_select(
        DAILY_AD_STAGING_TABLES,
        _DAILY_AD_SELECT,
        date_since,
        date_until,
    )

    _agg_inner = (
        "SELECT"
        " toStartOfDay(date_start) AS d_start,"
        " campaign_id AS c_id,"
        " ad_group_id AS ag_id,"
        " ad_id AS a_id,"
        " any(campaign_name) AS c_name,"
        " any(campaign_status) AS c_status,"
        " any(campaign_primary_status) AS c_eff,"
        " any(ad_group_name) AS ag_name,"
        " any(ad_name) AS a_name,"
        " any(landing_page_url) AS dest_url,"
        " any(google_ads_goal_type) AS obj,"
        " any(advertising_channel_type) AS m_type,"
        " sum(spend) AS s_spend,"
        " sum(impressions) AS s_impr,"
        " sum(clicks) AS s_clicks,"
        " sum(video_views) AS s_views,"
        " any(daily_budget) AS d_budget,"
        " any(lifetime_budget) AS l_budget"
        " FROM (" + union_sql + ")"
        " GROUP BY d_start, c_id, ag_id, a_id"
    )

    _agg_sql = (
        "SELECT"
        " d_start AS date_start,"
        " d_start + INTERVAL 1 DAY AS date_stop,"
        " c_id AS campaign_id,"
        " c_name AS campaign_name,"
        " c_status AS campaign_status,"
        " c_eff AS campaign_effective_status,"
        " ag_id AS ad_group_id,"
        " ag_name AS ad_group_name,"
        " a_id AS ad_id,"
        " a_name AS ad_name,"
        " dest_url AS destination_url,"
        " obj AS objective,"
        " m_type AS media_type,"
        " s_spend AS spend,"
        " s_impr AS impressions,"
        " if(s_impr>0, s_spend/s_impr*1000, NULL) AS cpm,"
        " s_clicks AS clicks,"
        " if(s_impr>0, s_clicks/s_impr, NULL) AS ctr,"
        " if(s_clicks>0, s_spend/s_clicks, NULL) AS cpc,"
        " if(s_views>0, s_spend/s_views, NULL) AS cpv,"
        " s_views AS true_views,"
        " if(s_impr>0, s_views/s_impr, NULL) AS view_rate,"
        " d_budget AS daily_budget,"
        " l_budget AS lifetime_budget,"
        " now() AS loaded_at"
        " FROM (" + _agg_inner + ")"
    )

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} " + _agg_sql
    )
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until,
    )

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    etl_logger.run_quality_checks(
        run_id=run_id,
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        key_columns=[
            "date_start", "campaign_id",
            "ad_group_id", "ad_id",
        ],
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 3. core_hourly_campaign_level
# ============================================================

_HOURLY_SELECT = """
SELECT
    date_start,
    date_stop,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_primary_status     AS campaign_effective_status,
    google_ads_goal_type        AS objective,
    advertising_channel_type    AS media_type,
    device,
    spend,
    impressions,
    average_cpm                 AS cpm,
    clicks,
    ctr,
    average_cpc                 AS cpc,
    average_cpv                 AS cpv,
    video_views                 AS true_views,
    view_rate,
    daily_budget,
    lifetime_budget,
    now()                       AS loaded_at
FROM {db}.{table}
WHERE toDate(date_start)
      BETWEEN toDate('{date_since}')
      AND toDate('{date_until}')"""


def load_hourly_campaign(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "google_ads_core_hourly_campaign_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    union_sql = _union_select(
        HOURLY_STAGING_TABLES,
        _HOURLY_SELECT,
        date_since,
        date_until,
    )

    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} "
        + union_sql
    )
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until,
    )

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 4. core_geo_daily_level
# ============================================================

def load_geo_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "google_ads_core_geo_daily_level"
    staging_table = (
        "google_ads_geo_daily_region_level_staging"
    )
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    # Staging geo сегментирован по device × ad_network_type,
    # а в core этих колонок нет — поэтому агрегируем до грейна
    # (день, кампания, страна, регион, город), иначе строки
    # задваиваются по числу комбинаций устройство×сеть.
    agg_inner = f"""
        SELECT
            toStartOfDay(date_start)    AS d_start,
            campaign_id                 AS c_id,
            geo_country_name            AS country,
            geo_region_name             AS region,
            geo_city_name               AS city,
            any(campaign_name)          AS c_name,
            any(campaign_status)        AS c_status,
            any(campaign_primary_status) AS c_eff,
            any(google_ads_goal_type)   AS obj,
            sum(spend)                  AS s_spend,
            sum(impressions)            AS s_impr,
            sum(clicks)                 AS s_clicks,
            sum(video_views)            AS s_views
        FROM {STAGING_DB}.{staging_table}
        WHERE toDate(date_start)
              BETWEEN toDate('{date_since}')
              AND toDate('{date_until}')
        GROUP BY d_start, c_id, country, region, city
    """

    insert_sql = f"""
    INSERT INTO {CORE_DB}.{table}
    SELECT
        d_start                     AS date_start,
        d_start + INTERVAL 1 DAY    AS date_stop,
        c_id                        AS campaign_id,
        c_name                      AS campaign_name,
        c_status                    AS campaign_status,
        c_eff                       AS campaign_effective_status,
        obj                         AS objective,
        country,
        region,
        city,
        s_spend                     AS spend,
        s_impr                      AS impressions,
        if(s_impr > 0, s_spend / s_impr * 1000, NULL) AS cpm,
        s_clicks                    AS clicks,
        if(s_impr > 0, s_clicks / s_impr, NULL) AS ctr,
        if(s_clicks > 0, s_spend / s_clicks, NULL) AS cpc,
        if(s_views > 0, s_spend / s_views, NULL) AS cpv,
        s_views                     AS true_views,
        if(s_impr > 0, s_views / s_impr, NULL) AS view_rate,
        now()                       AS loaded_at
    FROM ({agg_inner})
    """
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until,
    )

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 5. core_device_daily_level
# Агрегируем hourly -> daily через GROUP BY
# ============================================================

_HOURLY_FOR_DEVICE_SELECT = """
SELECT
    date_start,
    campaign_id,
    campaign_status,
    campaign_primary_status,
    google_ads_goal_type,
    device,
    spend,
    impressions,
    clicks,
    video_views,
    average_cpv
FROM {db}.{table}
WHERE toDate(date_start)
      BETWEEN toDate('{date_since}')
      AND toDate('{date_until}')"""


def load_device_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "google_ads_core_device_daily_level"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    union_sql = _union_select(
        HOURLY_STAGING_TABLES,
        _HOURLY_FOR_DEVICE_SELECT,
        date_since,
        date_until,
    )

    _agg_inner = (
        "SELECT"
        " toStartOfDay(date_start) AS d_start,"
        " campaign_id AS c_id,"
        " device AS dev,"
        " any(campaign_status) AS c_status,"
        " any(campaign_primary_status) AS c_eff_status,"
        " any(google_ads_goal_type) AS obj,"
        " sum(spend) AS s_spend,"
        " sum(impressions) AS s_impr,"
        " sum(clicks) AS s_clicks,"
        " sum(video_views) AS s_views"
        " FROM (" + union_sql + ")"
        " GROUP BY d_start, c_id, dev"
    )

    _agg_sql = (
        "SELECT"
        " d_start AS date_start,"
        " d_start + INTERVAL 1 DAY AS date_stop,"
        " c_id AS campaign_id,"
        " c_status AS campaign_status,"
        " c_eff_status AS campaign_effective_status,"
        " obj AS objective,"
        " dev AS device,"
        " s_spend AS spend,"
        " s_impr AS impressions,"
        " if(s_impr>0, s_spend/s_impr*1000, NULL) AS cpm,"
        " s_clicks AS clicks,"
        " if(s_impr>0, s_clicks/s_impr, NULL) AS ctr,"
        " if(s_clicks>0, s_spend/s_clicks, NULL) AS cpc,"
        " if(s_views>0, s_spend/s_views, NULL) AS cpv,"
        " s_views AS true_views,"
        " if(s_impr>0, s_views/s_impr, NULL) AS view_rate,"
        " now() AS loaded_at"
        " FROM (" + _agg_inner + ")"
    )
    insert_sql = (
        f"INSERT INTO {CORE_DB}.{table} " + _agg_sql
    )
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until,
    )

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 6. core_gender_daily_level
# ============================================================

def load_gender_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "google_ads_core_gender_daily_level"
    staging_table = "google_ads_gender_daily_level_staging"
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    insert_sql = f"""
    INSERT INTO {CORE_DB}.{table}
    SELECT
        date_start,
        date_stop,
        campaign_id,
        campaign_status,
        campaign_primary_status     AS campaign_effective_status,
        google_ads_goal_type        AS objective,
        gender_type,
        device,
        spend,
        impressions,
        average_cpm                 AS cpm,
        clicks,
        ctr,
        average_cpc                 AS cpc,
        CAST(NULL AS Nullable(Float64)) AS cpv,
        video_views                 AS true_views,
        view_rate,
        now()                       AS loaded_at
    FROM {STAGING_DB}.{staging_table}
    WHERE toDate(date_start)
          BETWEEN toDate('{date_since}')
          AND toDate('{date_until}')
    """
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until,
    )

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 7. core_search_term_daily_level
# ============================================================

def load_search_term_daily(
    *,
    run_id: str,
    date_since: str,
    date_until: str,
) -> int:
    table = "google_ads_core_search_term_daily_level"
    staging_table = (
        "google_ads_daily_search_term_level_staging"
    )
    client = _get_client(CORE_DB)

    rows_before = _count(
        client, CORE_DB, table, date_since, date_until,
    )
    _delete(client, CORE_DB, table, date_since, date_until)

    # Staging search_term сегментирован по device × ad_network,
    # а метрик в core-таблице нет — только измерения. Без DISTINCT
    # строки задвоятся по числу комбинаций устройство×сеть.
    insert_sql = f"""
    INSERT INTO {CORE_DB}.{table}
    SELECT DISTINCT
        date_start,
        date_stop,
        campaign_id,
        campaign_name,
        campaign_status,
        campaign_primary_status     AS campaign_effective_status,
        search_term,
        keyword_text,
        now()                       AS loaded_at
    FROM {STAGING_DB}.{staging_table}
    WHERE toDate(date_start)
          BETWEEN toDate('{date_since}')
          AND toDate('{date_until}')
    """
    client.command(insert_sql)

    rows_after = _count(
        client, CORE_DB, table, date_since, date_until,
    )

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since=date_since,
        date_until=date_until,
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
        min_loaded_date=date_since,
        max_loaded_date=date_until,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# 8. core_creative_assets
# Без date фильтра — перезапись по customer
# ============================================================

def load_creative_assets(
    *,
    run_id: str,
    customer_id: str,
) -> int:
    table = "google_ads_core_creative_assets"
    staging_table = "google_ads_creative_assets_staging"

    client_core = _get_client(CORE_DB)

    rows_before_result = client_core.query(
        f"SELECT count() FROM {CORE_DB}.{table} "
        f"WHERE customer_id = '{customer_id}'"
    )
    rows_before = rows_before_result.first_row[0]

    client_core.command(
        f"ALTER TABLE {CORE_DB}.{table} "
        f"DELETE WHERE customer_id = '{customer_id}'"
    )

    insert_sql = f"""
    INSERT INTO {CORE_DB}.{table}
    SELECT
        source_type, customer_id, customer_name,
        campaign_id, campaign_name, campaign_status,
        advertising_channel_type,
        advertising_channel_sub_type,
        ad_group_id, ad_group_name, ad_group_status,
        ad_id, ad_name, ad_type, ad_status,
        asset_group_id, asset_group_name,
        asset_group_status, asset_group_strength,
        asset_group_asset_status,
        asset_id, asset_name, asset_type, asset_field_type,
        image_url, image_width, image_height,
        image_mime_type, image_file_size,
        youtube_video_id, youtube_video_url,
        youtube_video_title,
        now() AS loaded_at
    FROM {STAGING_DB}.{staging_table}
    WHERE customer_id = '{customer_id}'
    """
    client_core.command(insert_sql)

    rows_after_result = client_core.query(
        f"SELECT count() FROM {CORE_DB}.{table} "
        f"WHERE customer_id = '{customer_id}'"
    )
    rows_after = rows_after_result.first_row[0]

    etl_logger.log_table_load(
        run_id=run_id,
        layer="core",
        database_name=CORE_DB,
        table_name=table,
        date_since="1970-01-01",
        date_until="2099-12-31",
        rows_before=rows_before,
        rows_deleted=rows_before,
        rows_inserted=rows_after,
        rows_after=rows_after,
    )

    print(
        f"[CORE] {table}: "
        f"before={rows_before}, after={rows_after}"
    )
    return rows_after


# ============================================================
# Главная функция — все core загрузки
# ============================================================

def run_staging_to_core(
    *,
    run_id: str,
    customer_id: str,
    date_since: str,
    date_until: str,
    load_creatives: bool = True,
) -> int:
    """
    Запускает все core загрузки за период.
    Возвращает суммарное кол-во строк в core.
    """
    total = 0

    with etl_logger.etl_step(
        run_id=run_id,
        step_name="staging_to_core",
        step_order=3,
        target_database=CORE_DB,
    ) as step:

        total += load_daily_campaign(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        total += load_daily_ad(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        total += load_hourly_campaign(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        total += load_geo_daily(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        total += load_device_daily(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        total += load_gender_daily(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        total += load_search_term_daily(
            run_id=run_id,
            date_since=date_since,
            date_until=date_until,
        )

        if load_creatives:
            total += load_creative_assets(
                run_id=run_id,
                customer_id=customer_id,
            )

        step["output_rows"] = total

    return total


# ============================================================
# Подсчёт фактических строк в core за период
# ============================================================

CORE_DATE_TABLES = [
    "google_ads_core_daily_campaign_level",
    "google_ads_core_daily_ad_level",
    "google_ads_core_hourly_campaign_level",
    "google_ads_core_geo_daily_level",
    "google_ads_core_device_daily_level",
    "google_ads_core_gender_daily_level",
    "google_ads_core_search_term_daily_level",
]


def count_core_rows(
    *,
    date_since: str,
    date_until: str,
) -> int:
    """
    Считает фактические строки в core.
    Для total_core_rows в etl_runs.
    """
    client = _get_client(CORE_DB)
    total = 0

    for table in CORE_DATE_TABLES:
        total += _count(
            client, CORE_DB, table,
            date_since, date_until,
        )

    result = client.query(
        f"SELECT count() FROM {CORE_DB}"
        f".google_ads_core_creative_assets"
    )
    total += result.first_row[0]

    return total
