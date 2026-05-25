-- ============================================================
-- PostgreSQL + pgvector schema for Google Ads creative embeddings
-- Database: google_ads_embeddings
-- Table: google_ad_embeddings
-- ============================================================

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;


CREATE TABLE IF NOT EXISTS google_ad_embeddings
(
    embedding_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- account
    customer_id TEXT NOT NULL,
    customer_name TEXT,

    -- campaign
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    campaign_status TEXT,
    advertising_channel_type TEXT,
    advertising_channel_sub_type TEXT,

    -- ad group / ad
    ad_group_id TEXT,
    ad_group_name TEXT,
    ad_group_status TEXT,

    ad_id TEXT,
    ad_name TEXT,
    ad_type TEXT,
    ad_status TEXT,

    -- Performance Max asset group
    asset_group_id TEXT,
    asset_group_name TEXT,
    asset_group_status TEXT,
    asset_group_strength TEXT,
    asset_group_asset_status TEXT,

    -- asset
    source_type TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    asset_name TEXT,
    asset_type TEXT,
    asset_field_type TEXT,

    -- image asset
    image_url TEXT,
    image_width INTEGER,
    image_height INTEGER,
    image_mime_type TEXT,
    image_file_size BIGINT,

    -- YouTube video asset
    youtube_video_id TEXT,
    youtube_video_url TEXT,
    youtube_video_title TEXT,

    -- embedding target
    embedding_asset_type TEXT NOT NULL,
    -- IMAGE = one row, frame_percent NULL
    -- YOUTUBE_VIDEO = five rows: 0, 25, 50, 75, 100
    frame_percent SMALLINT,

    -- embedding
    embedding_model TEXT NOT NULL,
    embedding vector(512),

    -- processing statuses
    download_status TEXT NOT NULL DEFAULT 'pending',
    embedding_status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,

    -- service
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- Чтобы повторный запуск не создавал дубли.
-- Для картинки frame_percent = NULL, поэтому используем COALESCE(frame_percent, -1).
CREATE UNIQUE INDEX IF NOT EXISTS ux_google_ad_embeddings_asset_frame
ON google_ad_embeddings
(
    customer_id,
    source_type,
    asset_id,
    COALESCE(ad_group_id, ''),
    COALESCE(ad_id, ''),
    COALESCE(asset_group_id, ''),
    embedding_asset_type,
    COALESCE(frame_percent, -1),
    embedding_model
);


CREATE INDEX IF NOT EXISTS idx_google_ad_embeddings_customer
ON google_ad_embeddings (customer_id);


CREATE INDEX IF NOT EXISTS idx_google_ad_embeddings_campaign
ON google_ad_embeddings (campaign_id);


CREATE INDEX IF NOT EXISTS idx_google_ad_embeddings_asset
ON google_ad_embeddings (asset_id);


CREATE INDEX IF NOT EXISTS idx_google_ad_embeddings_asset_type
ON google_ad_embeddings (asset_type);


CREATE INDEX IF NOT EXISTS idx_google_ad_embeddings_embedding_status
ON google_ad_embeddings (embedding_status);


-- ANN индекс для поиска похожих креативов.
-- На маленьком количестве данных PostgreSQL может дать warning — это нормально.
CREATE INDEX IF NOT EXISTS idx_google_ad_embeddings_vector
ON google_ad_embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
