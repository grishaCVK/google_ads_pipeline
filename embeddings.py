"""
embeddings.py

Векторные эмбеддинги креативов Google Ads.
Читает креативы из core (google_ads_core_creative_assets),
скачивает картинки и кадры YouTube-видео, считает CLIP-эмбеддинги
и пишет их в Postgres (pgvector). Запускается после основного
пайплайна; управляется GOOGLE_EMBEDDINGS_ENABLED.
"""

import os
import tempfile
from datetime import datetime, timezone
from typing import Any

import cv2
import numpy as np
import psycopg2
import requests
import yt_dlp
from PIL import Image
from sentence_transformers import SentenceTransformer

import clickhouse_db


EMBEDDING_MODEL_NAME = os.getenv(
    "GOOGLE_EMBEDDING_MODEL",
    "sentence-transformers/clip-ViT-B-32",
)

POSTGRES_HOST = os.getenv(
    "GOOGLE_EMBEDDINGS_POSTGRES_HOST",
    os.getenv("POSTGRES_HOST", "host.docker.internal"),
)
POSTGRES_PORT = int(
    os.getenv(
        "GOOGLE_EMBEDDINGS_POSTGRES_PORT",
        os.getenv("POSTGRES_PORT", "5432"),
    )
)
POSTGRES_DB = os.getenv("GOOGLE_EMBEDDINGS_DB", "google_ads_embeddings")
POSTGRES_USER = os.getenv(
    "GOOGLE_EMBEDDINGS_POSTGRES_USER",
    os.getenv("POSTGRES_USER", "postgres"),
)
POSTGRES_PASSWORD = os.getenv(
    "GOOGLE_EMBEDDINGS_POSTGRES_PASSWORD",
    os.getenv("POSTGRES_PASSWORD", "postgres"),
)

GOOGLE_EMBEDDINGS_TABLE = "google_ad_embeddings"
FRAME_PERCENTS = [0, 25, 50, 75, 100]


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def vector_to_pgvector_text(vector: list[float]) -> str:
    return "[" + ",".join(str(float(item)) for item in vector) + "]"


def download_file(url: str, output_path: str, timeout: int = 60) -> None:
    response = requests.get(url, timeout=timeout, stream=True)
    response.raise_for_status()

    with open(output_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)


def download_youtube_video(youtube_url: str, output_dir: str) -> str:
    output_template = os.path.join(output_dir, "%(id)s.%(ext)s")

    ydl_opts = {
        # Берем H.264/AVC mp4, чтобы OpenCV нормально читал видео.
        # AV1 часто вызывает ошибки декодирования
        # внутри slim Docker-контейнера.
        "format": (
            "bestvideo[vcodec^=avc1][ext=mp4]/"
            "best[vcodec^=avc1][ext=mp4]/"
            "best[ext=mp4]/"
            "best"
        ),
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(youtube_url, download=True)
        downloaded_path = ydl.prepare_filename(info)

    if not os.path.exists(downloaded_path):
        raise FileNotFoundError(
            f"YouTube video was not downloaded: {downloaded_path}"
        )

    return downloaded_path


def extract_video_frame(
    *,
    video_path: str,
    frame_percent: int,
    output_path: str,
) -> None:
    video = cv2.VideoCapture(video_path)

    if not video.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")

    frame_count = int(video.get(cv2.CAP_PROP_FRAME_COUNT))

    if frame_count <= 0:
        video.release()
        raise RuntimeError(f"Video has no readable frames: {video_path}")

    if frame_percent <= 0:
        frame_index = 0
    elif frame_percent >= 100:
        frame_index = frame_count - 1
    else:
        frame_index = round((frame_percent / 100) * (frame_count - 1))

    video.set(cv2.CAP_PROP_POS_FRAMES, frame_index)

    success, frame = video.read()
    video.release()

    if not success or frame is None:
        raise RuntimeError(
            f"Cannot read frame {frame_percent}% from video: {video_path}"
        )

    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    image = Image.fromarray(frame_rgb)
    image.save(output_path, format="JPEG", quality=95)


def build_image_embedding(
    *,
    model: SentenceTransformer,
    image_path: str,
) -> list[float]:
    image = Image.open(image_path).convert("RGB")
    embedding = model.encode(image, normalize_embeddings=True)

    if isinstance(embedding, np.ndarray):
        return embedding.astype(float).tolist()

    return list(embedding)


def fetch_google_creative_assets_from_clickhouse(
    *,
    customer_id: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    client = clickhouse_db.get_client()

    where_parts = [
        """
        (
            (asset_type = 'IMAGE'
             AND image_url IS NOT NULL
             AND image_url != '')
            OR
            (asset_type = 'YOUTUBE_VIDEO'
             AND youtube_video_url IS NOT NULL
             AND youtube_video_url != '')
        )
        """,
        "campaign_status != 'REMOVED'",
        "(ad_status IS NULL OR ad_status != 'REMOVED')",
        "(ad_group_status IS NULL OR ad_group_status != 'REMOVED')",
        "(asset_group_status IS NULL OR asset_group_status != 'REMOVED')",
        "(asset_group_asset_status IS NULL"
        " OR asset_group_asset_status != 'REMOVED')",
    ]

    if customer_id:
        where_parts.append(f"customer_id = '{customer_id}'")

    limit_sql = f"LIMIT {int(limit)}" if limit else ""

    query = f"""
    SELECT
        source_type,
        customer_id,
        customer_name,

        campaign_id,
        campaign_name,
        campaign_status,
        advertising_channel_type,
        advertising_channel_sub_type,

        ad_group_id,
        ad_group_name,
        ad_group_status,

        ad_id,
        ad_name,
        ad_type,
        ad_status,

        asset_group_id,
        asset_group_name,
        asset_group_status,
        asset_group_strength,
        asset_group_asset_status,

        asset_id,
        asset_name,
        asset_type,
        asset_field_type,

        image_url,
        image_width,
        image_height,
        image_mime_type,
        image_file_size,

        youtube_video_id,
        youtube_video_url,
        youtube_video_title
    FROM google_ads_core.google_ads_core_creative_assets
    WHERE {" AND ".join(where_parts)}
    ORDER BY campaign_id, asset_id, source_type
    {limit_sql}
    """

    result = client.query(query)

    rows: list[dict[str, Any]] = []

    for item in result.result_rows:
        rows.append(dict(zip(result.column_names, item)))

    return rows


def delete_existing_embedding(
    *,
    conn,
    asset: dict[str, Any],
    embedding_asset_type: str,
    frame_percent: int | None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            DELETE FROM {GOOGLE_EMBEDDINGS_TABLE}
            WHERE customer_id = %s
              AND source_type = %s
              AND asset_id = %s
              AND COALESCE(ad_group_id, '') = COALESCE(%s, '')
              AND COALESCE(ad_id, '') = COALESCE(%s, '')
              AND COALESCE(asset_group_id, '') = COALESCE(%s, '')
              AND embedding_asset_type = %s
              AND COALESCE(frame_percent, -1) = COALESCE(%s, -1)
              AND embedding_model = %s
            """,
            (
                asset["customer_id"],
                asset["source_type"],
                asset["asset_id"],
                asset.get("ad_group_id"),
                asset.get("ad_id"),
                asset.get("asset_group_id"),
                embedding_asset_type,
                frame_percent,
                EMBEDDING_MODEL_NAME,
            ),
        )


def insert_embedding(
    *,
    conn,
    asset: dict[str, Any],
    embedding_asset_type: str,
    frame_percent: int | None,
    embedding: list[float] | None,
    download_status: str,
    embedding_status: str,
    error_message: str | None = None,
) -> None:
    delete_existing_embedding(
        conn=conn,
        asset=asset,
        embedding_asset_type=embedding_asset_type,
        frame_percent=frame_percent,
    )

    embedding_text = (
        vector_to_pgvector_text(embedding)
        if embedding is not None
        else None
    )

    now = datetime.now(timezone.utc)

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {GOOGLE_EMBEDDINGS_TABLE}
            (
                customer_id,
                customer_name,

                campaign_id,
                campaign_name,
                campaign_status,
                advertising_channel_type,
                advertising_channel_sub_type,

                ad_group_id,
                ad_group_name,
                ad_group_status,

                ad_id,
                ad_name,
                ad_type,
                ad_status,

                asset_group_id,
                asset_group_name,
                asset_group_status,
                asset_group_strength,
                asset_group_asset_status,

                source_type,
                asset_id,
                asset_name,
                asset_type,
                asset_field_type,

                image_url,
                image_width,
                image_height,
                image_mime_type,
                image_file_size,

                youtube_video_id,
                youtube_video_url,
                youtube_video_title,

                embedding_asset_type,
                frame_percent,

                embedding_model,
                embedding,

                download_status,
                embedding_status,
                error_message,

                created_at,
                updated_at
            )
            VALUES
            (
                %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s::vector,
                %s, %s, %s,
                %s, %s
            )
            """,
            (
                asset["customer_id"],
                asset.get("customer_name"),

                asset["campaign_id"],
                asset.get("campaign_name"),
                asset.get("campaign_status"),
                asset.get("advertising_channel_type"),
                asset.get("advertising_channel_sub_type"),

                asset.get("ad_group_id"),
                asset.get("ad_group_name"),
                asset.get("ad_group_status"),

                asset.get("ad_id"),
                asset.get("ad_name"),
                asset.get("ad_type"),
                asset.get("ad_status"),

                asset.get("asset_group_id"),
                asset.get("asset_group_name"),
                asset.get("asset_group_status"),
                asset.get("asset_group_strength"),
                asset.get("asset_group_asset_status"),

                asset["source_type"],
                asset["asset_id"],
                asset.get("asset_name"),
                asset.get("asset_type"),
                asset.get("asset_field_type"),

                asset.get("image_url"),
                asset.get("image_width"),
                asset.get("image_height"),
                asset.get("image_mime_type"),
                asset.get("image_file_size"),

                asset.get("youtube_video_id"),
                asset.get("youtube_video_url"),
                asset.get("youtube_video_title"),

                embedding_asset_type,
                frame_percent,

                EMBEDDING_MODEL_NAME,
                embedding_text,

                download_status,
                embedding_status,
                error_message,

                now,
                now,
            ),
        )


def process_image_asset(
    *,
    conn,
    model: SentenceTransformer,
    asset: dict[str, Any],
    temp_dir: str,
) -> None:
    image_path = os.path.join(
        temp_dir,
        f"image_{asset['asset_id']}.jpg",
    )

    try:
        download_file(asset["image_url"], image_path)
        embedding = build_image_embedding(model=model, image_path=image_path)

        insert_embedding(
            conn=conn,
            asset=asset,
            embedding_asset_type="IMAGE",
            frame_percent=None,
            embedding=embedding,
            download_status="success",
            embedding_status="success",
        )

    except Exception as error:
        insert_embedding(
            conn=conn,
            asset=asset,
            embedding_asset_type="IMAGE",
            frame_percent=None,
            embedding=None,
            download_status="failed",
            embedding_status="failed",
            error_message=str(error),
        )


def process_youtube_video_asset(
    *,
    conn,
    model: SentenceTransformer,
    asset: dict[str, Any],
    temp_dir: str,
) -> None:
    try:
        video_path = download_youtube_video(
            asset["youtube_video_url"],
            output_dir=temp_dir,
        )

        for frame_percent in FRAME_PERCENTS:
            frame_path = os.path.join(
                temp_dir,
                f"video_{asset['asset_id']}_{frame_percent}.jpg",
            )

            extract_video_frame(
                video_path=video_path,
                frame_percent=frame_percent,
                output_path=frame_path,
            )

            embedding = build_image_embedding(
                model=model,
                image_path=frame_path,
            )

            insert_embedding(
                conn=conn,
                asset=asset,
                embedding_asset_type="YOUTUBE_VIDEO",
                frame_percent=frame_percent,
                embedding=embedding,
                download_status="success",
                embedding_status="success",
            )

    except Exception as error:
        for frame_percent in FRAME_PERCENTS:
            insert_embedding(
                conn=conn,
                asset=asset,
                embedding_asset_type="YOUTUBE_VIDEO",
                frame_percent=frame_percent,
                embedding=None,
                download_status="failed",
                embedding_status="failed",
                error_message=str(error),
            )


def run_google_embeddings(
    *,
    customer_id: str | None = None,
    limit: int | None = None,
) -> None:
    print("Google Ads embeddings started")
    print(f"model={EMBEDDING_MODEL_NAME}")
    print(f"postgres_db={POSTGRES_DB}")
    print(f"postgres_table={GOOGLE_EMBEDDINGS_TABLE}")

    assets = fetch_google_creative_assets_from_clickhouse(
        customer_id=customer_id,
        limit=limit,
    )

    print(f"Fetched creative assets for embeddings: {len(assets)}")

    if not assets:
        print("No assets for embeddings")
        return

    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    conn = get_postgres_connection()

    processed = 0
    failed = 0

    try:
        with tempfile.TemporaryDirectory(
            prefix="google_ads_embeddings_"
        ) as temp_dir:
            for asset in assets:
                asset_type = asset.get("asset_type")

                try:
                    if asset_type == "IMAGE":
                        process_image_asset(
                            conn=conn,
                            model=model,
                            asset=asset,
                            temp_dir=temp_dir,
                        )
                    elif asset_type == "YOUTUBE_VIDEO":
                        process_youtube_video_asset(
                            conn=conn,
                            model=model,
                            asset=asset,
                            temp_dir=temp_dir,
                        )
                    else:
                        continue

                    conn.commit()
                    processed += 1

                    print(
                        f"processed={processed} | "
                        f"asset_id={asset.get('asset_id')} | "
                        f"asset_type={asset_type}"
                    )

                except Exception as error:
                    conn.rollback()
                    failed += 1

                    print(
                        f"FAILED asset_id={asset.get('asset_id')} | "
                        f"asset_type={asset_type} | error={error}"
                    )

        print(
            f"Google Ads embeddings finished: "
            f"processed_assets={processed}, failed_assets={failed}"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    limit_value = os.getenv("GOOGLE_EMBEDDINGS_LIMIT")

    run_google_embeddings(
        customer_id=os.getenv("GOOGLE_EMBEDDINGS_CUSTOMER_ID"),
        limit=int(limit_value) if limit_value else None,
    )
