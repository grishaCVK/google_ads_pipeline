import os

from dotenv import load_dotenv

load_dotenv()


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.lower() in ("1", "true", "yes", "y", "on")


GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or None

_raw_customer_ids = os.getenv("GOOGLE_ADS_CUSTOMER_IDS", "")
GOOGLE_ADS_CUSTOMER_IDS = [
    item.strip()
    for item in _raw_customer_ids.split(",")
    if item.strip()
]

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "host.docker.internal")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

CLICKHOUSE_RAW_DB = os.getenv("CLICKHOUSE_RAW_DB", "google_ads_raw")
CLICKHOUSE_STAGING_DB = os.getenv("CLICKHOUSE_STAGING_DB", "google_ads_staging")
CLICKHOUSE_CORE_DB = os.getenv("CLICKHOUSE_CORE_DB", "google_ads_core")
CLICKHOUSE_METADATA_DB = os.getenv("CLICKHOUSE_METADATA_DB", "etl_metadata")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "google_ads_embeddings")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

BACKFILL_MODE = get_bool_env("BACKFILL_MODE", True)
BACKFILL_START_DATE = os.getenv("BACKFILL_START_DATE") or None
BACKFILL_END_DATE = os.getenv("BACKFILL_END_DATE") or None
BACKFILL_BATCH_DAYS = int(os.getenv("BACKFILL_BATCH_DAYS", "1"))


def validate_config() -> None:
    required = {
        "GOOGLE_ADS_DEVELOPER_TOKEN": GOOGLE_ADS_DEVELOPER_TOKEN,
        "GOOGLE_ADS_CLIENT_ID": GOOGLE_ADS_CLIENT_ID,
        "GOOGLE_ADS_CLIENT_SECRET": GOOGLE_ADS_CLIENT_SECRET,
        "GOOGLE_ADS_REFRESH_TOKEN": GOOGLE_ADS_REFRESH_TOKEN,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise ValueError(
            f"Missing required env variables: {', '.join(missing)}"
        )

    if not GOOGLE_ADS_CUSTOMER_IDS:
        print(
            "Warning: GOOGLE_ADS_CUSTOMER_IDS is empty. "
            "You can still run accessible customers check."
        )
