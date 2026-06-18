"""
etl_logger.py

Модуль для записи ETL метаданных:
- etl_runs        — один запуск пайплайна
- etl_step_runs   — шаги внутри запуска
- etl_table_loads — статистика по каждой таблице
- etl_data_quality_checks — проверки качества данных
"""

import traceback
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from zoneinfo import ZoneInfo

import clickhouse_connect

import config


ALMATY_TZ = ZoneInfo("Asia/Almaty")

PIPELINE_NAME = "google_ads_pipeline"
SOURCE_PLATFORM = "google_ads"

# Порядок шагов
STEP_FETCH_RAW = ("fetch_raw", 1)
STEP_RAW_TO_STAGING = ("raw_to_staging", 2)
STEP_STAGING_TO_CORE = ("staging_to_core", 3)
STEP_DATA_QUALITY = ("data_quality", 4)


def _now() -> datetime:
    return datetime.now(ALMATY_TZ)


def _seconds(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds())


def _get_client():
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_METADATA_DB,
    )


def _count_rows(
    *,
    database: str,
    table: str,
    customer_id: str,
    date_since: str,
    date_until: str,
) -> int:
    """Считает строки в таблице за период."""
    client = _get_client()

    # raw_data — по fetched_at, остальные по date_start
    if table == "raw_data":
        sql = f"""
        SELECT count()
        FROM {database}.{table}
        WHERE customer_id = '{customer_id}'
          AND toDate(fetched_at) BETWEEN
              toDate('{date_since}') AND toDate('{date_until}')
        """
    else:
        sql = f"""
        SELECT count()
        FROM {database}.{table}
        WHERE customer_id = '{customer_id}'
          AND toDate(date_start) BETWEEN
              toDate('{date_since}') AND toDate('{date_until}')
        """

    result = client.query(sql)
    return result.first_row[0]


def _count_rows_no_filter(
    *,
    database: str,
    table: str,
) -> int:
    """Считает все строки в таблице (для core)."""
    client = _get_client()
    sql = f"SELECT count() FROM {database}.{table}"
    result = client.query(sql)
    return result.first_row[0]


# ============================================================
# etl_runs
# ============================================================

def create_run(
    *,
    run_type: str,
    date_since: str,
    date_until: str,
) -> str:
    """
    Создаёт запись о запуске пайплайна.
    Возвращает run_id.
    run_type: 'backfill' | 'daily'
    """
    run_id = str(uuid.uuid4())
    client = _get_client()

    date_since_obj = date.fromisoformat(date_since)
    date_until_obj = date.fromisoformat(date_until)

    client.insert(
        "etl_runs",
        [[
            run_id,
            PIPELINE_NAME,
            SOURCE_PLATFORM,
            run_type,
            "running",
            date_since_obj,
            date_until_obj,
            None,           # actual_min_date
            None,           # actual_max_date
            _now(),         # started_at
            None,           # finished_at
            None,           # duration_seconds
            0,              # total_raw_rows
            0,              # total_staging_rows
            0,              # total_core_rows
            None,           # error_stage
            None,           # error_message
            None,           # error_trace
        ]],
        column_names=[
            "run_id", "pipeline_name", "source_platform",
            "run_type", "status",
            "requested_date_since", "requested_date_until",
            "actual_min_date", "actual_max_date",
            "started_at", "finished_at", "duration_seconds",
            "total_raw_rows", "total_staging_rows",
            "total_core_rows",
            "error_stage", "error_message", "error_trace",
        ],
    )

    print(f"[ETL] Run created: {run_id}")
    return run_id


def finish_run(
    *,
    run_id: str,
    started_at: datetime,
    status: str,
    total_raw_rows: int = 0,
    total_staging_rows: int = 0,
    total_core_rows: int = 0,
    actual_min_date: str | None = None,
    actual_max_date: str | None = None,
    error_stage: str | None = None,
    error_message: str | None = None,
    error_trace: str | None = None,
) -> None:
    """Обновляет запись о запуске."""
    finished_at = _now()
    client = _get_client()

    def _esc(val: str | None, limit: int = 500) -> str:
        if not val:
            return "NULL"
        safe = (
            val[:limit]
            .replace("\\", "\\\\")
            .replace("'", "\\'")
        )
        return f"'{safe}'"

    min_date = (
        f"'{actual_min_date}'"
        if actual_min_date else "NULL"
    )
    max_date = (
        f"'{actual_max_date}'"
        if actual_max_date else "NULL"
    )
    dur = _seconds(started_at, finished_at)
    db = config.CLICKHOUSE_METADATA_DB

    client.command(
        f"ALTER TABLE {db}.etl_runs UPDATE "
        f"status = '{status}', "
        f"finished_at = '{finished_at}', "
        f"duration_seconds = {dur}, "
        f"total_raw_rows = {total_raw_rows}, "
        f"total_staging_rows = {total_staging_rows}, "
        f"total_core_rows = {total_core_rows}, "
        f"actual_min_date = {min_date}, "
        f"actual_max_date = {max_date}, "
        f"error_stage = {_esc(error_stage)}, "
        f"error_message = {_esc(error_message, 500)}, "
        f"error_trace = {_esc(error_trace, 2000)} "
        f"WHERE run_id = '{run_id}'"
    )
    print(f"[ETL] Run finished: {run_id} -> {status}")


# ============================================================
# etl_step_runs
# ============================================================

def create_step(
    *,
    run_id: str,
    step_name: str,
    step_order: int,
    target_database: str | None = None,
    target_table: str | None = None,
) -> tuple[str, datetime]:
    """
    Создаёт запись о шаге.
    Возвращает (step_run_id, started_at).
    """
    step_run_id = str(uuid.uuid4())
    started_at = _now()
    client = _get_client()

    client.insert(
        "etl_step_runs",
        [[
            run_id,
            PIPELINE_NAME,
            SOURCE_PLATFORM,
            step_name,
            step_order,
            "running",
            started_at,
            None,
            None,
            0,
            0,
            target_database,
            target_table,
            None,
            None,
        ]],
        column_names=[
            "run_id", "pipeline_name", "source_platform",
            "step_name", "step_order", "status",
            "started_at", "finished_at", "duration_seconds",
            "input_rows", "output_rows",
            "target_database", "target_table",
            "error_message", "error_trace",
        ],
    )

    return step_run_id, started_at


def finish_step(
    *,
    run_id: str,
    step_name: str,
    started_at: datetime,
    status: str,
    input_rows: int = 0,
    output_rows: int = 0,
    error_message: str | None = None,
    error_trace: str | None = None,
) -> None:
    """Обновляет запись о шаге."""
    finished_at = _now()
    client = _get_client()

    def _esc(val: str | None, limit: int = 500) -> str:
        if not val:
            return "NULL"
        safe = (
            val[:limit]
            .replace("\\", "\\\\")
            .replace("'", "\\'")
        )
        return f"'{safe}'"

    dur = _seconds(started_at, finished_at)
    db = config.CLICKHOUSE_METADATA_DB

    client.command(
        f"ALTER TABLE {db}.etl_step_runs UPDATE "
        f"status = '{status}', "
        f"finished_at = '{finished_at}', "
        f"duration_seconds = {dur}, "
        f"input_rows = {input_rows}, "
        f"output_rows = {output_rows}, "
        f"error_message = {_esc(error_message, 500)}, "
        f"error_trace = {_esc(error_trace, 2000)} "
        f"WHERE run_id = '{run_id}' "
        f"AND step_name = '{step_name}'"
    )
    print(
        f"[ETL] Step '{step_name}' finished -> {status} "
        f"(in={input_rows}, out={output_rows})"
    )


# ============================================================
# etl_table_loads
# ============================================================

def log_table_load(
    *,
    run_id: str,
    layer: str,
    database_name: str,
    table_name: str,
    date_since: str,
    date_until: str,
    rows_before: int,
    rows_deleted: int,
    rows_inserted: int,
    rows_after: int,
    min_loaded_date: str | None = None,
    max_loaded_date: str | None = None,
) -> None:
    """
    Записывает статистику загрузки таблицы.
    layer: 'raw' | 'staging' | 'core'
    """
    client = _get_client()

    date_since_obj = date.fromisoformat(date_since)
    date_until_obj = date.fromisoformat(date_until)
    min_date_obj = (
        date.fromisoformat(min_loaded_date)
        if min_loaded_date
        else None
    )
    max_date_obj = (
        date.fromisoformat(max_loaded_date)
        if max_loaded_date
        else None
    )

    client.insert(
        "etl_table_loads",
        [[
            run_id,
            PIPELINE_NAME,
            SOURCE_PLATFORM,
            layer,
            database_name,
            table_name,
            date_since_obj,
            date_until_obj,
            rows_before,
            rows_deleted,
            rows_inserted,
            rows_after,
            min_date_obj,
            max_date_obj,
            _now(),
        ]],
        column_names=[
            "run_id", "pipeline_name", "source_platform",
            "layer", "database_name", "table_name",
            "date_since", "date_until",
            "rows_before", "rows_deleted",
            "rows_inserted", "rows_after",
            "min_loaded_date", "max_loaded_date",
            "loaded_at",
        ],
    )


# ============================================================
# etl_data_quality_checks
# ============================================================

def run_quality_checks(
    *,
    run_id: str,
    database_name: str,
    table_name: str,
    date_since: str,
    date_until: str,
    key_columns: list[str],
) -> bool:
    """
    Запускает проверки качества для таблицы.
    Возвращает True если все проверки прошли.

    Проверки:
    1. null_check     — NULL в ключевых колонках
    2. negative_check — отрицательные spend/impressions
    3. duplicate_check — дубли по ключу
    4. future_date_check — loaded_at в будущем
    """
    client = _get_client()
    all_passed = True

    date_filter = (
        f"toDate(date_start) BETWEEN "
        f"toDate('{date_since}') AND toDate('{date_until}')"
    )

    checks = []

    # 1. NULL check по ключевым колонкам
    for col in key_columns:
        null_sql = f"""
        SELECT count()
        FROM {database_name}.{table_name}
        WHERE {date_filter}
          AND ({col} IS NULL OR toString({col}) = '')
        """
        try:
            result = client.query(null_sql)
            failed = result.first_row[0]
            checks.append({
                "name": f"null_check_{col}",
                "level": "error",
                "status": "passed" if failed == 0 else "failed",
                "failed_rows": failed,
                "details": (
                    f"NULL values in {col}: {failed}"
                    if failed > 0
                    else f"No NULLs in {col}"
                ),
            })
            if failed > 0:
                all_passed = False
        except Exception as e:
            checks.append({
                "name": f"null_check_{col}",
                "level": "error",
                "status": "error",
                "failed_rows": 0,
                "details": str(e)[:500],
            })
            all_passed = False

    # 2. Negative values check (только если есть spend)
    has_spend_sql = f"""
    SELECT count()
    FROM system.columns
    WHERE database = '{database_name}'
      AND table = '{table_name}'
      AND name = 'spend'
    """
    has_spend = client.query(has_spend_sql).first_row[0] > 0

    if has_spend:
        neg_sql = f"""
        SELECT count()
        FROM {database_name}.{table_name}
        WHERE {date_filter}
          AND (spend < 0 OR impressions < 0)
        """
        try:
            result = client.query(neg_sql)
            failed = result.first_row[0]
            checks.append({
                "name": "negative_values_check",
                "level": "warning",
                "status": (
                    "passed" if failed == 0 else "failed"
                ),
                "failed_rows": failed,
                "details": (
                    f"Negative spend/impressions: {failed}"
                    if failed > 0
                    else "No negative values"
                ),
            })
        except Exception as e:
            checks.append({
                "name": "negative_values_check",
                "level": "warning",
                "status": "error",
                "failed_rows": 0,
                "details": str(e)[:500],
            })

    # 3. Duplicate check по полному ключу строки
    # key_columns — уникальный ключ таблицы
    dup_keys = ", ".join(key_columns)
    dup_sql = f"""
    SELECT count()
    FROM (
        SELECT {dup_keys}, count() AS cnt
        FROM {database_name}.{table_name}
        WHERE {date_filter}
        GROUP BY {dup_keys}
        HAVING cnt > 1
    )
    """
    try:
        result = client.query(dup_sql)
        failed = result.first_row[0]
        checks.append({
            "name": "duplicate_check",
            "level": "warning",
            "status": "passed" if failed == 0 else "failed",
            "failed_rows": failed,
            "details": (
                f"Duplicate key combos ({dup_keys}): {failed}"
                if failed > 0
                else "No duplicates found"
            ),
        })
    except Exception as e:
        checks.append({
            "name": "duplicate_check",
            "level": "warning",
            "status": "error",
            "failed_rows": 0,
            "details": str(e)[:500],
        })

    # 4. Future date check
    future_sql = f"""
    SELECT count()
    FROM {database_name}.{table_name}
    WHERE {date_filter}
      AND loaded_at > now() + INTERVAL 1 HOUR
    """
    try:
        result = client.query(future_sql)
        failed = result.first_row[0]
        checks.append({
            "name": "future_date_check",
            "level": "warning",
            "status": "passed" if failed == 0 else "failed",
            "failed_rows": failed,
            "details": (
                f"Rows with future loaded_at: {failed}"
                if failed > 0
                else "No future dates"
            ),
        })
    except Exception as e:
        checks.append({
            "name": "future_date_check",
            "level": "warning",
            "status": "error",
            "failed_rows": 0,
            "details": str(e)[:500],
        })

    # Записываем все проверки
    now = _now()
    rows = [
        [
            run_id,
            PIPELINE_NAME,
            SOURCE_PLATFORM,
            c["name"],
            c["level"],
            database_name,
            table_name,
            c["status"],
            now,
            c["failed_rows"],
            c["details"],
        ]
        for c in checks
    ]

    if rows:
        client.insert(
            "etl_data_quality_checks",
            rows,
            column_names=[
                "run_id", "pipeline_name", "source_platform",
                "check_name", "check_level",
                "database_name", "table_name",
                "status", "checked_at",
                "failed_rows", "details",
            ],
        )

    passed = sum(1 for c in checks if c["status"] == "passed")
    failed = sum(1 for c in checks if c["status"] == "failed")
    print(
        f"[ETL] Quality checks {table_name}: "
        f"{passed} passed, {failed} failed"
    )

    return all_passed


# ============================================================
# Context manager для удобного использования в main.py
# ============================================================

@contextmanager
def etl_step(
    *,
    run_id: str,
    step_name: str,
    step_order: int,
    target_database: str | None = None,
    target_table: str | None = None,
):
    """
    Context manager для шага ETL.

    Использование:
        with etl_step(run_id=run_id, step_name='raw_to_staging',
                      step_order=2) as step:
            step['input_rows'] = 100
            # ... делаем работу ...
            step['output_rows'] = 95
    """
    _, started_at = create_step(
        run_id=run_id,
        step_name=step_name,
        step_order=step_order,
        target_database=target_database,
        target_table=target_table,
    )

    step_info: dict = {
        "input_rows": 0,
        "output_rows": 0,
    }

    try:
        yield step_info

        finish_step(
            run_id=run_id,
            step_name=step_name,
            started_at=started_at,
            status="success",
            input_rows=step_info["input_rows"],
            output_rows=step_info["output_rows"],
        )

    except Exception as e:
        finish_step(
            run_id=run_id,
            step_name=step_name,
            started_at=started_at,
            status="failed",
            input_rows=step_info.get("input_rows", 0),
            output_rows=step_info.get("output_rows", 0),
            error_message=str(e),
            error_trace=traceback.format_exc(),
        )
        raise
