"""Persistencia idempotente das metricas por categoria no Postgres."""
from datetime import date, datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "postgres_shopbrasil"

UPSERT_SNAPSHOT_SQL = """
    INSERT INTO category_metrics
        (category, average_price, min_price, max_price, product_count, snapshot_date, updated_at)
    VALUES
        (%(category)s, %(average_price)s, %(min_price)s, %(max_price)s,
         %(product_count)s, %(snapshot_date)s, NOW())
    ON CONFLICT (category, snapshot_date) DO UPDATE SET
        average_price = EXCLUDED.average_price,
        min_price     = EXCLUDED.min_price,
        max_price     = EXCLUDED.max_price,
        product_count = EXCLUDED.product_count,
        updated_at    = NOW()
"""

UPSERT_HISTORY_SQL = """
    INSERT INTO category_metrics_history
        (dag_run_id, category, average_price, min_price, max_price, product_count, execution_date)
    VALUES
        (%(dag_run_id)s, %(category)s, %(average_price)s, %(min_price)s,
         %(max_price)s, %(product_count)s, %(execution_date)s)
    ON CONFLICT (dag_run_id, category) DO UPDATE SET
        average_price  = EXCLUDED.average_price,
        min_price      = EXCLUDED.min_price,
        max_price      = EXCLUDED.max_price,
        product_count  = EXCLUDED.product_count,
        execution_date = EXCLUDED.execution_date
"""

_METRIC_KEYS = ("category", "average_price", "min_price", "max_price", "product_count")


def _naive_utc(value: datetime) -> datetime:
    """A coluna execution_date e TIMESTAMP sem fuso: grava em UTC, sem tzinfo."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def save_metrics(
    rows: list[dict],
    dag_run_id: str,
    execution_date: datetime,
    snapshot_date: date,
    conn_id: str = CONN_ID,
) -> int:
    """Grava snapshot e historico em uma unica transacao. Pode ser reexecutado.

    - category_metrics: upsert por (category, snapshot_date).
    - category_metrics_history: upsert por (dag_run_id, category).

    Retorna o numero de categorias gravadas.
    """
    if not rows:
        raise ValueError("Nada para gravar: lista de metricas vazia")
    for row in rows:
        missing = [k for k in _METRIC_KEYS if k not in row]
        if missing:
            raise ValueError(f"Metrica sem campos {missing}: {row!r}")

    execution_date = _naive_utc(execution_date)
    snapshot_params = [{**{k: r[k] for k in _METRIC_KEYS}, "snapshot_date": snapshot_date} for r in rows]
    history_params = [
        {**{k: r[k] for k in _METRIC_KEYS}, "dag_run_id": dag_run_id, "execution_date": execution_date}
        for r in rows
    ]

    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SNAPSHOT_SQL, snapshot_params)
            cur.executemany(UPSERT_HISTORY_SQL, history_params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(rows)
