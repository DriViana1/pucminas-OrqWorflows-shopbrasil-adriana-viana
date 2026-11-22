"""Pipeline de analise de precos da Fake Store API.

ingestao:  buscar_produtos
analise:   extrair_categorias -> validar_produtos[*] -> calcular_metricas[*] -> consolidar
persistir: grava snapshot e historico no Postgres (idempotente)

[*] Dynamic Task Mapping (expand), um por categoria, limitado pelo pool `ecommerce_pool`.
"""
import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

from include.api.fakestore_client import FakeStoreClient
from include.callbacks.alerts import on_failure_alert, on_retry_alert, on_success_alert
from include.database.postgres_repository import save_metrics
from include.processing.metrics import compute_category_metrics, consolidate_metrics
from operators.validar_produtos import ValidarProdutosOperator

log = logging.getLogger(__name__)

TIMEZONE = "America/Sao_Paulo"
POOL = "ecommerce_pool"

default_args = {
    "owner": "adriana-viana",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id="ecommerce_pipeline",
    description="Metricas de preco por categoria (Fake Store API -> Postgres)",
    schedule="0 6 * * *",  # diario, 06:00 no fuso do start_date
    start_date=pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["shopbrasil", "ecommerce"],
)
def ecommerce_pipeline():
    """Pipeline de analise de precos da Fake Store API."""

    @task_group(group_id="ingestao")
    def ingestao():
        """Busca produtos da Fake Store API e retorna lista de dicts."""
        @task(
            retries=3,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
            on_failure_callback=on_failure_alert,
            on_retry_callback=on_retry_alert,
            on_success_callback=on_success_alert,
        )
        def buscar_produtos() -> list[dict]:
            """Busca produtos da Fake Store API e retorna lista de dicts."""
            log.info("Buscando produtos da Fake Store API...")

            try:
                produtos = FakeStoreClient().get_products()
                log.info("%d produtos recebidos da API", len(produtos))
                return produtos

            except Exception as e:
                log.error("Erro ao buscar produtos da Fake Store API: %s", e)
                raise

            

        return buscar_produtos()

    @task_group(group_id="analise")
    def analise(produtos):
        """Analisa os produtos e calcula métricas por categoria."""

        @task
        def extrair_categorias(produtos: list[dict]) -> list[dict]:
            """Agrupa os produtos por categoria: [{"category": ..., "products": [...]}]."""

            grupos: dict[str, list[dict]] = {}
            sem_categoria = 0
            for produto in produtos:
                categoria = produto.get("category") if isinstance(produto, dict) else None
                if not categoria:
                    sem_categoria += 1
                    continue
                grupos.setdefault(categoria, []).append(produto)
            if sem_categoria:
                log.warning("%d produtos sem categoria ignorados", sem_categoria)
            if not grupos:
                raise ValueError("Nenhuma categoria encontrada nos produtos recebidos")
            log.info("Categorias: %s", sorted(grupos))
            return [{"category": c, "products": p} for c, p in sorted(grupos.items())]

        @task(
            pool=POOL, 
            map_index_template="{{ categoria }}"
        )
        def calcular_metricas(products: list[dict]) -> dict:
            """Calcula métricas de preço para uma categoria de produtos."""

            log.info("Calculando métricas para categoria %s (%d produtos)", products[0]["category"], len(products))
            categoria = products[0]["category"]
            get_current_context()["categoria"] = categoria  # nome na UI, no lugar do indice
            return compute_category_metrics(categoria, products)

        @task
        def consolidar(metricas) -> list[dict]:
            """Consolida as métricas calculadas em uma lista de dicts."""
            return consolidate_metrics(list(metricas))

        grupos = extrair_categorias(produtos)
        validos = ValidarProdutosOperator.partial(
            task_id="validar_produtos", pool=POOL
        ).expand_kwargs(grupos)
        metricas = calcular_metricas.expand(products=validos.output)
        return consolidar(metricas)

    @task
    def persistir(metricas: list[dict]) -> int:
        """Persiste as métricas no Postgres e retorna o total de categorias gravadas."""
        dag_run = get_current_context()["dag_run"]
        referencia = dag_run.logical_date or dag_run.run_after  # logical_date pode ser None em runs manuais
        snapshot_date = referencia.astimezone(ZoneInfo(TIMEZONE)).date()
        total = save_metrics(
            metricas,
            dag_run_id=dag_run.run_id,
            execution_date=referencia,
            snapshot_date=snapshot_date,
        )
        log.info("%d categorias gravadas (snapshot %s)", total, snapshot_date)
        return total

    persistir(analise(ingestao()))


ecommerce_pipeline()
