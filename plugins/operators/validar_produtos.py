"""Operador que valida produtos da Fake Store API antes do calculo de metricas."""
import logging
import math
from numbers import Real
from typing import Optional

from airflow.sdk import BaseOperator

log = logging.getLogger(__name__)


def _product_error(product) -> Optional[str]:
    """Retorna o motivo da invalidez do produto, ou None se ele for valido."""
    if not isinstance(product, dict):
        return f"produto nao e um objeto: {type(product).__name__}"
    if product.get("id") is None:
        return "sem id"
    if not product.get("category"):
        return "sem categoria"
    price = product.get("price")
    if isinstance(price, bool) or not isinstance(price, Real):
        return f"preco nao numerico: {price!r}"
    if not math.isfinite(price) or price <= 0:
        return f"preco invalido: {price!r}"
    return None


def validar_produtos(products: list) -> tuple[list[dict], list[str]]:
    """Separa produtos validos dos invalidos.

    Retorna (validos, erros), com um texto de erro por produto descartado.
    """
    valid, errors = [], []
    for product in products:
        error = _product_error(product)
        if error is None:
            valid.append(product)
        else:
            pid = product.get("id") if isinstance(product, dict) else None
            errors.append(f"id={pid}: {error}")
    return valid, errors


class ValidarProdutosOperator(BaseOperator):
    """Valida a lista de produtos de uma categoria.

    - Lista vazia ou sem nenhum produto valido: falha a task.
    - Alguns produtos invalidos: descarta, registra aviso no log e segue.

    Retorna (via XCom) a lista de produtos validos.
    """

    # Sem template_fields: evita que o Jinja renderize textos dos produtos (ex.: description).

    def __init__(self, *, products, category: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.products = products
        self.category = category

    def execute(self, context):
        label = self.category or "categoria"
        if not self.products:
            raise ValueError(f"[{label}] Lista de produtos vazia")

        valid, errors = validar_produtos(self.products)

        if not valid:
            raise ValueError(
                f"[{label}] Nenhum produto valido em {len(self.products)}. "
                f"Motivos: {errors[:5]}"
            )
        if errors:
            log.warning(
                "[%s] %d de %d produtos descartados: %s",
                label, len(errors), len(self.products), errors,
            )
        else:
            log.info("[%s] %d produtos validos", label, len(valid))
        return valid
