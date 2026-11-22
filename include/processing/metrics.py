"""Calculo de metricas de preco por categoria (funcoes puras, sem I/O)."""
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

_CENTS = Decimal("0.01")


def _to_decimal(product: dict) -> Decimal:
    try:
        return Decimal(str(product["price"]))
    except (KeyError, InvalidOperation) as exc:
        raise ValueError(f"Produto sem preco valido: {product!r}") from exc


def _money(value: Decimal) -> float:
    return float(value.quantize(_CENTS, rounding=ROUND_HALF_UP))


def compute_category_metrics(category: str, products: list[dict]) -> dict:
    """Calcula media, minimo, maximo e quantidade de produtos de uma categoria.

    O retorno e serializavel em JSON, para trafegar via XCom.
    Levanta ValueError se a lista for vazia ou se algum produto nao tiver preco valido.
    """
    if not products:
        raise ValueError(f"Categoria sem produtos: {category!r}")

    prices = [_to_decimal(p) for p in products]
    return {
        "category": category,
        "average_price": _money(sum(prices) / len(prices)),
        "min_price": _money(min(prices)),
        "max_price": _money(max(prices)),
        "product_count": len(prices),
    }


def consolidate_metrics(results: list[dict]) -> list[dict]:
    """Fan-in: junta os resultados por categoria, ordenados por nome.

    Levanta ValueError se houver categorias repetidas.
    """
    categories = [r["category"] for r in results]
    duplicated = {c for c in categories if categories.count(c) > 1}
    if duplicated:
        raise ValueError(f"Categorias duplicadas: {sorted(duplicated)}")
    return sorted(results, key=lambda r: r["category"])
