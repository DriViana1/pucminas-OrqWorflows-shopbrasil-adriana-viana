import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins"))

from operators.validar_produtos import ValidarProdutosOperator, validar_produtos  # noqa: E402


def _p(**over):
    base = {"id": 1, "category": "electronics", "price": 10.0}
    base.update(over)
    return base


def _run(products, category="electronics"):
    op = ValidarProdutosOperator(task_id="validar", products=products, category=category)
    return op.execute({})


def test_all_valid_returns_same_products():
    products = [_p(id=1), _p(id=2, price=5)]
    assert _run(products) == products


def test_partial_invalid_discards_and_warns(caplog):
    good = _p(id=1)
    bad = [_p(id=2, price=-1), _p(id=3, price=None), _p(id=4, price="9.9"), _p(id=5, category="")]
    with caplog.at_level(logging.WARNING):
        result = _run([good, *bad])
    assert result == [good]
    assert "4 de 5 produtos descartados" in caplog.text


def test_empty_list_fails():
    with pytest.raises(ValueError, match="vazia"):
        _run([])


def test_all_invalid_fails():
    with pytest.raises(ValueError, match="Nenhum produto valido"):
        _run([_p(price=0), _p(id=2, price=float("nan"))])


@pytest.mark.parametrize(
    "bad",
    [
        {"category": "x", "price": 1.0},   # sem id
        _p(price=True),                    # bool nao e preco
        _p(price=float("inf")),
        "texto",
        None,
    ],
)
def test_invalid_products_are_reported(bad):
    valid, errors = validar_produtos([bad])
    assert valid == [] and len(errors) == 1
