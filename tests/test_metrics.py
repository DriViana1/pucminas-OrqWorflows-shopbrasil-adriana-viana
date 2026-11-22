import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.processing.metrics import (  # noqa: E402
    compute_category_metrics,
    consolidate_metrics,
)


def test_compute_category_metrics():
    products = [{"price": 10.0}, {"price": 20.5}, {"price": 30.0}]
    assert compute_category_metrics("electronics", products) == {
        "category": "electronics",
        "average_price": 20.17,
        "min_price": 10.0,
        "max_price": 30.0,
        "product_count": 3,
    }


def test_average_rounds_half_up():
    # media exata 0.125 -> 0.13 (round() nativo daria 0.12)
    assert compute_category_metrics("x", [{"price": 0.1}, {"price": 0.15}])["average_price"] == 0.13


def test_result_is_json_serializable():
    json.dumps(compute_category_metrics("x", [{"price": 1.5}]))


def test_empty_category_raises():
    with pytest.raises(ValueError):
        compute_category_metrics("x", [])


@pytest.mark.parametrize("bad", [{}, {"price": None}, {"price": "abc"}])
def test_invalid_price_raises(bad):
    with pytest.raises(ValueError):
        compute_category_metrics("x", [bad])


def test_consolidate_sorts_by_category():
    results = [{"category": "b"}, {"category": "a"}]
    assert [r["category"] for r in consolidate_metrics(results)] == ["a", "b"]


def test_consolidate_rejects_duplicates():
    with pytest.raises(ValueError):
        consolidate_metrics([{"category": "a"}, {"category": "a"}])
