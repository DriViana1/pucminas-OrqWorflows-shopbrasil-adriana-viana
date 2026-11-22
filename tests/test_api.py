import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.api.fakestore_client import FakeStoreClient  # noqa: E402


def _client(payload=None, status_error=None):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.side_effect = status_error
    session = MagicMock()
    session.get.return_value = response
    return FakeStoreClient(base_url="https://api.test/", session=session), session


def test_get_products_all():
    client, session = _client([{"id": 1, "price": 10.0}])
    assert client.get_products() == [{"id": 1, "price": 10.0}]
    session.get.assert_called_once_with("https://api.test/products", timeout=client.timeout)


def test_get_products_by_category_encodes_path():
    client, session = _client([])
    client.get_products("men's clothing")
    url = session.get.call_args.args[0]
    assert url == "https://api.test/products/category/men%27s%20clothing"


def test_get_categories():
    client, _ = _client(["electronics", "jewelery"])
    assert client.get_categories() == ["electronics", "jewelery"]


def test_http_error_is_raised():
    client, _ = _client(status_error=requests.HTTPError("500"))
    with pytest.raises(requests.HTTPError):
        client.get_categories()


def test_non_list_response_raises_value_error():
    client, _ = _client({"message": "oops"})
    with pytest.raises(ValueError):
        client.get_products()
