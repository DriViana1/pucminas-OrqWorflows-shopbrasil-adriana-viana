"""Cliente HTTP para a Fake Store API (https://fakestoreapi.com)."""
import os
from typing import Optional
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_BASE_URL = "https://fakestoreapi.com"
DEFAULT_TIMEOUT = 10  # segundos


def _build_session() -> requests.Session:
    """Sessao com retry e backoff para erros transitorios (429 e 5xx)."""
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


class FakeStoreClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        session: Optional[requests.Session] = None,
    ):
        self.base_url = (
            base_url or os.getenv("FAKESTORE_BASE_URL", DEFAULT_BASE_URL)
        ).rstrip("/")
        self.timeout = timeout
        self.session = session or _build_session()

    def _get_list(self, path: str) -> list:
        response = self.session.get(f"{self.base_url}{path}", timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(
                f"Resposta inesperada de {path}: esperado list, recebido {type(data).__name__}"
            )
        return data

    def get_products(self, category: Optional[str] = None) -> list[dict]:
        """Retorna todos os produtos, ou apenas os de uma categoria."""
        if category is None:
            return self._get_list("/products")
        return self._get_list(f"/products/category/{quote(category, safe='')}")

    def get_categories(self) -> list[str]:
        """Retorna os nomes das categorias."""
        return self._get_list("/products/categories")
