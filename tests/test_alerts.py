import logging
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.callbacks.alerts import on_failure_alert  # noqa: E402


def test_logs_failure_details(caplog):
    ti = SimpleNamespace(dag_id="ecommerce_pipeline", task_id="calcular", map_index=2, try_number=3)
    context = {
        "task_instance": ti,
        "run_id": "manual__1",
        "logical_date": "2025-01-02",
        "exception": ValueError("boom"),
    }

    with caplog.at_level(logging.ERROR):
        on_failure_alert(context)

    msg = caplog.text
    for expected in ("ecommerce_pipeline", "calcular", "map_index=2", "manual__1", "boom"):
        assert expected in msg


def test_never_raises_on_empty_context(caplog):
    with caplog.at_level(logging.ERROR):
        on_failure_alert({})
    assert "Falha na task" in caplog.text


def test_retry_and_success_alerts_log_details(caplog):
    from include.callbacks.alerts import on_retry_alert, on_success_alert

    ti = SimpleNamespace(dag_id="ecommerce_pipeline", task_id="buscar_produtos", map_index=-1, try_number=2)
    context = {"task_instance": ti, "run_id": "manual__1", "exception": RuntimeError("api off")}

    with caplog.at_level(logging.INFO):
        on_retry_alert(context)
        on_success_alert(context)

    assert "Retry da task" in caplog.text and "api off" in caplog.text
    assert "Sucesso na task" in caplog.text and "buscar_produtos" in caplog.text


def test_retry_and_success_alerts_never_raise_on_empty_context():
    from include.callbacks.alerts import on_retry_alert, on_success_alert

    on_retry_alert({})
    on_success_alert({})
