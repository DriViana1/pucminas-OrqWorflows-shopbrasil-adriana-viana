"""Callbacks de alerta. Por enquanto apenas registram em log (sem envio externo)."""
import logging

log = logging.getLogger(__name__)

def _task_info(context: dict) -> tuple:
    ti = context.get("task_instance")
    return (
        getattr(ti, "dag_id", None),
        getattr(ti, "task_id", None),
        getattr(ti, "map_index", None),
        context.get("run_id"),
        getattr(ti, "try_number", None),
    )


def on_success_alert(context: dict) -> None:
    """Registra no log que a task terminou com sucesso."""
    try:
        log.info(
            "Sucesso na task | dag_id=%s task_id=%s map_index=%s run_id=%s tentativa=%s",
            *_task_info(context),
        )
    except Exception:  # noqa: BLE001
        log.exception("Erro ao registrar alerta de sucesso")


def on_retry_alert(context: dict) -> None:
    """Registra no log que a task falhou e sera retentada."""
    try:
        log.warning(
            "Retry da task | dag_id=%s task_id=%s map_index=%s run_id=%s tentativa=%s | erro=%r",
            *_task_info(context),
            context.get("exception"),
        )
    except Exception:  # noqa: BLE001
        log.exception("Erro ao registrar alerta de retry")


def on_failure_alert(context: dict) -> None:
    """Registra no log os dados de uma task que falhou.

    Uso: default_args={"on_failure_callback": on_failure_alert}
    Nunca levanta excecao, para nao mascarar a falha original.
    """
    try:
        ti = context.get("task_instance")
        log.error(
            "Falha na task | dag_id=%s task_id=%s map_index=%s run_id=%s "
            "logical_date=%s tentativa=%s | erro=%r",
            getattr(ti, "dag_id", None),
            getattr(ti, "task_id", None),
            getattr(ti, "map_index", None),
            context.get("run_id"),
            context.get("logical_date"),
            getattr(ti, "try_number", None),
            context.get("exception"),
        )
    except Exception:  # noqa: BLE001
        log.exception("Erro ao registrar alerta de falha")
