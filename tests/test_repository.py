import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.database import postgres_repository as repo  # noqa: E402

ROW = {
    "category": "electronics",
    "average_price": 20.17,
    "min_price": 10.0,
    "max_price": 30.0,
    "product_count": 3,
}


@pytest.fixture
def conn():
    connection = MagicMock()
    with patch.object(repo, "PostgresHook") as hook:
        hook.return_value.get_conn.return_value = connection
        yield connection


def test_save_metrics_writes_both_tables_and_commits(conn):
    cur = conn.cursor.return_value.__enter__.return_value
    run_date = datetime(2025, 1, 2, 3, 0, tzinfo=timezone(timedelta(hours=-3)))

    n = repo.save_metrics([ROW], "run_1", run_date, date(2025, 1, 2))

    assert n == 1
    (snap_sql, snap_params), (hist_sql, hist_params) = [c.args for c in cur.executemany.call_args_list]
    assert "ON CONFLICT (category, snapshot_date)" in snap_sql
    assert "ON CONFLICT (dag_run_id, category)" in hist_sql
    assert snap_params[0]["snapshot_date"] == date(2025, 1, 2)
    assert hist_params[0]["dag_run_id"] == "run_1"
    assert hist_params[0]["execution_date"] == datetime(2025, 1, 2, 6, 0)  # UTC, sem tzinfo
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_rollback_and_close_on_error(conn):
    conn.cursor.return_value.__enter__.return_value.executemany.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        repo.save_metrics([ROW], "run_1", datetime(2025, 1, 2), date(2025, 1, 2))

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_empty_rows_raises(conn):
    with pytest.raises(ValueError):
        repo.save_metrics([], "run_1", datetime(2025, 1, 2), date(2025, 1, 2))


def test_missing_field_raises(conn):
    bad = {k: v for k, v in ROW.items() if k != "min_price"}
    with pytest.raises(ValueError):
        repo.save_metrics([bad], "run_1", datetime(2025, 1, 2), date(2025, 1, 2))
    conn.commit.assert_not_called()
