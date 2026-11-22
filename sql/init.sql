-- Executado pelo entrypoint do Postgres apenas na primeira inicializacao do volume.
-- Cria o banco de dados da aplicacao, separado do banco de metadados do Airflow.

SELECT 'CREATE DATABASE shopbrasil'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'shopbrasil')\gexec

\connect shopbrasil

-- Snapshot atual: uma linha por categoria e data.
CREATE TABLE IF NOT EXISTS category_metrics (
    category       VARCHAR(100)  NOT NULL,
    average_price  NUMERIC(12,2) NOT NULL,
    min_price      NUMERIC(12,2) NOT NULL,
    max_price      NUMERIC(12,2) NOT NULL,
    product_count  INTEGER       NOT NULL CHECK (product_count >= 0),
    snapshot_date  DATE          NOT NULL,
    updated_at     TIMESTAMP     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_category_metrics UNIQUE (category, snapshot_date)
);

-- Historico: uma linha por execucao da DAG e categoria.
CREATE TABLE IF NOT EXISTS category_metrics_history (
    id             SERIAL        PRIMARY KEY,
    dag_run_id     VARCHAR(250)  NOT NULL,
    category       VARCHAR(100)  NOT NULL,
    average_price  NUMERIC(12,2) NOT NULL,
    min_price      NUMERIC(12,2) NOT NULL,
    max_price      NUMERIC(12,2) NOT NULL,
    product_count  INTEGER       NOT NULL CHECK (product_count >= 0),
    execution_date TIMESTAMP     NOT NULL,
    CONSTRAINT uq_category_metrics_history UNIQUE (dag_run_id, category)
);
