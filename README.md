# shopbrasil-pricing-pipeline

Pipeline de orquestração fim a fim para análise de preços, com Apache Airflow 3, Docker e PostgreSQL.
Ingere produtos da [Fake Store API](https://fakestoreapi.com/docs), calcula métricas de preço por categoria
com Dynamic Task Mapping e persiste o resultado de forma idempotente.

## Estrutura do projeto

```
shopbrasil-airflow/
├── dags/
│   └── dag_ecommerce_pipeline.py      # DAG ecommerce_pipeline
├── plugins/
│   └── operators/
│       └── validar_produtos.py        # ValidarProdutosOperator
├── include/
│   ├── api/fakestore_client.py        # cliente HTTP (retry, timeout)
│   ├── processing/metrics.py          # cálculo de métricas (funções puras)
│   ├── database/postgres_repository.py# upsert idempotente via PostgresHook
│   └── callbacks/alerts.py            # callbacks de falha, retry e sucesso (log)
├── sql/
│   └── init.sql                       # cria o banco shopbrasil e as tabelas
├── config/
│   └── airflow.cfg
├── logs/
├── tests/
│   ├── test_api.py
│   ├── test_metrics.py
│   ├── test_repository.py
│   ├── test_validar_produtos.py
│   └── test_alerts.py
├── .env                               # segredos locais (ignorado pelo git)
├── .env.example
├── .gitignore
├── docker-compose.yaml
└── README.md
```

Ainda não criados: `docker/Dockerfile` e `requirements.txt`. Hoje nenhum pacote extra é necessário,
porque tudo que o projeto usa já vem na imagem `apache/airflow`.

## DAG `ecommerce_pipeline`

- **Agendamento:** diário às 06:00, fuso `America/Sao_Paulo` (`0 6 * * *`). `catchup=False`, `max_active_runs=1`.
- **API:** TaskFlow API com dois `TaskGroup`.
- **Retry:** 3 tentativas, backoff exponencial de 30 s até 5 min, definido no `default_args` e valendo para todas as tasks.
  O cliente HTTP também tenta de novo em 429 e 5xx.
- **Tratamento de erro:** `buscar_produtos` captura a exceção, registra no log e a relança com `raise`, o que aciona o retry do Airflow.
- **Callbacks** (`include/callbacks/alerts.py`, só registram em log, sem envio externo):
  - `on_failure_alert`: registra `dag_id`, `task_id`, `map_index`, `run_id`, data, tentativa e exceção.
    Está no `default_args` e vale para todas as tasks.
  - `on_retry_alert` e `on_success_alert`: configurados só em `buscar_produtos`, a task crítica.
    Ela também declara `on_failure_alert` de forma explícita.

### Topologia

```
ingestao
  └─ buscar_produtos                 GET /products
                │
analise         ▼
  ├─ extrair_categorias              agrupa os produtos por categoria, usa Xcom (usar com poucos dados)
  │             │ fan-out: .expand() por categoria
  │             ▼
  ├─ validar_produtos[categoria]     ValidarProdutosOperator
  ├─ calcular_metricas[categoria]    média, mínimo, máximo e contagem
  │             │ fan-in
  │             ▼
  └─ consolidar                      junta e ordena os resultados
                │
persistir       ▼                    snapshot + histórico, em uma transação
```

- **Linear:** ingestão → extração de categorias.
- **Fan-out:** `validar_produtos` e `calcular_metricas` rodam em paralelo, uma task por categoria (`.expand()`).
  Na interface, cada task mapeada aparece com o nome da categoria.
- **Fan-in:** `consolidar` reúne os resultados antes da persistência.
- **Pool:** as tasks mapeadas usam o pool `ecommerce_pool` (2 slots) para limitar a concorrência.
  O pool é obrigatório para a DAG executar (veja [Pool `ecommerce_pool`](#3-pool-ecommerce_pool)).

### Validação de produtos

O `ValidarProdutosOperator` roda antes do cálculo, dentro de cada categoria.

| Situação | Comportamento |
|---|---|
| Lista vazia | A task falha |
| Nenhum produto válido | A task falha, com os primeiros motivos na mensagem |
| Alguns produtos inválidos | Descarta e registra um `WARNING` no log |

Um produto é inválido se não tiver `id` ou `category`, ou se o `price` não for um número finito maior que zero.

### Idempotência

- `category_metrics`: upsert por `(category, snapshot_date)`. Reexecutar no mesmo dia atualiza a linha.
- `category_metrics_history`: upsert por `(dag_run_id, category)`. Reexecutar o mesmo run atualiza a linha.
- `snapshot_date` é calculado no fuso `America/Sao_Paulo`.

## Modelagem do banco de dados

Banco `shopbrasil`, separado do banco de metadados do Airflow (`airflow`). Ambos ficam no mesmo servidor Postgres.

### `category_metrics` (snapshot atual)

| Campo | Tipo |
|---|---|
| category | VARCHAR(100) |
| average_price | NUMERIC(12,2) |
| min_price | NUMERIC(12,2) |
| max_price | NUMERIC(12,2) |
| product_count | INTEGER |
| snapshot_date | DATE |
| updated_at | TIMESTAMP (padrão `NOW()`) |

Chave única: `(category, snapshot_date)`.

### `category_metrics_history` (histórico)

| Campo | Tipo |
|---|---|
| id | SERIAL (chave primária) |
| dag_run_id | VARCHAR(250) |
| category | VARCHAR(100) |
| average_price | NUMERIC(12,2) |
| min_price | NUMERIC(12,2) |
| max_price | NUMERIC(12,2) |
| product_count | INTEGER |
| execution_date | TIMESTAMP (UTC, sem fuso) |

Chave única: `(dag_run_id, category)`.

## Como executar

### 1. Configurar o `.env`

```bash
cp .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Cole a chave gerada em `FERNET_KEY`. O `.env.example` já traz `AIRFLOW_UID` e a Connection
`AIRFLOW_CONN_POSTGRES_SHOPBRASIL` (`postgresql://airflow:airflow@postgres:5432/shopbrasil`).
O `.env` não vai para o git.

### 2. Subir o ambiente

```bash
docker compose up airflow-init
docker compose up -d
```

- Interface: http://localhost:8080 (usuário `airflow`, senha `airflow`).
- O Postgres executa `sql/init.sql` só na primeira criação do volume. Se o volume já existia,
  rode o script manualmente ou recrie o volume com `docker compose down -v`. Esse comando apaga também
  os metadados do Airflow.

### 3. Pool `ecommerce_pool`

A DAG exige o pool `ecommerce_pool`. As tasks `validar_produtos` e `calcular_metricas` o declaram com `pool=...`. Se o pool não existir, o Airflow não consegue agendar essas tasks e elas ficam em `queued`, então a DAG não conclui.

O pool é criado automaticamente pelo serviço `airflow-init-pools` do `docker-compose.yaml`, que executa:

```bash
airflow pools set ecommerce_pool 2 "Pool para pipeline de ecommerce"
```

- O serviço roda a cada `docker compose up -d`, depois que o `airflow-init` termina com sucesso, e encerra
  com código 0. Não é preciso criar o pool manualmente.
- O comando é idempotente: cria o pool se não existir e atualiza os slots e a descrição se já existir.
  Para mudar a concorrência, altere o número de slots no compose e rode `docker compose up -d` de novo.
- O pool fica no banco de metadados. Se o volume for apagado (`docker compose down -v`), o próximo
  `docker compose up -d` o recria.

Para conferir:

```bash
docker compose logs airflow-init-pools
docker compose exec airflow-scheduler airflow pools list
```

### 4. Executar a DAG

A DAG nasce pausada. Para ativar o agendamento, use o toggle na interface. Para uma execução de teste, sem ativar o agendamento:

```bash
docker compose exec airflow-scheduler airflow dags test ecommerce_pipeline
```

### 5. Conferir o resultado

```bash
docker compose exec postgres psql -U airflow -d shopbrasil -c '\dt'
docker compose exec postgres psql -U airflow -d shopbrasil -c 'SELECT * FROM category_metrics ORDER BY category'
docker compose exec postgres psql -U airflow -d shopbrasil
  
O prompt muda para shopbrasil=#. Dentro dele, os comandos úteis são:
  
  \dt                                  -- lista as tabelas
  \d category_metrics                  -- mostra a estrutura de uma tabela
  SELECT * FROM category_metrics;      -- consulta o snapshot
  SELECT * FROM category_metrics_history;
  \x                                   -- alterna a exibição em formato vertical
  \q   

```

## Testes

Os testes não precisam de rede nem de banco: a API e o Postgres são simulados. Para rodar sem instalar nada na máquina, use um container descartável com a imagem do Airflow:

```bash
docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -v "$PWD:/work:ro" -w /work --entrypoint bash \
  apache/airflow:3.4.0 -c "pip install -q pytest && python -m pytest tests -q -p no:cacheprovider"
```

## Configuração do Compose

- `include/` é montado em `/opt/airflow/include`, e `PYTHONPATH=/opt/airflow` permite `from include... import ...` nas DAGs.
- `plugins/` é montado em `/opt/airflow/plugins`, o que expõe `from operators.validar_produtos import ...`.
- `sql/init.sql` é montado em `/docker-entrypoint-initdb.d/init.sql` no serviço `postgres`.
- A Connection `postgres_shopbrasil` é definida pela variável de ambiente `AIRFLOW_CONN_POSTGRES_SHOPBRASIL`.
  Ela não aparece em Admin → Connections, mas o `PostgresHook` a encontra normalmente.
- As credenciais `airflow/airflow` servem para desenvolvimento local. Não use em produção.
