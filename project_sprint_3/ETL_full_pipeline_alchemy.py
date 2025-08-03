"""
────────────────────────────── DAG: ETL_full_pipeline_alchemy ───────────────────────────────

Этап 1:
    ▪ Получение данных по API (/generate_report, /get_report).
    ▪ Загрузка CSV → stage.user_order_log и других stage-таблиц.
    ▪ Учет статусов shipped/refunded.
    ▪ Пересчет транзакционной витрины mart.f_sales (отрицательные суммы для refunded).

Этап 2:
    ▪ Создание витрины mart.f_customer_retention (weekly):
        – new, returning, refunded клиенты.
        – Доходы от каждой группы.

Этап 3:
    ▪ Идемпотентность:
        – Stage пересоздается на каждую партию (pandas.to_sql(if_exists="replace")).
        – Mart.f_sales и mart.f_customer_retention чистятся за пересчитываемые даты/недели.
        – Повторный запуск DAG не создает дубликатов.

Используется SQLAlchemy для всех операций с БД:
    – PostgresHook.get_sqlalchemy_engine()
    – pandas.to_sql для stage
    – engine.begin() + text() для DML и DDL
"""

import datetime as dt
import time
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# ─────────────────────────────── 1. Константы ───────────────────────────────
PG_CONN_ID = "pg_connection"                    # Airflow connection к Postgres
LOCAL_DIR = Path("/opt/airflow/data")
LOCAL_DIR.mkdir(exist_ok=True, parents=True)

API_HOST  = "d5dg1j9kt695d30blp03.apigw.yandexcloud.net"
API_TOKEN = "5f55e6c0-e9e5-4a9c-b313-63c01fc31460"

NICKNAME  = "Ajdar"
COHORT    = "40"

HEADERS = {
    "X-API-KEY": API_TOKEN,
    "X-Nickname": NICKNAME,
    "X-Cohort": COHORT,
    "X-Project": "True",
}

# ─────────────────────────────── 2. DDL ───────────────────────────────
DDL_STAGE = """
CREATE SCHEMA IF NOT EXISTS stage;

CREATE TABLE IF NOT EXISTS stage.user_order_log (
    order_id        BIGINT,
    date_time       TIMESTAMP,
    item_id         INT,
    customer_id     INT,
    quantity        INT,
    payment_amount  NUMERIC,
    status          VARCHAR(10) DEFAULT 'shipped'
);

CREATE TABLE IF NOT EXISTS stage.user_activity_log (
    action_id    INT,
    date_time    TIMESTAMP,
    quantity     INT
);

CREATE TABLE IF NOT EXISTS stage.customer_research (
    date_id     INT,
    customer_id INT,
    item_id     INT,
    quantity    INT
);
"""

DDL_MART = """
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.f_sales (
    order_id        BIGINT,
    order_status    VARCHAR(10),
    order_date      DATE,
    item_id         INT,
    customer_id     INT,
    price           NUMERIC,
    quantity        INT,
    payment_amount  NUMERIC,
    sign            SMALLINT,
    PRIMARY KEY (order_id, order_status)
);

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    period_name                  TEXT,
    period_id                    DATE,
    item_id                      INT,
    new_customers_count          INT,
    returning_customers_count    INT,
    refunded_customer_count      INT,
    new_customers_revenue        NUMERIC,
    returning_customers_revenue  NUMERIC,
    customers_refunded           NUMERIC,
    PRIMARY KEY (period_id, item_id)
);
"""

# ─────────────────────────────── 3. Утилиты SQLAlchemy ───────────────────────────────
def get_engine():
    """Возвращает SQLAlchemy Engine для подключения к Postgres."""
    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_sqlalchemy_engine()

def exec_sql(sql: str):
    """Выполняет многострочный SQL в транзакции через SQLAlchemy."""
    engine = get_engine()
    with engine.begin() as conn:
        for stmt in sql.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))

# ─────────────────────────────── 4. Таски ───────────────────────────────

# ───── Этап 1: API
def create_report(**context):
    """
    POST /generate_report — запускаем формирование отчета.
    Сохраняем task_id в XCom.
    """
    r = requests.post(f"https://{API_HOST}/generate_report", headers=HEADERS, timeout=10)
    r.raise_for_status()
    context["ti"].xcom_push(key="task_id", value=r.json()["task_id"])

def wait_report(**context):
    """
    GET /get_report — ждем пока статус SUCCESS.
    Сохраняем report_id для скачивания файлов.
    """
    task_id = context["ti"].xcom_pull(key="task_id")
    for _ in range(10):
        resp = requests.get(
            f"https://{API_HOST}/get_report",
            headers=HEADERS,
            params={"task_id": task_id},
            timeout=10,
        ).json()
        if resp["status"] == "SUCCESS":
            context["ti"].xcom_push(key="report_id", value=resp["data"]["report_id"])
            return
        time.sleep(30)
    raise TimeoutError("Отчет не готов после 10 попыток")

# ───── Этап 1: Загрузка в stage
def load_stage(**context):
    """
    Скачиваем CSV и грузим в stage.*.
    ▪ Если нет колонки status (старый формат) — добавляем 'shipped'.
    ▪ Используем pandas.to_sql(if_exists='replace'), чтобы пересоздать stage
      на каждую партию (идемпотентность Этапа 3).
    """
    report_id = context["ti"].xcom_pull(key="report_id")
    base = (
        f"https://storage.yandexcloud.net/s3-sprint3/"
        f"cohort_{COHORT}/{NICKNAME}/project/{report_id}/{{}}"
    )
    files = {
        "user_order_log.csv": "user_order_log",
        "user_activity_log.csv": "user_activity_log",
        "customer_research.csv": "customer_research",
    }

    engine = get_engine()

    for fname, table in files.items():
        df = pd.read_csv(base.format(fname))
        if table == "user_order_log" and "status" not in df.columns:
            df["status"] = "shipped"

        df.to_csv(LOCAL_DIR / fname, index=False)  # сохраняем копию

        df.to_sql(
            name=table,
            con=engine,
            schema="stage",
            if_exists="replace",
            index=False,
            method="multi"
        )

# ───── Этап 1: mart.f_sales
def refresh_f_sales(**_):
    """
    Пересчитываем транзакционную витрину mart.f_sales:
    ▪ shipped → +, refunded → - (через поле sign).
    ▪ Перед вставкой удаляем только даты из stage.user_order_log (идемпотентность Этап 3).
    """
    sql = """
          DELETE FROM mart.f_sales
           WHERE order_date IN (SELECT DISTINCT date_time::date FROM stage.user_order_log);      
          INSERT INTO mart.f_sales
            (order_id, order_status, order_date,
             item_id, customer_id, price, quantity, payment_amount, sign)
          -- Вообще по хорошему нужна специальная таблица мапер, но для магазина с колличеством заказов до
          -- 1kk сойдёт. При 100kk заказов риск коллизии p ≈ (10^16) / (3.7×10^19) ≈ 0.00027 (~1 к 3.7 тыс.).
          -- Думаю можно жить если мы не Амазон.
          SELECT DISTINCT ON (uniq_id, status)
                 ('x' || substr(md5(uniq_id), 1, 16))::bit(64)::bigint AS order_id,
                 status,
                 date_time::date,
                 item_id,
                 customer_id,
                 payment_amount/NULLIF(quantity,0),
                 quantity,
                 payment_amount * CASE WHEN status='refunded' THEN -1 ELSE 1 END,
                 CASE WHEN status='refunded' THEN -1 ELSE 1 END
          FROM stage.user_order_log
          ORDER BY uniq_id, status, date_time DESC
          ON CONFLICT (order_id, order_status) DO UPDATE
          SET order_date      = EXCLUDED.order_date,
              item_id         = EXCLUDED.item_id,
              customer_id     = EXCLUDED.customer_id,
              price           = EXCLUDED.price,
              quantity        = EXCLUDED.quantity,
              payment_amount  = EXCLUDED.payment_amount,
              sign            = EXCLUDED.sign;
          """
    exec_sql(sql)

# ───── Этап 2: mart.f_customer_retention
def refresh_f_retention(**_):
    """
    Строим weekly customer retention:
    ▪ new, returning, refunded клиенты.
    ▪ Доходы для каждой группы.
    ▪ Удаляем только недели из инкремента (идемпотентность Этап 3).
    """
    sql = """
          WITH sales AS (
              SELECT order_date, item_id, customer_id, payment_amount, order_status
              FROM mart.f_sales
          )
          , by_week AS (
              SELECT date_trunc('week', order_date)::date AS week_start,
                     item_id,
                     customer_id,
                     SUM(CASE WHEN order_status='shipped'  THEN 1 END) AS shipped_cnt,
                     SUM(CASE WHEN order_status='refunded' THEN 1 END) AS refunded_cnt,
                     SUM(payment_amount) AS gm
              FROM sales
              GROUP BY 1,2,3
          )
          , flags AS (
              SELECT bw.*,
                     EXISTS (SELECT 1 FROM sales s2
                             WHERE s2.customer_id=bw.customer_id
                               AND s2.order_status='shipped'
                               AND s2.order_date < bw.week_start) AS has_history
              FROM by_week bw
          )
          , agg AS (
              SELECT week_start AS period_id,
                     item_id,
                     COUNT(*) FILTER (WHERE shipped_cnt=1 AND NOT has_history)         AS new_customers_count,
                     COUNT(*) FILTER (WHERE shipped_cnt>1 OR has_history)             AS returning_customers_count,
                     COUNT(*) FILTER (WHERE refunded_cnt>0)                           AS refunded_customer_count,
                     SUM(gm) FILTER (WHERE shipped_cnt=1 AND NOT has_history)         AS new_customers_revenue,
                     SUM(gm) FILTER (WHERE shipped_cnt>1 OR has_history)              AS returning_customers_revenue,
                     SUM(ABS(gm)) FILTER (WHERE refunded_cnt>0)                       AS customers_refunded
              FROM flags
              GROUP BY period_id,item_id
          )
          , deleted AS (
                DELETE FROM mart.f_customer_retention
                WHERE period_id IN (SELECT DISTINCT period_id FROM agg)
          )
          INSERT INTO mart.f_customer_retention
                (period_name, period_id, item_id,
                 new_customers_count, returning_customers_count, refunded_customer_count,
                 new_customers_revenue, returning_customers_revenue, customers_refunded)
          SELECT 'weekly', *
          FROM agg;
          """
    exec_sql(sql)

# ─────────────────────────────── 5. DAG ───────────────────────────────
with DAG(
    dag_id="ETL_full_pipeline_alchemy",
    start_date=dt.datetime(2021, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=dt.timedelta(hours=3),
) as dag:

    # Этап 3: init_db (идемпотентное создание схем и таблиц)
    with TaskGroup("init_db") as init_db:
        ddl_stage = PostgresOperator(
            task_id="ddl_stage",
            postgres_conn_id=PG_CONN_ID,
            sql=DDL_STAGE,
        )
        ddl_mart = PostgresOperator(
            task_id="ddl_mart",
            postgres_conn_id=PG_CONN_ID,
            sql=DDL_MART,
        )

    # Этап 1: API → Stage → mart.f_sales
    t_create = PythonOperator(task_id="create_report", python_callable=create_report)
    t_wait   = PythonOperator(task_id="wait_report",   python_callable=wait_report)
    t_load   = PythonOperator(task_id="load_stage",    python_callable=load_stage)
    t_f_sales   = PythonOperator(task_id="refresh_f_sales",     python_callable=refresh_f_sales)

    # Этап 2: mart.f_customer_retention
    t_retention = PythonOperator(task_id="refresh_f_retention", python_callable=refresh_f_retention)

    # Цепочка задач
    init_db >> t_create >> t_wait >> t_load >> t_f_sales >> t_retention
