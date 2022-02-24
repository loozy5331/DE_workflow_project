from datetime import datetime, timedelta

import requests
import pandas as pd    
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

# from ..plugins import calculator
import calculator

def get_connector():
    hook = PostgresHook(postgres_conn_id="postgres_DB")
    return hook.get_conn()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def service_stock_trigger_fn(**context):
    """
        (*임시) stock 데이터를 수집할 수 있는 트리거 함수

        추후에는 실시간으로 수집되는 정보를 활용할 것이기 때문에 제거되어야 함.
    """
    TICKERS = ["SPY", "QQQ"]

    url = context["params"]["url"]
    res = requests.post(url, json={"tickers": TICKERS})

    return None

def copy_stock_table(**context):
    """
        원본 테이블을 복사하여 더비 테이블 생성
    """
    conn = get_connector()
    cur = conn.cursor()

    # copy temporary table
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS dummy.stock;
        CREATE TABLE dummy.stock 
        AS SELECT * FROM service.stock;
    """
    cur.execute(sql)
    cur.execute("END;")

def summary_stock_index(**context):
    """
        주가 지표를 계산하여 요약 테이블에 추가
    """
    SUMMARY_TICKERS = ["SPY"]

    conn = get_connector()
    cursor = conn.cursor()

    for s_ticker in SUMMARY_TICKERS:
        # extract dataset from ticker
        sql = f"""
            SELECT ts, close FROM dummy.stock
            WHERE ticker = '{s_ticker}'
        """
        cursor.execute(sql)

        dates = []
        prices = []
        for ts, price in cursor.fetchall():
            dates.append(ts)
            prices.append(price)

        # calculate stock index
        df = pd.DataFrame(data=prices, index=dates, columns=["close"])
        df["mv200"] = calculator.get_moving_average(df["close"], window=200)
        df["mdd"] = calculator.get_moving_average(df["close"], window=52*5) # 52주
        
        # create summary table
        sql = f"""
            BEGIN;
            DROP TABLE IF EXISTS summary.stock;
            CREATE TABLE summary.stock (
                ts timestamp,
                close FLOAT,
                mv200 FLOAT,
                mdd FLOAT
            );
        """
        for ts, row in df.iterrows():
            sql += f"""
                INSERT INTO summary.stock (ts, close, mv200, mdd)
                VALUES ('{ts.strftime("%Y-%m-%d %H:%M:%S")}', {row.close}, {row.mv200}, {row.mdd});
            """
        cursor.execute(sql)
        cursor.execute("END;")


with DAG(
    dag_id="stock_etl",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["stock"]) as dag:
    
    collect_stock_task = PythonOperator(task_id="collect_stock_price",
                                        python_callable=service_stock_trigger_fn,
                                        params= {
                                            "url":Variable.get("service_stock_url")
                                            },
                                        provide_context=True,
                                        dag=dag)

    dummy_table_task = PythonOperator(task_id="copy_service_table",
                                        python_callable=copy_stock_table,
                                        dag=dag)

    summary_stock_task = PythonOperator(task_id="summary_stock_task",
                                        python_callable=summary_stock_index,
                                        dag=dag)

    collect_stock_task >> dummy_table_task >> summary_stock_task
    


