from datetime import datetime, timedelta

import requests
import pandas as pd    
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

## plugin libs
# from ..plugins import calculator
import calculator
# from ..plugins import evnet_trigger
import event_trigger


## <default>
slack_alarm = event_trigger.SlackAlert(channel="#airflow_alarm")

def get_connector():
    hook = PostgresHook(postgres_conn_id="postgres_DB")
    return hook.get_conn()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": slack_alarm.slack_fail_alert,
}

## </default>
def service_stock_trigger_fn(**context):
    """
        (*임시) stock 데이터를 수집할 수 있는 트리거 함수

        추후에는 실시간으로 수집되는 정보를 활용할 것이기 때문에 제거되어야 함.
    """
    url = context["params"]["url"]
    end_date = context["execution_date"].strftime("%Y-%m-%d")
    
    res = requests.post(url, json={"end_date":end_date})

    return res.status_code
        

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

    cur.close()
    conn.close()

def summary_stock_index(**context):
    """
        주가 지표를 계산하여 요약 테이블에 추가
    """
    conn = get_connector()
    cur = conn.cursor()

    cur.execute("SELECT ticker FROM service.tb_ticker")
    tickers = list(map(lambda x:x[0], cur.fetchall()))

    # create summary temporary table
    sql = f"""
        DROP TABLE IF EXISTS summary.temp_stock;
        CREATE TABLE summary.temp_stock (
            ts timestamp,
            adj_close FLOAT,
            mv200 FLOAT,
            high_52w FLOAT,
            low_52w FLOAT,
            ticker VARCHAR
        );
    """
    cur.execute(sql)

    for s_ticker in tickers:
        # extract dataset from ticker
        sql = f"""
            SELECT ts, adj_close FROM dummy.stock
            WHERE ticker = '{s_ticker}'
            ORDER BY ts;
        """

        dates = []
        prices = []
        cur.execute(sql)
        for ts, price in cur.fetchall():
            dates.append(ts)
            prices.append(price)

        # calculate stock index
        df = pd.DataFrame(data=prices, index=dates, columns=["adj_close"])
        df = calculator.get_moving_average(df, window=200)
        df = calculator.get_low_n_high_52week(df)
        df = df.fillna(0) # 결측치는 0으로 처리
        
        # insert temp summary table
        sql = ""
        for ts, row in df.iterrows():
            sql += f"""
                INSERT INTO summary.temp_stock (ts, adj_close, mv200, high_52w, low_52w, ticker)
                VALUES ('{ts.strftime("%Y-%m-%d %H:%M:%S")}', {row.adj_close}, {row.mv200}, {row.high_52w}, {row.low_52w}, '{s_ticker}');
            """
        cur.execute(sql)

    # remove origin summary table
    # alter temp_table to origin_table
    sql = f"""
        DROP TABLE IF EXISTS summary.stock;
        ALTER TABLE summary.temp_stock RENAME TO stock;
        END;
    """
    cur.execute(sql)

    cur.close()
    conn.close()

"""
    0. collect_stock_task:  주가 수집 서비스를 동작시킴
        - 추후 실시간 주가 데이터로 바뀔 예정이므로 임시 코드
    1. dummy_table_task:    OLTP(DB) -> OLAP(DB)로 복사
        - 분석을 위해 사용될 OLAP로 복사
        - 현재는 full refresh를 통해 기존 테이블 제거 -> 생성 -> 복사 순으로 이어짐
    2. summary_stock_task:  OLAP(DB)를 바탕으로 요약 테이블 생성 후 저장
        - OLAP 데이터를 바탕으로 주가 지표들을 계산하여 저장한 요약 테이블 생성
"""
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
    


