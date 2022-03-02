from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def func(**context):
    print("here!!!")
    print(context["execution_date"].strftime("%Y-%m-%d"))
    print(type(context["execution_date"]))

with DAG(
    dag_id="test_param",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["test"]) as dag:

    task = PythonOperator(task_id="test_execution_date",
                        python_callable=func,
                        provide_context=True,
                        dag=dag)