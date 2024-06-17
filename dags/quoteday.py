import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

import msg2teams

with DAG(
    dag_id='send_quote_to_msteams',
    start_date=pendulum.datetime(2024, 6, 17),
    schedule='0 11 * * *',
    tags=['daily_quote_sender']
) as dag:
    start_op = PythonOperator(
        task_id='send_daily_quote',
        python_callable=msg2teams.daily_quote
    )
