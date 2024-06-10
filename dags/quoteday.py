import random

import pendulum
from airflow import DAG
# from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.python import BranchPythonOperator
# from airflow.utils.trigger_rule import TriggerRule

import msg2teams

def _func():
    branching_paths = ['choice_0', 'choice_1']
    return random.choice(branching_paths)


with DAG(
    dag_id='daily_quote_DAG',
    start_date=pendulum.today(),
    schedule='0 0 * * *',
    tags=['daily_quote_try_1']
) as dag:
    start_op = PythonOperator(
        task_id='daily_quote',
        python_callable=msg2teams.daily_quote
    )
