from airflow.utils.task_group import TaskGroup
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow import DAG
from datetime import datetime
import pendulum


with DAG(
        dag_id=f'shr_dag',
        default_args={
            'owner': 'shr',
            'weight_rule': WeightRule.ABSOLUTE,
        },
        start_date=datetime(2024, 9, 20, 19, 0, 0, tzinfo=pendulum.timezone('Asia/Seoul')),
        end_date=None,
        schedule_interval='55 * * * *',
        tags=['test'],
        catchup=False,
) as dag:
    start_wf = DummyOperator(task_id='start_wf')
    end_wf = DummyOperator(task_id='end_wf')

    with TaskGroup(f"middle_wf") as middle_wf_group:

        middle_wf = DummyOperator(task_id='middle_wf')

        middle_wf

    start_wf >> middle_wf_group

    middle_wf_group >> end_wf