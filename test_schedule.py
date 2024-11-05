from datetime import datetime, timedelta, timezone
from pendulum.tz.timezone import Timezone
import subprocess
import textwrap

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


with DAG(
        dag_id=f'end_test_dag',
        default_args={
            'owner': 'och',
            'weight_rule': WeightRule.ABSOLUTE,
        },
        start_date=datetime(2024, 9, 20, 19, 0, 0, tzinfo=Timezone('Asia/Seoul')),
        end_date=None,
        schedule_interval='55 * * * *',
        tags=['test'],
        catchup=False,
) as dag:
    start_wf = DummyOperator(task_id='start_wf')
    end_wf = DummyOperator(task_id='end_wf')

    with TaskGroup(f"middle_wf") as middle_wf_group:

        middle_wf = DummyOperator(task_id='end_wf')

        middle_wf

    start_wf >> middle_wf_group

    middle_wf_group >> end_wf