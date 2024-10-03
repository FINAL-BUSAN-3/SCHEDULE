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
from .lib.util import TrinoOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


with DAG(
        dag_id=f'trino_baseline',
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

        check_data = TrinoOperator(
            task_id='check_data',
            query=f"""
                SELECT *
                FROM dl_iceberg.ods.test
            """
        )

        create_temp_data = TrinoOperator(
            task_id='create_temp_data',
            query=f"""
                CREATE TABLE dl_iceberg.ods.test2
                AS SELECT * FROM dl_iceberg.ods.test
            """
        )

        check_data >> create_temp_data


    start_wf >> middle_wf_group

    middle_wf_group >> end_wf