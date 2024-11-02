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
from SCHEDULE.lib.util import TrinoOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

# config
#####################
# section
section = {
    "100" : "정치",
    "101" : "경제",
    "102" : "사회",
    "103" : "생활/문화",
    "105" : "IT/과학",
    "104" : "세계"
}


#####################

with DAG(
        dag_id=f'social.news.naver',
        default_args={
            'owner': 'och',
            'weight_rule': WeightRule.ABSOLUTE,
        },
        start_date=datetime(2024, 10, 7, 0, 0, 0, tzinfo=Timezone('Asia/Seoul')),
        end_date=None,
        schedule_interval='* * * * *',
        tags=['social','naver','news'],
        catchup=False,
) as dag:
    # 시작
    start_wf = DummyOperator(task_id='start_wf')



    #
    middle_wf_group = DummyOperator(task_id='middle_wf')



    # 끝
    end_wf = DummyOperator(task_id='end_wf')




    start_wf >> middle_wf_group

    middle_wf_group >> end_wf