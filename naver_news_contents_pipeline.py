import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import subprocess
from SCHEDULE.lib.util import TrinoOperator, SlackOperator, TrinoReturnOperator
from textwrap import dedent
import random

# 기본 인자 설정
default_args = {
    'owner': 'bigdata_busan_3',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def python_ssh(sh):
    return subprocess.check_output(sh, shell=True)

def naver_news_contents_crawling_handler(**context):
    time.sleep(random.randint(100, 150))
    return random.randint(10, 20)

def handler(**context):
    time.sleep(random.randint(30, 40))
    return random.randint(10, 20)

DEFAULT_POOL = 'contents'

# DAG 정의
with (DAG(
        'naver_news_contents_pipeline',
        default_args=default_args,
        description='naver_news_contents_pipeline',
        schedule_interval='0 17 * * *',  # 매일 실행
        tags=['naver', 'news', 'contents', 'social'],
        catchup=False,
) as dag):


    # 1. 소셜 배치 시작 알림 전송
    naver_news_contents_alaram = SlackOperator(
        task_id='naver_news_contents_alaram',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Naver News Batch]
        소셜 데이터(Naver News Contents) Batch 를 시작 합니다.
        Time : {{ ts }}
        """)
    )

    # 2. 소셜 배치 수집
    naver_news_contents_crawling = PythonOperator(
        task_id='naver_news_contents_crawling',
        pool=DEFAULT_POOL,
        priority_weight=1,
        python_callable=naver_news_contents_crawling_handler,
        do_xcom_push=True
    )

    # 3. 크롤링 완료
    crawling_done_alarm = SlackOperator(
        task_id='crawling_done_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
            [Naver News Contents Batch]
            소셜 데이터(Naver News Contents) crawling 을 완료 했습니다.
            수집된 기사 수 : {{ ti.xcom_pull(task_ids='naver_news_contents_crawling', key='return_value') }}
            Time : {{ ts }}
            """)
    )

    # 4. stg 초기화 알람 전송
    contents_stg_drop_alarm = SlackOperator(
        task_id='contents_stg_drop_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
            [Naver News Contents Batch]
            Staging 영역을 초기화 했습니다.
            Table Name : DW_HIVE.STG.STG_NAVER_NEWS
            Time : {{ ts }}
            """)
    )

    # 5. stg 적재
    contents_stg = PythonOperator(
        task_id='contents_stg',
        pool=DEFAULT_POOL,
        priority_weight=1,
        python_callable=handler
    )

    # 6. press stg 검증
    contents_stg_count = PythonOperator(
        task_id='contents_stg_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        python_callable=handler
    )

    # 7. stg 적재 완료 알람 전송
    contents_stg_alarm = SlackOperator(
        task_id='contents_stg_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
            [Naver News Contents Batch]
            Staging 완료 했습니다.
            count : {{ ti.xcom_pull(task_ids='naver_news_contents_crawling', key='return_value') }}
            Time : {{ ts }}
            """)
    )

    # 8. ods 파티션 삭제
    contents_ods_drop_partition = PythonOperator(
        task_id='contents_ods_drop_partition',
        pool=DEFAULT_POOL,
        priority_weight=1,
        python_callable=handler
    )

    # 9. press ods
    contents_ods = PythonOperator(
        task_id='contents_ods',
        pool=DEFAULT_POOL,
        priority_weight=1,
        python_callable=handler
    )

    # 10. press ods count
    contents_ods_count = PythonOperator(
        task_id='contents_ods_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        python_callable=handler
    )

    # 11 ods 적재 알람
    contents_ods_alarm = SlackOperator(
        task_id='contents_ods_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Naver News Contents Batch]
        ODS 완료 했습니다.
        count : {{ ti.xcom_pull(task_ids='naver_news_contents_crawling', key='return_value') }}
        Time : {{ ts }}
        """)
    )

    naver_news_contents_alaram >> naver_news_contents_crawling >> crawling_done_alarm >> contents_stg_drop_alarm
    contents_stg_drop_alarm >> contents_stg >> contents_stg_count >> contents_stg_alarm >> contents_ods_drop_partition
    contents_ods_drop_partition >> contents_ods >> contents_ods_count >> contents_ods_alarm
