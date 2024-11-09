import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from SCHEDULE.lib.util import TrinoOperator


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

DEFAULT_POOL = 'press'

# DAG 정의
with DAG(
        'press_pipeline',
        default_args=default_args,
        description='A Titanic data preprocessing pipeline',
        schedule_interval='0 1 * * *',  # 매일 실행
        tags=['press'],
        catchup=False,
) as dag:
    # 각 단계 정의
    press_count = TrinoOperator(
        task_id='press_extract',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        SELECT COUNT(*) 
        FROM OPERATION_MYSQL.PRESS.PRESS_RAW_DATA
        """,
        do_xcom_push=True,
    )

    # 작업 순서 정의
    press_count
