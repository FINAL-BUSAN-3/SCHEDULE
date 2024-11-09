import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from SCHEDULE.lib.util import TrinoOperator, SlackOperator, TrinoReturnOperator


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
with (DAG(
        'press_pipeline',
        default_args=default_args,
        description='press_pipeline',
        schedule_interval='0 1 * * *',  # 매일 실행
        tags=['press'],
        catchup=False,
) as dag):

    # 1. 각 단계 정의
    press_count = TrinoReturnOperator(
        task_id='press_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        SELECT COUNT(*) 
        FROM OPERATION_MYSQL.PRESS.PRESS_RAW_DATA
        """,
        do_xcom_push=True,
    )

    # 2. press 배치 시작 알림 전송
    press_batch_start_alert = SlackOperator(
        task_id='press_batch_start_alert',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message="""
        [Press Batch]
        Press count : {{task_instance.xcom_pull(task_ids='press_count', key='return_value')}}
        Press Batch 를 시작 합니다.
        Time : {{ts}}
        """
    )

    # 3. press stg 초기화
    press_stg_drop = TrinoOperator(
        task_id='press_stg_drop',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
                DROP TABLE DW_HIVE.STG.PREE_RAW_DATA
                """,
        do_xcom_push=True,
    )

    # 4. press stg 초기화 알람 전송
    press_stg_drop_alarm = SlackOperator(
        task_id='press_stg_drop_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message="""
            [Press Batch]
            Staging 영역을 초기화 했습니다.
            Table Name : DW_HIVE.STG.PREE_RAW_DATA
            Time : {{ts}}
            """
    )

    # 5. press_stg 적재
    press_stg = TrinoOperator(
        task_id='press_stg',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            CREATE TABLE DW_HIVE.STG.PREE_RAW_DATA AS
            SELECT * 
            FROM OPERATION_MYSQL.PRESS.PRESS_RAW_DATA
            """,
        do_xcom_push=True,
    )

    # 6. press stg 검증
    press_stg_count = TrinoReturnOperator(
        task_id='press_stg_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            SELECT COUNT(*) 
            FROM DW_HIVE.STG.PREE_RAW_DATA
            """,
        do_xcom_push=True,
    )

    # 7. press stg 적재 완료 알람 전송
    press_stg_alarm = SlackOperator(
        task_id='press_stg_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message="""
                [Press Batch]
                Staging 완료 했습니다.
                count : {{task_instance.xcom_pull(task_ids='press_stg_count', key='return_value')}}
                Time : {{ts}}
                """
    )



    # 작업 순서 정의
    press_count >> press_batch_start_alert >> press_stg_drop >> press_stg_drop_alarm >> press_stg >>
    press_stg_count >> press_stg_alarm
