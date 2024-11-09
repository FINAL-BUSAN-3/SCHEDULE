import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from SCHEDULE.lib.util import TrinoOperator, SlackOperator, TrinoReturnOperator
from textwrap import dedent

src_table = "OPERATION_MYSQL.PRESS.PRESS_RAW_DATA"
stg_table = "DW_HIVE.STG.STG_PRESS_RAW_DATA"
ods_table = "DL_ICEBERG.ODS.DS_PRESS_RAW_DATA"

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
        FROM {src_table}
        """,
        do_xcom_push=True,
    )

    # 2. press 배치 시작 알림 전송
    press_batch_start_alert = SlackOperator(
        task_id='press_batch_start_alert',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Press Batch]
        Press count : {{ ti.xcom_pull(task_ids='press_count', key='return_value') }}
        Press Batch 를 시작 합니다.
        Time : {{ ts }}
        """)
    )

    # 3. press stg 초기화
    press_stg_drop = TrinoOperator(
        task_id='press_stg_drop',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
                DROP TABLE {stg_table}
                """,
        do_xcom_push=True,
    )

    # 4. press stg 초기화 알람 전송
    press_stg_drop_alarm = SlackOperator(
        task_id='press_stg_drop_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Press Batch]
        Staging 영역을 초기화 했습니다.
        Table Name : DW_HIVE.STG.STG_PRESS_RAW_DATA
        Time : {{ ts }}
        """)
    )

    # 5. press_stg 적재
    press_stg = TrinoOperator(
        task_id='press_stg',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            CREATE TABLE {stg_table} AS
            SELECT * 
            FROM {src_table}
            """
    )

    # 6. press stg 검증
    press_stg_count = TrinoReturnOperator(
        task_id='press_stg_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            SELECT COUNT(*) 
            FROM {stg_table}
            """,
        do_xcom_push=True,
    )

    # 7. press stg 적재 완료 알람 전송
    press_stg_alarm = SlackOperator(
        task_id='press_stg_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Press Batch]
        Staging 완료 했습니다.
        count : {{ ti.xcom_pull(task_ids='press_stg_count', key='return_value') }}
        Time : {{ ts }}
        """)
    )

    # 8. press ods 파티션 삭제
    press_ods_drop_partition = TrinoOperator(
        task_id='press_ods_drop_partition',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        DELETE FROM {ods_table}
        WHERE PART_DT = DATE_FORMAT(NOW(), '%Y%m%d')
        """
    )

    # 9. press ods
    press_ods = TrinoOperator(
        task_id='press_ods',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        INSERT INTO {ods_table}
        SELECT *,
               DATE_FORMAT(NOW(), '%Y%m%d') PART_DT
        FROM {stg_table}
        """
    )

    # 10. press ods count
    press_ods_count = TrinoReturnOperator(
        task_id='press_ods_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        SELECT COUNT(*) 
        FROM {ods_table}
        WHERE PART_DT = DATE_FORMAT(NOW(), '%Y%m%d')
        """,
        do_xcom_push=True,
    )

    # 11 ods 적재 알람
    press_ods_alarm = SlackOperator(
        task_id='press_ods_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
            [Press Batch]
            ODS 완료 했습니다.
            count : {{ ti.xcom_pull(task_ids='press_ods_count', key='return_value') }}
            Time : {{ ts }}
            """)
    )



    # 작업 순서 정의
    press_count >> press_batch_start_alert >> press_stg_drop >> press_stg_drop_alarm >> press_stg
    press_stg >> press_stg_count >> press_stg_alarm >> press_ods_drop_partition >> press_ods >> press_ods_count
    press_ods_count >> press_ods_alarm
