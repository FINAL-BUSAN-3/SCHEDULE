import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from SCHEDULE.lib.util import TrinoOperator, SlackOperator, TrinoReturnOperator
from textwrap import dedent

src_table = "OPERATION_MYSQL.WELDING.WELDING_RAW_DATA"
stg_table = "DW_HIVE.STG.STG_WELDING_RAW_DATA"
ods_table = "DL_ICEBERG.ODS.DS_WELDING_RAW_DATA"

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

DEFAULT_POOL = 'welding'

# DAG 정의
with (DAG(
        'welding_pipeline',
        default_args=default_args,
        description='welding_pipeline',
        schedule_interval='0 17 * * *',  # 매일 실행
        tags=['welding'],
        catchup=False,
) as dag):

    # 1. 각 단계 정의
    welding_count = TrinoReturnOperator(
        task_id='welding_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        SELECT COUNT(*) 
        FROM {src_table}
        """,
        do_xcom_push=True,
    )

    # 2. welding 배치 시작 알림 전송
    welding_batch_start_alert = SlackOperator(
        task_id='welding_batch_start_alert',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Welding Batch]
        Welding count : {{ ti.xcom_pull(task_ids='welding_count', key='return_value') }}
        Welding Batch 를 시작 합니다.
        Time : {{ ts }}
        """)
    )

    # 3. welding stg 초기화
    welding_stg_drop = TrinoOperator(
        task_id='welding_stg_drop',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
                DROP TABLE {stg_table}
                """,
        do_xcom_push=True,
    )

    # 4. welding stg 초기화 알람 전송
    welding_stg_drop_alarm = SlackOperator(
        task_id='welding_stg_drop_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Welding Batch]
        Staging 영역을 초기화 했습니다.
        Table Name : DW_HIVE.STG.STG_WELDING_RAW_DATA
        Time : {{ ts }}
        """)
    )

    # 5. welding_stg 적재
    welding_stg = TrinoOperator(
        task_id='welding_stg',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            CREATE TABLE {stg_table} AS
            SELECT * 
            FROM {src_table}
            """
    )

    # 6. welding stg 검증
    welding_stg_count = TrinoReturnOperator(
        task_id='welding_stg_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            SELECT COUNT(*) 
            FROM {stg_table}
            """,
        do_xcom_push=True,
    )

    # 7. welding stg 적재 완료 알람 전송
    welding_stg_alarm = SlackOperator(
        task_id='welding_stg_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Welding Batch]
        Staging 완료 했습니다.
        count : {{ ti.xcom_pull(task_ids='welding_stg_count', key='return_value') }}
        Time : {{ ts }}
        """)
    )

    # 8. press ods 파티션 삭제
    welding_ods_drop_partition = TrinoOperator(
        task_id='welding_ods_drop_partition',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        DELETE FROM {ods_table}
        WHERE PART_DT = DATE_FORMAT(NOW(), '%Y%m%d')
        """
    )

    # 9. welding ods
    welding_ods = TrinoOperator(
        task_id='welding_ods',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        INSERT INTO {ods_table}
        SELECT *,
               DATE_FORMAT(NOW(), '%Y%m%d') PART_DT
        FROM {stg_table}
        """
    )

    # 10. welding ods count
    welding_ods_count = TrinoReturnOperator(
        task_id='welding_ods_count',
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
    welding_ods_alarm = SlackOperator(
        task_id='welding_ods_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
            [Welding Batch]
            ODS 완료 했습니다.
            count : {{ ti.xcom_pull(task_ids='press_ods_count', key='return_value') }}
            Time : {{ ts }}
            """)
    )



    # 작업 순서 정의
    welding_count >> welding_batch_start_alert >> welding_stg_drop >> welding_stg_drop_alarm >> welding_stg
    welding_stg >> welding_stg_count >> welding_stg_alarm >> welding_ods_drop_partition >> welding_ods >> welding_ods_count
    welding_ods_count >> welding_ods_alarm
