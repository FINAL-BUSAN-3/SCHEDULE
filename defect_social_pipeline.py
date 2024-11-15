import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
from SCHEDULE.lib.util import TrinoOperator, SlackOperator, TrinoReturnOperator
from textwrap import dedent

wf_server = "ec2-100-24-7-128.compute-1.amazonaws.com"

src_table = "OPERATION_MYSQL.SOCIAL.CAR_DEFECTS"
stg_table = "DW_HIVE.STG.STG_CAR_DEFECTS"
ods_table = "DL_ICEBERG.ODS.DS_CAR_DEFECTS"

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

def defect_batch_handler(**context):
    python_ssh(f"""
    ssh -i /opt/airflow/busan.pem ubuntu@{wf_server} "python3 /home/ubuntu/social_defect_crawl.py"
    """)

DEFAULT_POOL = 'defect'

# DAG 정의
with (DAG(
        'defect_social_pipeline',
        default_args=default_args,
        description='defect_social_pipeline',
        schedule_interval='0 18 * * *',  # 매일 실행
        tags=['defect', 'social'],
        catchup=False,
) as dag):


    # 1. 소셜 배치 시작 알림 전송
    defect_batch_alarm = SlackOperator(
        task_id='defect_batch_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Social Defect Batch]
        소셜 데이터(Defect) Batch 를 시작 합니다.
        Time : {{ ts }}
        """)
    )

    # 2. 수집 시작 알람
    defect_batch_ingestion_alarm = SlackOperator(
        task_id='defect_batch_ingestion_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
                [Social Defect Batch]
                소셜 데이터(Defect) 수집을 시작 합니다.
                Time : {{ ts }}
                """)
    )

    # 3. 수집 시작
    defect_ingestion = PythonOperator(
        task_id='defect_ingestion',
        python_callable=defect_batch_handler,
        priority_weight=1,
        pool=DEFAULT_POOL
    )

    # 4. 수집 정보 확인
    defect_source_count = TrinoReturnOperator(
        task_id='defect_source_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            SELECT COUNT(*)
            FROM {src_table}
            WHERE CRAWL_DT >= {datetime.now().strftime('%Y-%m-%d')}
            """,
        do_xcom_push=True,
    )

    # 5. 수집 완료 알람
    defect_source_ingestion_done_alarm = SlackOperator(
        task_id='defect_source_ingestion_done_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Social Defect Batch]
        소셜 데이터(Defect) 수집을 완료 했습니다.
        Count : {{ ti.xcom_pull(task_ids='defect_source_count', key='return_value') }}
        Time : {{ ts }}
        """)
    )

    # 6. defect stg 초기화 알람 전송
    defect_stg_drop_alarm = SlackOperator(
        task_id='defect_stg_drop_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Social Defect Batch]
        Staging 영역을 초기화 했습니다.
        Table Name : DW_HIVE.STG.STG_CAR_DEFECTS
        Time : {{ ts }}
        """)
    )

    # 6. defect stg 초기화 알람 전송
    defect_stg_alarm = SlackOperator(
        task_id='defect_stg_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
            [Social Defect Batch]
            데이터를 Staging 합니다.
            Table Name : DW_HIVE.STG.STG_CAR_DEFECTS
            Time : {{ ts }}
            """)
    )

    # 7. defect stg 적재
    defect_stg = TrinoOperator(
        task_id='defect_stg',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            CREATE TABLE {stg_table} AS
            SELECT *
            FROM {src_table}
            WHERE CRAWL_DT >= {datetime.now().strftime('%Y-%m-%d')}
            """
    )

    # 8. defect stg 검증
    defect_stg_count = TrinoReturnOperator(
        task_id='defect_stg_count',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
            SELECT COUNT(*)
            FROM {stg_table}
            """,
        do_xcom_push=True,
    )

    # 9. defect stg 적재 완료 알람 전송
    defect_stg_done_alarm = SlackOperator(
        task_id='defect_stg_done_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
        [Social Defect Batch]
        Staging 완료 했습니다.
        count : {{ ti.xcom_pull(task_ids='defect_stg_count', key='return_value') }}
        Time : {{ ts }}
        """)
    )

    # 6. defect ods 알람 전송
    defect_ods_alarm = SlackOperator(
        task_id='defect_ods_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
                [Social Defect Batch]
                데이터를 ODS 로 적재 합니다.
                Table Name : DL_ICEBERG.ODS.DS_CAR_DEFECTS
                Time : {{ ts }}
                """)
    )

    # 9. defect ods
    defect_ods = TrinoOperator(
        task_id='defect_ods',
        pool=DEFAULT_POOL,
        priority_weight=1,
        query=f"""
        INSERT INTO {ods_table}
        SELECT *,
               DATE_FORMAT(NOW(), '%Y%m%d') PART_DT
        FROM {stg_table}
        """
    )

    # 10. defect ods count
    defect_ods_count = TrinoReturnOperator(
        task_id='defect_ods_count',
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
    defect_ods_inspection_alarm = SlackOperator(
        task_id='defect_ods_inspection_alarm',
        pool=DEFAULT_POOL,
        priority_weight=1,
        channel_name='operation-alert',
        message=dedent("""
            [Social Defect Batch]
            ODS 완료 했습니다.
            count : {{ ti.xcom_pull(task_ids='defect_ods_count', key='return_value') }}
            Time : {{ ts }}
            """)
    )


    defect_batch_alarm >> defect_batch_ingestion_alarm >> defect_ingestion >> defect_source_count
    defect_source_count >> defect_source_ingestion_done_alarm >> defect_stg_drop_alarm >> defect_stg_alarm
    defect_stg_alarm >> defect_stg >> defect_stg_count >> defect_stg_done_alarm >> defect_ods_alarm >> defect_ods
    defect_ods >> defect_ods_count >> defect_ods_inspection_alarm
