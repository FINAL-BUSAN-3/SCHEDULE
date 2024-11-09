import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
from SCHEDULE.lib.util import TrinoOperator, SlackOperator, TrinoReturnOperator
from textwrap import dedent

wf_server = "ec2-100-24-7-128.compute-1.amazonaws.com"

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

def python_ssh(sh):
    subprocess.check_output(sh, shell=True)

def defect_batch_handler(**context):
    python_ssh(f"""
    ssh -i /opt/airflow/busan.pem ubuntu@{wf_server} "python3 /home/ubuntu/test.py"
    """)

DEFAULT_POOL = 'defect'

# DAG 정의
with (DAG(
        'defect_pipeline',
        default_args=default_args,
        description='press_pipeline',
        schedule_interval='0 2 * * *',  # 매일 실행
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

    # 2. 수집 시작
    defect_batch = PythonOperator(
        task_id='defect_batch',
        python_callable=defect_batch_handler,
        priority_weight=1,
        pool=DEFAULT_POOL
    )
    # # 3. press stg 초기화
    # press_stg_drop = TrinoOperator(
    #     task_id='press_stg_drop',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     query=f"""
    #         DROP TABLE {stg_table}
    #         """,
    #     do_xcom_push=True,
    # )
    #
    # # 4. press stg 초기화 알람 전송
    # press_stg_drop_alarm = SlackOperator(
    #     task_id='press_stg_drop_alarm',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     channel_name='operation-alert',
    #     message=dedent("""
    #     [Press Batch]
    #     Staging 영역을 초기화 했습니다.
    #     Table Name : DW_HIVE.STG.STG_PRESS_RAW_DATA
    #     Time : {{ ts }}
    #     """)
    # )
    #
    # # 5. press_stg 적재
    # press_stg = TrinoOperator(
    #     task_id='press_stg',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     query=f"""
    #         CREATE TABLE {stg_table} AS
    #         SELECT *
    #         FROM {src_table}
    #         """
    # )
    #
    # # 6. press stg 검증
    # press_stg_count = TrinoReturnOperator(
    #     task_id='press_stg_count',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     query=f"""
    #         SELECT COUNT(*)
    #         FROM {stg_table}
    #         """,
    #     do_xcom_push=True,
    # )
    #
    # # 7. press stg 적재 완료 알람 전송
    # press_stg_alarm = SlackOperator(
    #     task_id='press_stg_alarm',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     channel_name='operation-alert',
    #     message=dedent("""
    #     [Press Batch]
    #     Staging 완료 했습니다.
    #     count : {{ ti.xcom_pull(task_ids='press_stg_count', key='return_value') }}
    #     Time : {{ ts }}
    #     """)
    # )
    #
    # # 8. press ods 파티션 삭제
    # press_ods_drop_partition = TrinoOperator(
    #     task_id='press_ods_drop_partition',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     query=f"""
    #     DELETE FROM {ods_table}
    #     WHERE PART_DT = DATE_FORMAT(NOW(), '%Y%m%d')
    #     """
    # )
    #
    # # 9. press ods
    # press_ods = TrinoOperator(
    #     task_id='press_ods',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     query=f"""
    #     INSERT INTO {ods_table}
    #     SELECT *,
    #            DATE_FORMAT(NOW(), '%Y%m%d') PART_DT
    #     FROM {stg_table}
    #     """
    # )
    #
    # # 10. press ods count
    # press_ods_count = TrinoReturnOperator(
    #     task_id='press_ods_count',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     query=f"""
    #     SELECT COUNT(*)
    #     FROM {ods_table}
    #     WHERE PART_DT = DATE_FORMAT(NOW(), '%Y%m%d')
    #     """,
    #     do_xcom_push=True,
    # )
    #
    # # 11 ods 적재 알람
    # press_ods_alarm = SlackOperator(
    #     task_id='press_ods_alarm',
    #     pool=DEFAULT_POOL,
    #     priority_weight=1,
    #     channel_name='operation-alert',
    #     message=dedent("""
    #         [Press Batch]
    #         ODS 완료 했습니다.
    #         count : {{ ti.xcom_pull(task_ids='press_ods_count', key='return_value') }}
    #         Time : {{ ts }}
    #         """)
    # )



    # 작업 순서 정의
    defect_batch_alarm >> defect_batch
