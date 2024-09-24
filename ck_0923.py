### 브랜치 수정
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# DAG 정의
with DAG(
    dag_id='ck_dag',       # DAG의 이름
    default_args=default_args,
    start_date=datetime(2023, 9, 19),
    schedule_interval='@daily',  # 매일 실행
    catchup=False
) as dag:

    # 현재 날짜 출력
    task_1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # 메시지 출력
    task_2 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello, Airflow!"'
    )

    # 실행 순서 정의
    task_1 >> task_2
