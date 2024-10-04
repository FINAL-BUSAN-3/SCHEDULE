import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os


# 데이터 추출 함수
def extract_data():
    print("Titanic 데이터 추출 중...")
    try:
        df = pd.read_csv('/actual/path/to/titanic.csv')
        df.to_csv('/actual/path/to/extracted_data.csv', index=False)
        print(df.head())
    except FileNotFoundError as e:
        print(f"Error: {e}")


# 데이터 변환 함수 (결측치 처리 및 컬럼 제거)
def transform_data():
    print("Titanic 데이터 변환 중...")
    try:
        df = pd.read_csv('/actual/path/to/extracted_data.csv')

        # 결측치 처리: 나이('Age')와 선임자('Embarked')의 결측치를 채우기
        df['Age'].fillna(df['Age'].mean(), inplace=True)
        df['Embarked'].fillna(df['Embarked'].mode()[0], inplace=True)

        # 필요 없는 컬럼 제거: 'Cabin', 'Ticket'
        df = df.drop(columns=['Cabin', 'Ticket'])

        df.to_csv('/actual/path/to/transformed_data.csv', index=False)
        print(df.head())
    except FileNotFoundError as e:
        print(f"Error: {e}")


# 데이터 적재 함수
def load_data():
    print("전처리된 Titanic 데이터 적재 중...")
    try:
        df = pd.read_csv('/actual/path/to/transformed_data.csv')
        df.to_csv('/actual/path/to/final_data.csv', index=False)
        print(df.head())
    except FileNotFoundError as e:
        print(f"Error: {e}")


# 기본 인자 설정
default_args = {
    'owner': 'shr',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
        'shr_titanic_pipeline',
        default_args=default_args,
        description='A Titanic data preprocessing pipeline',
        schedule_interval='@daily',  # 매일 실행
        tags=['test'],
        catchup=False,
) as dag:
    # 각 단계 정의
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # 작업 순서 정의
    extract_task >> transform_task >> load_task