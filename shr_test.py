import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 데이터 추출 함수
def extract_data():
    print("Titanic 데이터 추출 중...")
    try:
        # Titanic 데이터셋을 로컬 CSV에서 불러오기
        df = pd.read_csv(r'C:\바탕 화면\최종\데이터셋\titanic.csv')
        df.to_csv(r'C:\바탕 화면\최종\데이터셋\extracted_data.csv', index=False)
        print("Extracted Data Sample:")
        print(df.head())  # 추출된 데이터 샘플 출력
    except Exception as e:
        print(f"Error: {e}")

# 데이터 변환 함수 (결측치 처리 및 컬럼 제거)
def transform_data():
    print("Titanic 데이터 변환 중...")
    try:
        df = pd.read_csv(r'C:\바탕 화면\최종\데이터셋\extracted_data.csv')

        # 결측치 처리: 나이('age')와 선임자('embarked')의 결측치를 채우기
        df['age'].fillna(df['age'].mean(), inplace=True)
        df['embarked'].fillna(df['embarked'].mode()[0], inplace=True)

        # 필요 없는 컬럼 제거: 'cabin', 'ticket'
        df = df.drop(columns=['cabin', 'ticket'])

        df.to_csv(r'C:\바탕 화면\최종\데이터셋\transformed_data.csv', index=False)
        print("Transformed Data Sample:")
        print(df.head())  # 변환된 데이터 샘플 출력
    except FileNotFoundError as e:
        print(f"Error: {e}")

# 데이터 적재 함수
def load_data():
    print("전처리된 Titanic 데이터 적재 중...")
    try:
        df = pd.read_csv(r'C:\바탕 화면\최종\데이터셋\transformed_data.csv')
        df.to_csv(r'C:\바탕 화면\최종\데이터셋\final_data.csv', index=False)
        print("Final Data Sample:")
        print(df.head())  # 최종 데이터 샘플 출력
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
