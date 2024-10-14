import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 데이터 추출 함수
def extract_data():
    print("Titanic 데이터 추출 중...")
    try:
        df = pd.read_csv(r'C:\Users\kfq\Desktop\최종\데이터셋\titanic.csv')
        df.to_csv(r'C:\Users\kfq\Desktop\최종\데이터셋\extracted_data.csv', index=False)
        print("Extracted Data Sample:")
        print(df.head())
    except Exception as e:
        print(f"Error: {e}")

# 데이터 변환 함수
def transform_data():
    print("Titanic 데이터 변환 중...")
    try:
        df = pd.read_csv(r'C:\Users\kfq\Desktop\최종\데이터셋\extracted_data.csv')

        # 결측치 처리
        df['age'].fillna(df['age'].mean(), inplace=True)
        df['embarked'].fillna(df['embarked'].mode()[0], inplace=True)

        # 필요 없는 컬럼 제거
        df = df.drop(columns=['cabin', 'ticket'])

        df.to_csv(r'C:\Users\kfq\Desktop\최종\데이터셋\transformed_data.csv', index=False)
        print("Transformed Data 저장 완료:")
        print(df.head())
    except FileNotFoundError as e:
        print(f"Error: {e}")

# 데이터 적재 함수
def load_data():
    print("전처리된 Titanic 데이터 적재 중...")
    try:
        df = pd.read_csv(r'C:\Users\kfq\Desktop\최종\데이터셋\transformed_data.csv')

        df.to_csv(r'C:\Users\kfq\Desktop\최종\데이터셋\final_data.csv', index=False)
        print("Final Data 저장 완료:")
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

with DAG(
        'shr_titanic_pipeline',
        default_args=default_args,
        description='A Titanic data preprocessing pipeline',
        schedule_interval='@daily',
        tags=['test'],
        catchup=False,
) as dag:
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

    extract_task >> transform_task >> load_task
