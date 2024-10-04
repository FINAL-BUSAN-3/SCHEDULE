from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
import pandas as pd

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='train_titanic',
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval=None
)

download_dataset = BashOperator(
    task_id='download_dataset',
    bash_command='curl -o /tmp/titanic.csv -L https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv',
    dag=dag
)

def _preprocessing_data():
    df = pd.read_csv('/tmp/titanic.csv')

    # 결측값 채우기
    df['Age'].fillna(df['Age'].mean(), inplace=True)
    df['Cabin'].fillna('N', inplace=True)
    df['Embarked'].fillna('N', inplace=True)

    # 컬럼 제거
    df.drop(['PassengerId', 'Name', 'Ticket'], axis=1, inplace=True)

    # lable encoding
    df['Cabin'] = df['Cabin'].str[:1]
    features = ['Cabin', 'Sex', 'Embarked']
    for feature in features:
        le = LabelEncoder()
        le = le.fit(df[feature])
        df[feature] = le.transform(df[feature])

    df.to_csv('/tmp/titanic_preprocessed.csv', index=False)
    print('csv file saved.')

def _train_model():
    df = pd.read_csv('/tmp/titanic_preprocessed.csv')

    # x, y split
    y_df = df['Survived']
    X_df = df.drop('Survived', axis=1)

    # train test split
    X_train, X_test, y_train, y_test = train_test_split(X_df, y_df, test_size=0.2, random_state=11)

    # train
    lr_clf = LogisticRegression()
    lr_clf.fit(X_train, y_train)
    lr_pred = lr_clf.predict(X_test)
    print('LogisticRegression 정확도: {0:.4f}'.format(accuracy_score(y_test, lr_pred)))

preprocessing_data = PythonOperator(
    task_id='preprocessing_data',
    python_callable=_preprocessing_data,
    dag=dag
)

train_model = PythonOperator(
    task_id='train_model',
    python_callable=_train_model,
    dag=dag
)

notify = BashOperator(
    task_id='notify',
    bash_command='echo "pipeline is complete."',
    dag=dag
)

download_dataset >> preprocessing_data >> train_model >> notify