from airflow.models.baseoperator import BaseOperator
from trino.dbapi import connect
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import certifi
import ssl


class TrinoOperator(BaseOperator):
    def __init__(self, query: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.host = "opyter.iptime.org"
        self.port = 40000
        self.user = "airflow"
        self.catalog = "dl_iceberg"
        self.schema = "stg"
        self.query = query

    def execute(self, context):
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )

        cur = conn.cursor(self.query)
        cur.execute(self.query)


class SlackOperator(BaseOperator):
    def __init__(self, channel_name, message, **kwargs) -> None:
        ssl._create_default_https_context = ssl._create_unverified_context

        conn = connect(
            host="opyter.iptime.org",
            port=40000,
            user="airflow",
            catalog="opertaion_mysql",
            schema="secure",
        )

        cur = conn.cursor()
        token = cur.execute("""
                select code 
                from operation_mysql.secure.pjt_key_store 
                where category = 'SLACK'
                """).fetchall()[0][0]

        self.SLACK_BOT_TOKEN = token  # 받은 Bot User OAuth Access Token으로 바꾸기
        self.channel_name = chennal_name # "operation-alert"
        self.message = message
        self.slack_client = WebClient(token=self.SLACK_BOT_TOKEN)

    def execute(self):
        try:

            response = self.slack_client.conversations_list()
            channels = response["channels"]
            channel_id = None

            for channel in channels:
                if channel["name"] == self.channel_name:
                    channel_id = channel["id"]
                    break

            if channel_id is not None:
                self.slack_client.chat_postMessage(channel=channel_id, text=self.message)
                print(f"{self.channel_name}에 메시지 보내기 성공")
            else:
                print(f"{self.channel_name} 채널을 찾을 수 없다.")

        except SlackApiError as e:
            print(f"오류: {e}")

