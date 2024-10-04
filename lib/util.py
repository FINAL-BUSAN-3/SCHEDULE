from airflow.models.baseoperator import BaseOperator
from trino.dbapi import connect


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