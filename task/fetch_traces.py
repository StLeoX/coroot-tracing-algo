"""
从 Clickhouse 拉取 traces 数据，包括不同的列组合方式。
"""

from .init_variables import ch_engine

from prefect import task
from clickhouse_sqlalchemy import make_session
import pandas


@task
def fetch_traces():
    fetch_traces_sql = r"""SELECT * FROM otel_traces WHERE timestamp > 0 and timestamp < 10"""

    session = make_session(ch_engine)
    handler = session.execute(fetch_traces_sql)
    try:
        fields = handler._metadata.keys
        result = pandas.DataFrame([dict(zip(fields, item)) for item in handler.fetchall()])
    finally:
        handler.close()
        session.close()

    # todo
    result.to_numpy()
