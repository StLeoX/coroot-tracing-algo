"""
从 Clickhouse 拉取 traces 数据，包括不同的列组合方式。
"""

from prefect import task
import pandas
import time

from src.task.init_variables import *
from src.task.dto.span import Span


@task(retries=3, retry_delay_seconds=2)
def fetch_spans():
    """
    从 Clickhouse 拉取 span 数据。
    # todo 这里可以指定一个 batch_size 参数，再结合 timeout 做 batch。
    :return: time-batch spans
    """
    util_sec = time.time()
    since_sec = util_sec - fetch_timeout_sec

    span_columns = ['Timestamp',
                    'SpanId',
                    'Duration',
                    'ServiceName',
                    'SpanAttributes[\'net.sock.host.addr\'] as HostAddr',
                    'SpanAttributes[\'net.sock.peer.addr\'] as PeerAddr']

    sql = f"SELECT {', '.join(span_columns)} " \
          f"FROM {t_trace} " \
          f"WHERE Timestamp > {since_sec} AND Timestamp <= {util_sec}"

    spans_df = pandas.read_sql_query(sql, ch_engine)

    sid_span_map = {}
    for _, s in spans_df.iterrows():
        sid_span_map[s['SpanId']] = Span('',
                                         s['SpanId'],
                                         s['Timestamp'],
                                         s['Duration'],
                                         s['HostAddr'],
                                         s['PeerAddr'],
                                         s['ServiceName'],
                                         'client'
                                         )

    return sid_span_map  # todo 使用更高级的 LRU 结构，取代内置的 map 结构。
