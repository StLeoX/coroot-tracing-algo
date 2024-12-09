"""
从 Clickhouse 拉取 Traces 数据，针对 coroot-node-agent 的采集数据也可以认为是拉取 Spans 数据。
"""

import pandas
from prefect import get_run_logger, task, states

from src.cache import span_cache_seeflow
from src.globals import *
from src.task.dto.span import Span


@task(retries=3, retry_delay_seconds=2)
def fetch_spans(util_sec, since_sec):
    """
    从 Clickhouse 拉取 span 数据。
    # todo 这里可以指定一个 batch_size 参数，再结合 timeout 做 batch。
    :return: time-batch spans
    """
    span_delta = fetch_spans_helper(util_sec, since_sec, span_cache_seeflow)
    if len(span_delta) == 0:
        return states.Failed(message="Empty time batch")
    else:
        return states.Completed(message=f"Fetch {len(span_delta)} spans.\nThey are {span_delta}.", data=span_delta)


def fetch_spans_helper(util_sec, since_sec, cache_context):
    logger = get_run_logger()

    since_sec_s = f"'{since_sec.strftime(timestamp_format)}'"
    util_sec_s = f"'{util_sec.strftime(timestamp_format)}'"
    fetch_sql = f"SELECT toDateTime64(Timestamp,6) AS TimestampUs, " \
                f"SpanId, " \
                f"Duration, " \
                f"ResourceAttributes['container.id'] AS ContainerID, " \
                f"SpanAttributes['net.host.name'] AS HostIP, " \
                f"SpanAttributes['net.peer.name'] AS PeerIP " \
                f"FROM {t_trace} " \
                f"WHERE Timestamp BETWEEN {since_sec_s} AND {util_sec_s} "
    logger.debug(fetch_sql)

    spans_df = pandas.read_sql_query(fetch_sql, ch_engine)

    span_delta = []
    for _, s in spans_df.iterrows():
        span = Span('',
                    s['SpanId'],
                    s['TimestampUs'],  # 类型 datetime
                    s['Duration'] // 1000,  # 类型 int，单位 nanoseconds 转 microseconds
                    s['HostIP'],
                    s['PeerIP'],
                    s['ContainerID'],
                    )
        cache_context[span.span_id] = span
        span_delta.append(span)
    return span_delta
