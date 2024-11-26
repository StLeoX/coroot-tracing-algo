"""
从 Clickhouse 拉取 Traces 数据，针对 coroot-node-agent 的采集数据也可以认为是拉取 Spans 数据。
"""

from prefect import get_run_logger, task, states
import pandas

from src.task.init_variables import *
from src.task.dto.span import Span


@task(retries=3, retry_delay_seconds=2)
def fetch_spans(util_sec, since_sec):
    """
    从 Clickhouse 拉取 span 数据。
    # todo 这里可以指定一个 batch_size 参数，再结合 timeout 做 batch。
    :return: time-batch spans
    """
    logger = get_run_logger()

    fetch_sql = f"SELECT toDateTime64(Timestamp,6) AS TimestampUs, " \
                f"SpanId, " \
                f"Duration, " \
                f"ResourceAttributes[\'container.id\'] AS ContainerID, " \
                f"SpanAttributes[\'net.host.name\'] AS HostIP, " \
                f"SpanAttributes[\'net.peer.name\'] AS PeerIP " \
                f"FROM {t_trace} " \
                f"WHERE Timestamp > {since_sec.strftime(timestamp_format)} " \
                f"AND Timestamp <= {util_sec.strftime(timestamp_format)}"
    logger.debug(fetch_sql)

    spans_df = pandas.read_sql_query(fetch_sql, ch_engine)

    sid_span_map = {}
    for _, s in spans_df.iterrows():
        sid_span_map[s['SpanId']] = Span('',
                                         s['SpanId'],
                                         s['TimestampUs'],
                                         s['Duration'],
                                         s['HostIP'],
                                         s['PeerIP'],
                                         s['ContainerID'],
                                         )
    if len(spans_df) == 0:
        return states.Failed(message="empty time_batch")
    else:
        # todo 引入 cache 后直接返回 states.Completed()
        logger.info(f"Fetch {len(spans_df)} spans, they are {[s.container_id for s in sid_span_map.values()]}.")

    return sid_span_map  # todo 使用更高级的 LRU 结构，取代内置的 map 结构。
