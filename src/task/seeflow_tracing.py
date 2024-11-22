"""
SeeFlow 追踪算法。
"""

import pandas
from prefect import get_run_logger, task, states
from sqlalchemy.sql import text

from src.task.init_variables import *
from src.task.dto.span import Span


@task()
def update_children(time_batch_spans):
    """
    更新 parent 属性。
    :param time_batch_spans: a map
    :return:
    """

    if len(time_batch_spans) == 0:
        return states.Failed(message="empty time_batch")

    logger = get_run_logger()

    should_count = 0
    updates_sql = ''
    for span in time_batch_spans.values():
        child_candidates = find_child_candidates(span)
        parent_span_id = span.span_id

        # todo 检查 span 之间的 child_candidates 的重叠情况。根据定义是不允许重叠的。当然这里已经用 empty(ParentSpanId) 过滤过。

        for child_span_id in child_candidates:
            # 先更新缓存
            if child_span_id in time_batch_spans.keys():
                time_batch_spans[child_span_id].parent_span_id = parent_span_id
            # 后更新DB
            updates_sql += f"ALTER TABLE {t_trace} UPDATE ParentSpanId = {parent_span_id} WHERE SpanId = {child_span_id};"
            should_count += 1

    if should_count == 0:
        return states.Failed(message="empty child_candidates, nothing to update")
    else:
        logger.debug(updates_sql)

    with ch_engine.connect() as conn:
        result = conn.execute(text(updates_sql))
        actual_count = result.rowcount

    logger.info(f"should update {should_count} records, actually update {actual_count} records.")


def find_child_candidates(parent: Span):
    logger = get_run_logger()

    parent_callee = f"'{parent.callee}'"
    parent_container_id = f"'{parent.container_id}'"
    parent_start_time = parent.start_time.strftime(timestamp_format)
    parent_end_time = parent.end_time.strftime(timestamp_format)
    find_sql = f"WITH time_range_ss AS (" \
               f"SELECT TgidRead, TgidWrite " \
               f"FROM {t_l7ss} " \
               f"WHERE ContainerId = {parent_container_id} " \
               f"AND Timestamp > {parent_start_time} " \
               f"AND Timestamp + Duration < {parent_end_time}" \
               f") " \
               f"SELECT SpanId " \
               f"FROM {t_trace}, time_range_ss " \
               f"WHERE empty(ParentSpanId) " \
               f"AND SpanAttributes[\'net.sock.host.addr\'] = {parent_callee} " \
               f"AND Timestamp > {parent_start_time} " \
               f"AND Timestamp + Duration < {parent_end_time} " \
               f"AND SpanAttributes['tgid_req_cs'] = TgidRead " \
               f"AND SpanAttributes['tgid_resp_cs'] = TgidWrite "

    # fixme DECIMAL_OVERFLOW Timestamp + Duration

    logger.debug(find_sql)
    child_candidates_df = pandas.read_sql_query(find_sql, ch_engine)
    logger.info(f"found {len(child_candidates_df)} child candidates for span {parent.span_id}")

    child_candidates_sid_list = []
    for cc in child_candidates_df.iterrows():
        child_candidates_sid_list.append(cc[0])
    return child_candidates_sid_list
