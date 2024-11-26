"""
SeeFlow 追踪算法。
"""

import pandas
from prefect import get_run_logger, task, states

from src.task.dto.span import Span
from src.task.init_variables import *


@task()
def update_children(time_batch_spans):
    """
    更新 parent 属性。
    :param time_batch_spans: a map
    :return:
    """

    if len(time_batch_spans) == 0:
        return states.Failed(message="Empty time batch")

    update_sqls = []
    for span in time_batch_spans.values():
        child_candidates = find_child_candidates(span)
        parent_span_id = span.span_id

        # todo 检查 span 之间的 child_candidates 的重叠情况。根据定义是不允许重叠的。当然这里已经用 empty(ParentSpanId) 过滤过。

        for child_span_id in child_candidates:
            # 先更新缓存
            if child_span_id in time_batch_spans:
                time_batch_spans[child_span_id].parent_span_id = parent_span_id
            # 后更新DB
            update_sqls.append(f"ALTER TABLE {t_trace} " \
                               f"UPDATE ParentSpanId = \'{parent_span_id}\' " \
                               f"WHERE SpanId = \'{child_span_id}\';")

    should_count = len(update_sqls)
    if should_count == 0:
        return states.Completed(message="Nothing for `update_children`")
    else:
        get_run_logger().debug(update_sqls)

    actual_count = 0
    try:
        for sql in update_sqls:
            pandas.read_sql_query(sql, ch_engine)
            actual_count += 1
    finally:
        if should_count != actual_count:
            return states.Failed(message=f"Updated {actual_count} records, but expected {should_count}.")
        else:
            return states.Completed(message=f"Updated {actual_count} records.")


def find_child_candidates(parent: Span):
    logger = get_run_logger()

    parent_callee = f"'{parent.callee}'"
    parent_start_time = parent.start_time.strftime(timestamp_format)
    parent_end_time = parent.end_time.strftime(timestamp_format)
    find_sql = f"WITH time_range_ss AS (" \
               f"SELECT TgidRead, TgidWrite " \
               f"FROM {t_l7ss} " \
               f"WHERE Timestamp > {parent_start_time} " \
               f"AND addNanoseconds(Timestamp, Duration) < {parent_end_time}" \
               f") " \
               f"SELECT DISTINCT SpanId " \
               f"FROM time_range_ss, {t_trace} " \
               f"WHERE empty(ParentSpanId) " \
               f"AND SpanAttributes[\'net.host.name\'] = {parent_callee} " \
               f"AND Timestamp > {parent_start_time} " \
               f"AND addNanoseconds(Timestamp, Duration) < {parent_end_time} " \
               f"AND (SpanAttributes['tgid_req_cs'] = TgidRead " \
               f"OR SpanAttributes['tgid_resp_cs'] = TgidWrite) "
    # 暂时无并发 span 可通过。

    logger.debug(find_sql)
    child_candidates_df = pandas.read_sql_query(find_sql, ch_engine)
    logger.info(f"Found {len(child_candidates_df)} child candidates for span {parent.span_id}.")

    child_candidates_sid_list = []
    for _, cc in child_candidates_df.iterrows():
        child_candidates_sid_list.append(cc['SpanId'])

    if len(child_candidates_df) != 0:
        logger.info(f"They are {child_candidates_sid_list}.")

    return child_candidates_sid_list
