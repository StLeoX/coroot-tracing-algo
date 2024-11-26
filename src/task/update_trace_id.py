"""
自底向上更新 trace_id 信息。
该 task 的前提是，各个 ParentSpanId 都被设好了，无论采用何种方式。
"""

import pandas
from prefect import get_run_logger, task, states

from src.task.dto.span import Span
from src.task.init_variables import *


@task()
def update_trace_ids(time_batch_spans):
    """
    更新 trace_id 属性。
    :param time_batch_spans:
    :return:
    """
    if len(time_batch_spans) == 0:
        return states.Failed(message="Empty time batch")

    update_sqls = []
    discovered_root_span_ids = set()  # 去重
    for span in time_batch_spans.values():
        root_span_id = upward_find_root_span_id(span, time_batch_spans)
        if root_span_id != '':
            update_sqls.append(f"ALTER TABLE {t_trace} " \
                               f"UPDATE TraceId = \'{root_span_id}\' " \
                               f"WHERE SpanId = \'{span.span_id}\';")
            discovered_root_span_ids.add(root_span_id)

    should_count = len(update_sqls)
    if should_count == 0:
        return states.Completed(message="Nothing for `update_trace_ids`.")
    else:
        get_run_logger().debug(update_sqls)

    for span_id in discovered_root_span_ids:
        update_sqls.append(f"ALTER TABLE {t_trace} " \
                           f"UPDATE TraceId = \'{span_id}\' " \
                           f"WHERE SpanId = \'{span_id}\';")

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

    # todo 更具弹性的缓存！
    # todo history time-batch 中的 span 如何找到并且更新 trace_id？
    # todo 或者说如何理解 update1 与 update2 之间的延迟？两者在逻辑上是完全异步的，只能说延迟越小越能命中内存。


def upward_find_root_span_id(span: Span, sid_span_map):
    """
    向上找到 root。双指针遍历。
    :param span:
    :param sid_span_map:
    :return:
    """
    current_span_id = span.span_id
    current_parent_span_id = span.parent_span_id
    while current_parent_span_id != '':
        current_span_id = current_parent_span_id
        if current_span_id in sid_span_map:  # lookup cache
            current_parent_span_id = sid_span_map[current_span_id].parent_span_id
        else:  # query db
            current_parent_span_id = query_parent_span_id(current_span_id)
    # 至少向上跳一步，才设置 trace_id，否则会影响 empty(ParentSpanId) 条件。
    if current_span_id != span.span_id:
        return current_span_id  # found root span
    return ''


def query_parent_span_id(span_id):
    query_sql = f"SELECT ParentSpanId FROM {t_trace} WHERE SpanId = \'{span_id}\'"
    get_run_logger().debug(query_sql)
    parent_span_id_df = pandas.read_sql_query(query_sql, ch_engine)
    if len(parent_span_id_df) != 1:
        return ''
    return parent_span_id_df['ParentSpanId']
