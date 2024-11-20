"""
自底向上更新 trace_id 信息。
"""

import pandas
from prefect import task

from src.task.init_variables import *
from src.task.dto.span import Span


@task()
def update_trace_ids(time_batch_spans):
    """
    更新 trace_id 属性。
    :param time_batch_spans:
    :return:
    """

    for s in time_batch_spans.values():
        root_span_id = up_find_root_span_id(s, time_batch_spans)
        if root_span_id != '':
            update_trace_id(s.span_id, root_span_id)

    # todo history time-batch 中的 span 如何找到并且更新 trace_id？


def up_find_root_span_id(span: Span, sid_span_map):
    """
    向上找到 root。
    :param span:
    :param sid_span_map:
    :return:
    """
    current_span_id = span.span_id
    current_parent_span_id = span.parent_span_id
    while current_parent_span_id != '':
        current_span_id = current_parent_span_id  # up
        if current_parent_span_id in sid_span_map.keys():  # lookup memory
            current_parent_span_id = sid_span_map[current_parent_span_id]
        else:  # query db
            current_parent_span_id = query_parent_span_id(current_parent_span_id)
    if current_span_id != span.span_id:  # 至少向上跳一步，才设置 trace_id
        return current_span_id  # found root
    return ''


def update_trace_id(span_id, trace_id):
    sql = f"ALTER TABLE {t_trace} UPDATE TraceId = {trace_id} WHERE SpanId = {span_id}"
    pandas.read_sql_query(sql, ch_engine)


def query_parent_span_id(span_id):
    sql = f"SELECT ParentSpanId FROM {t_trace} WHERE SpanId = {span_id}"
    parent_span_id_df = pandas.read_sql_query(sql, ch_engine)
    if len(parent_span_id_df) != 1:
        return ''
    return parent_span_id_df['ParentSpanId']
