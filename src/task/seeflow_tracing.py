"""
SeeFlow 追踪算法。
"""

import pandas
from prefect import task

from src.task.init_variables import *
from src.task.dto.span import Span


@task()
def update_children(time_batch_spans):
    """
    更新 parent 属性。
    :param time_batch_spans: a map
    :return:
    """

    for span in time_batch_spans.values():
        child_candidates = find_child_candidates(span)
        parent_span_id = span.span_id

        # todo 检查 span 之间的 child_candidates 的重叠情况。根据定义是不允许重叠的。当然这里已经用 empty(ParentSpanId) 过滤过。

        updates_sql = ";"
        for child_span_id in child_candidates:
            # 先更新缓存
            if child_span_id in time_batch_spans.keys():
                time_batch_spans[child_span_id].parent_span_id = parent_span_id
            # 后更新DB
            updates_sql += f"ALTER TABLE {t_trace} UPDATE ParentSpanId = {parent_span_id} WHERE SpanId = {child_span_id};"
        pandas.read_sql_query(updates_sql, ch_engine)


def find_child_candidates(parent: Span):
    find_sql = f"SELECT SpanId " \
               f"FROM {t_trace} " \
               f"WHERE empty(ParentSpanId) " \
               f"AND HostAddr = {parent.callee} " \
               f"AND Timestamp > {parent.start_time} " \
               f"AND Timestamp + Duration < {parent.end_time} " \
               f"AND EXISTS (" \
               f"SELECT * FROM {t_l7ss} WHERE " \
               f"{t_l7ss}.Timestamp > {parent.start_time} " \
               f"AND {t_l7ss}.Timestamp + {t_l7ss}.Duration < {parent.end_time} " \
               f"AND {t_l7ss}.TgidRead = {t_trace}.SpanAttributes['tgid_req_cs'] " \
               f"AND {t_l7ss}.TgidWrite = {t_trace}.SpanAttributes['tgid_resp_cs'] " \
               f")"

    child_candidates_df = pandas.read_sql_query(find_sql, ch_engine)

    child_candidates_sid_list = []
    for cc in child_candidates_df:
        child_candidates_sid_list.append(cc['SpanId'])
    return child_candidates_sid_list
