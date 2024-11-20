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

    for s in time_batch_spans.values():
        child_candidates = find_child_candidates(s)

        # todo 检查 span 之间的 child_candidates 的重叠情况。根据定义是不允许重叠的。当然这里已经用 empty(ParentSpanId) 过滤过。

        for cc in child_candidates:
            update_parent_span_id(cc, s.span_id, time_batch_spans)


def find_child_candidates(parent: Span):
    sql = f"SELECT SpanId " \
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

    child_candidates_df = pandas.read_sql_query(sql, ch_engine)

    child_candidates_sid_list = []
    for cc in child_candidates_df:
        child_candidates_sid_list.append(cc['SpanId'])
    return child_candidates_sid_list


def update_parent_span_id(span_id, parent_span_id, sid_span_map):
    if span_id in sid_span_map.keys():
        sid_span_map[span_id].parent_span_id = parent_span_id

    sql = f"ALTER TABLE {t_trace} UPDATE ParentSpanId = {parent_span_id} WHERE SpanId = {span_id}"
    pandas.read_sql_query(sql, ch_engine)
