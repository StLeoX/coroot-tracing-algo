"""
自顶向下更新 parent 属性。
"""

import pandas
from prefect import get_run_logger, task, states

from src.cache import span_cache_seeflow
from src.globals import *


@task()
def update_children(span_delta):
    """
    更新 parent 属性。
    :param span_delta 增量 Span 的 span_id 列表。
    :return:
    """

    if len(span_delta) == 0:
        return states.Failed(message="Empty time batch")

    updated_count = update_children_helper(span_delta, span_cache_seeflow)
    if updated_count == 0:
        return states.Failed(message="Updated nothing")
    else:
        return states.Completed(message=f"Updated {updated_count} spans.")


def update_children_helper(span_ids, cache_context):
    update_sqls = []
    for span_id in span_ids:
        if span_id not in cache_context:
            continue
        span = cache_context[span_id]
        parent_span_id = span_id
        child_candidates = find_child_candidates(span)

        # todo 检查 span 之间的 child_candidates 的重叠情况，根据定义是不允许重叠的。

        for child_span_id in child_candidates:
            # 先更新 Cache
            # 必须在缓存中才能更新，不能直接插入。
            if child_span_id in cache_context:
                cache_context[child_span_id].parent_span_id = parent_span_id
            # 后更新DB
            update_sqls.append(f"ALTER TABLE {t_trace} " \
                               f"UPDATE ParentSpanId = '{parent_span_id}' " \
                               f"WHERE SpanId = '{child_span_id}';")

    for sql in update_sqls:
        pandas.read_sql_query(sql, ch_engine)
    return len(update_sqls)


def find_child_candidates(parent):
    logger = get_run_logger()

    parent_callee = parent.callee
    parent_start_time = parent.start_time.strftime(timestamp_format)
    parent_end_time = parent.end_time.strftime(timestamp_format)
    find_sql = f"WITH time_range_ss AS (" \
               f"SELECT TgidRead, TgidWrite " \
               f"FROM {t_l7ss} " \
               f"WHERE Timestamp > '{parent_start_time}' " \
               f"AND addNanoseconds(Timestamp, Duration) < '{parent_end_time}'" \
               f") " \
               f"SELECT DISTINCT SpanId " \
               f"FROM time_range_ss, {t_trace} " \
               f"WHERE empty(ParentSpanId) " \
               f"AND SpanAttributes['net.host.name'] = '{parent_callee}' " \
               f"AND Timestamp > '{parent_start_time}' " \
               f"AND addNanoseconds(Timestamp, Duration) < '{parent_end_time}' " \
               f"AND (SpanAttributes['tgid_req_cs'] = TgidRead " \
               f"OR SpanAttributes['tgid_resp_cs'] = TgidWrite) "

    logger.debug(find_sql)
    child_candidates_df = pandas.read_sql_query(find_sql, ch_engine)
    logger.info(f"Found {len(child_candidates_df)} child candidates for span {parent.span_id}.")

    child_candidates_span_ids = []
    for _, cc in child_candidates_df.iterrows():
        child_candidates_span_ids.append(cc['SpanId'])

    if len(child_candidates_df) != 0:
        logger.info(f"They are {child_candidates_span_ids}.")

    return child_candidates_span_ids
