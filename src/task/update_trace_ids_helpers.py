import pandas
from prefect import get_run_logger

from src.globals import *


# 同时被 SpanCache 的成员函数调用。
def update_trace_ids_helper(span_delta, cache_context):
    update_sqls = []
    discovered_root_span_ids = set()  # 集合去重
    for span in span_delta:
        root_span_id = upward_find_root_span_id(span, cache_context)
        if root_span_id != '':
            update_sqls.append(f"ALTER TABLE {t_trace} " \
                               f"UPDATE TraceId = '{root_span_id}' " \
                               f"WHERE SpanId = '{span.span_id}';")
            discovered_root_span_ids.add(root_span_id)

    for span_id in discovered_root_span_ids:
        update_sqls.append(f"ALTER TABLE {t_trace} " \
                           f"UPDATE TraceId = '{span_id}' " \
                           f"WHERE SpanId = '{span_id}';")

    for sql in update_sqls:
        pandas.read_sql_query(sql, ch_engine)
    return len(update_sqls)


def upward_find_root_span_id(span, cache_context):
    """
    向上找到 root。采用双指针遍历。
    """
    current_span_id = span.span_id
    current_parent_span_id = span.parent_span_id
    while current_parent_span_id != '':
        current_span_id = current_parent_span_id
        if current_span_id in cache_context:  # lookup cache
            current_parent_span_id = cache_context[current_span_id].parent_span_id
        else:  # query db
            current_parent_span_id = query_parent_span_id(current_span_id)
    # 至少向上跳一步，才设置 trace_id，否则会影响 empty(ParentSpanId) 条件。
    if current_span_id != span.span_id:
        return current_span_id  # found root span
    return ''


def query_parent_span_id(span_id):
    logger = get_run_logger()
    query_sql = f"SELECT ParentSpanId FROM {t_trace} WHERE SpanId = '{span_id}'"
    logger.debug(query_sql)
    parent_span_id_df = pandas.read_sql_query(query_sql, ch_engine)
    if len(parent_span_id_df) == 0:
        return ''
    elif len(parent_span_id_df) > 1:
        logger.warning(f"Duplicate SpanId '{span_id}'.")
    return parent_span_id_df['ParentSpanId'][0]
