"""
自底向上更新 trace_id 属性。
该 task 的前提是，各个 ParentSpanId 都被设好了，无论采用何种方式。
"""

from prefect import task, states

from src.cache import span_cache_seeflow
from src.task.update_trace_ids_helpers import update_trace_ids_helper


@task()
def update_trace_ids(span_delta):
    """
    更新 trace_id 属性。
    :param span_delta 增量 Span 列表。
    :return:
    """
    if len(span_delta) == 0:
        return states.Failed(message="Empty time batch")

    updated_count = update_trace_ids_helper(span_delta, span_cache_seeflow)
    if updated_count == 0:
        return states.Failed(message="Updated nothing")
    else:
        return states.Completed(message=f"Updated {updated_count} spans.")
