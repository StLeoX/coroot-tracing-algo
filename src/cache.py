from datetime import datetime, timedelta

from cachetools import TTLCache
from prefect import get_run_logger

from src.config import *
from src.task.update_trace_ids_helpers import update_trace_ids_helper


# https://cachetools.readthedocs.io/en/v5.5.0/#cachetools.TTLCache
# https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_Recently_Used_(LRU)
class SpanCache(TTLCache):
    '''
    键值对 (span_id, span)。
    溢出/超时，更新 trace_id，复用 update_trace_ids。
    '''

    def __init__(self, maxsize, ttl, timer):
        super().__init__(maxsize, ttl, timer)

    def popitem(self):
        span_id, span = super().popitem()
        update_trace_ids_helper([span_id], self)
        get_run_logger().info(f"Span '{span_id}' overflowed.")
        return span_id, span

    def expire(self, time=None):
        items = super().expire(time)
        if items is None:
            return None
        expired_span_ids = []
        for span_id, span in items:
            expired_span_ids.append(span_id)
        update_trace_ids_helper(expired_span_ids, self)
        get_run_logger().info(f"Span '{expired_span_ids}' expired.")
        return items

    # todo 更具弹性的缓存？如何理解 update_children 与 update_trace_ids 之间的延迟与命中？


def new_span_cache(map0={}):
    sc = SpanCache(maxsize=fetch_maxsize,
                   ttl=timedelta(seconds=cache_timeout_sec),
                   timer=datetime.utcnow)
    for k, v in map0.items():
        sc[k] = v
    return sc


span_cache_seeflow = new_span_cache()
