"""
整合相关 task。
"""
import logging
from datetime import datetime, timedelta

from prefect import flow, get_run_logger

from src.config import *
from src.task.fetch_spans import fetch_spans
from src.task.update_children import update_children
from src.task.update_trace_ids import update_trace_ids


@flow(name="SeeFlow")
def SeeFlow():
    # todo introduce dummy task named `start_of_flow`
    get_run_logger().setLevel(level=logging.DEBUG if DEBUG_MODE else logging.INFO)

    # 获取处理时间，注意统一为 UTC+0 时区。
    util_sec = datetime.utcnow() - timedelta(seconds=monitoring_delay_sec)
    since_sec = util_sec - timedelta(seconds=fetch_timeout_sec)
    # 从 DB 拉取数据
    fetch_f = fetch_spans.submit(util_sec, since_sec)
    # 更新 parent 属性
    update_f_1 = update_children.submit(span_delta=fetch_f.result(), wait_for=[fetch_f])
    # 更新 trace_id 属性
    update_f_2 = update_trace_ids.submit(span_delta=fetch_f.result(), wait_for=[fetch_f])
    # end of flow
    # todo 确认是否要 wait
    update_f_1.wait()
    update_f_2.wait()
