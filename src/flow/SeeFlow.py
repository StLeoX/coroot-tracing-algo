"""
整合相关 task。
"""

from datetime import datetime, timedelta

from prefect import flow

from src.config import *
from src.task.fetch_spans import fetch_spans
from src.task.update_children import update_children
from src.task.update_trace_ids import update_trace_ids


@flow(name="SeeFlow")
def SeeFlow():
    # 获取处理时间
    util_sec = datetime.now() - timedelta(seconds=monitoring_delay_sec)
    since_sec = util_sec - timedelta(seconds=fetch_timeout_sec)
    # 拉取数据到内存
    fetch_f = fetch_spans.submit(util_sec, since_sec)
    # 更新 parent 属性
    update1_f = update_children.submit(time_batch_spans=fetch_f.result(), wait_for=[fetch_f])
    # 更新 trace_id 属性
    update2_f = update_trace_ids.submit(time_batch_spans=fetch_f.result(), wait_for=[fetch_f, update1_f])
    update2_f.wait()
