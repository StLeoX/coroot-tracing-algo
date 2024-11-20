"""
全局共享内存。
"""

from src.config import *

from sqlalchemy import create_engine

# 初始化 Clickhouse 客户侧配置，初始化连接池。
ch_uri = f"clickhouse://{ch_user}:{ch_password}@{ch_address}/{ch_database}"
ch_engine = create_engine(ch_uri, pool_size=50, pool_recycle=3600, pool_timeout=20)

t_trace = f'{ch_database}.otel_traces'
t_l7ss = f'{ch_database}.l7_events_ss'

# 初始化
all_traces = {}
all_cgs = {}
