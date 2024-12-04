"""
全局初始化变量。
"""

from sqlalchemy import create_engine

from src.config import *

# 初始化 Clickhouse 客户侧配置，初始化连接池。
ch_uri = f"clickhouse://{ch_user}:{ch_password}@{ch_address}/{ch_database}"
ch_engine = create_engine(ch_uri, pool_size=50, pool_recycle=3600, pool_timeout=10)

t_trace = f'{ch_database}.otel_traces'
t_l7ss = f'{ch_database}.l7_events_ss'

t_trace_test = 'test.otel_traces'
t_l7ss_test = f'test.l7_events_ss'
