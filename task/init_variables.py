"""
全局共享内存。
"""

from ..config import *

from sqlalchemy import create_engine

# 初始化 Clickhouse 客户侧配置，初始化连接池。
ch_uri = f"clickhouse://{ch_user}:{ch_password}@{ch_address}/{ch_database}"
ch_engine = create_engine(ch_uri, pool_size=50, pool_recycle=3600, pool_timeout=20)


# 初始化
all_traces = {}
all_cgs = {}

