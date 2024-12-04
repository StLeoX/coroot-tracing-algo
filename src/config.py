"""
集中配置。
"""

import os

# Clickhouse 连接配置

ch_address = os.getenv('COROOT_CLICKHOUSE_ADDRESS')
if not ch_address:
    ch_address = '127.0.0.1:8123'  # uses the HTTP port

ch_user = os.getenv('COROOT_CLICKHOUSE_USER')
if not ch_user:
    ch_user = 'default'

ch_password = os.getenv('COROOT_CLICKHOUSE_PASSWORD')
if not ch_password:
    ch_password = ''

ch_database = os.getenv('COROOT_CLICKHOUSE_DATABASE')
if not ch_database:
    ch_database = 'default'

# tracing-algo 算法参数

monitoring_delay_sec = 5  # 处理时间落后于墙上时间的延迟，为了适应 agent、server 的时延。
fetch_timeout_sec = 5  # 批处理的时间窗口

# 其他配置
timestamp_format = '%Y-%m-%d %H:%M:%S.%f'
DEBUG_MODE = True
