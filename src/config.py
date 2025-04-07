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
## 批处理的时间窗口，同时要求准确性和实时性。
fetch_timeout_sec = 15
interval = os.getenv('COROOT_TRACING_INTERVAL')
if interval:
    fetch_timeout_sec = int(interval)

# TraceWeaver 算法参数
## batching 规模（触发时机），论文推荐 100；冒烟测试设为 10
tw_batch_size = 40
## obatch 规模，论文推荐 30；冒烟测试设为 10
tw_batch_size_mis = 20
## TopK 规模，论文推荐 5
tw_top_size = 3
## MIS 迭代规模，论文推荐 20_000；同时兼顾计算效率。
tw_MIS_iterations = 10_000
## MIS 迭代过程中的 score 步长（使用论文推荐值）
tw_MIS_score_epsilon = 1e-6
## 构建 CG 过程中的头部采样率（论文使用 true_assginment 正样本）
tw_CG_sampling_rate = 0.2
tw_CG_sampling_threshold = 20

## 处理时间落后于墙上时间的延迟，为了适应 pipeline 中的时延。
monitoring_delay_sec = 1
delay = os.getenv('COROOT_TRACING_DELAY')
if delay:
    monitoring_delay_sec = int(delay)

# 其他配置
timestamp_format = '\'%Y-%m-%d %H:%M:%S.%f\''  # 通常是微妙精度。
timestamp_format_no_quote = '%Y-%m-%d %H:%M:%S.%f'  # 通常是微妙精度。

# 是否调试模式
DEBUG_MODE = True
