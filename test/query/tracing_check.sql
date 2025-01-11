-- 检查 Tracing 结果准确率。
with trace_ids as (
    select TraceId
    from otel_traces
    where TraceId = SpanId -- Root Span 条件（推荐）
)
select TraceId, count(distinct SpanId) as span_count
from trace_ids
         join otel_traces on trace_ids.TraceId = otel_traces.TraceId
where otel_traces.Timestamp > subtractHours(now(), 1) -- 一小时之内，注意 UTC 时间
  and 1                                               -- 其他范围条件
group by TraceId -- 按 TraceID 聚合
order by span_count desc
limit 20;



