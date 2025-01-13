-- 检查 Tracing 结果准确率。
with trace_ids as (
    select TraceId
    from otel_traces
    where TraceId = SpanId -- Root Span 条件（推荐）
)
select TraceId, count(distinct SpanId) as span_count
from trace_ids
         join otel_traces on trace_ids.TraceId = otel_traces.TraceId
where otel_traces.Timestamp between '2025-01-10 12:20:00' and '2025-01-10 12:22:00' -- 注意 UTC 时间
  and 1                                                                             -- 其他范围条件
group by TraceId -- 按 TraceID 聚合
order by span_count desc
limit 20;


with trace_ids as (
    select distinct TraceId
    from otel_traces
    where length(TraceId) = 16
)
select *
from trace_ids
;