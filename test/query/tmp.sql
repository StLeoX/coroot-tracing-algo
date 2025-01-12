with trace_ids as (
    select TraceId
    from otel_traces
    where length(TraceId) = 16
      and TraceId = SpanId -- Root Span 条件（之一，推荐）
)
select TraceId, count(distinct SpanId) as span_count
from trace_ids
         join otel_traces on trace_ids.TraceId = otel_traces.TraceId
where otel_traces.Timestamp between '2025-01-10 11:55:00' and '2025-01-10 11:56:00'
group by TraceId -- 按 TraceID 聚合
order by span_count desc
limit 20;

-- timestampDiff(unit, start, end)
select fromUnixTimestamp(timestampDiff('microsecond', timestamp('2025-01-10 11:55:33.814'),
                                       timestamp('2025-01-10 11:55:33.814968619')));
