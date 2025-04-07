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
limit 1000;


-- 统计各个 span_count 下的 TraceId 数量。
WITH trace_ids AS (
    SELECT TraceId
    FROM otel_traces
    WHERE TraceId = SpanId -- Root Span 条件（推荐）
),
     filtered_traces AS (
         SELECT TraceId, COUNT(DISTINCT SpanId) AS span_count
         FROM trace_ids
                  JOIN otel_traces ON trace_ids.TraceId = otel_traces.TraceId
         WHERE otel_traces.Timestamp > subtractMinutes(now(), 10) -- 一小时之内，注意 UTC 时间
         GROUP BY TraceId -- 按 TraceID 聚合
         LIMIT 1000 -- 限制样本数量
     )
SELECT span_count, COUNT(TraceId) AS trace_count
FROM filtered_traces
GROUP BY span_count
ORDER BY span_count DESC;


-- 统计各个 span_count 下的 TraceId 数量，并且计算占比。
WITH trace_ids AS (
    SELECT TraceId
    FROM otel_traces
    WHERE TraceId = SpanId -- Root Span 条件（推荐）
--     AND SpanName = 'GET /greeting'  -- 然后限制一下入口服务。
),
     filtered_traces AS (
         SELECT TraceId, COUNT(DISTINCT SpanId) AS span_count
         FROM trace_ids
                  JOIN otel_traces ON trace_ids.TraceId = otel_traces.TraceId
         WHERE otel_traces.Timestamp between '2025-04-07 08:40:00' and '2025-04-07 08:50:00' -- 一小时之内，注意 UTC 时间
         GROUP BY TraceId -- 按 TraceID 聚合
--          LIMIT 1000 -- 限制样本数量
     ),
     span_count_stats AS (
         SELECT span_count, COUNT(TraceId) AS trace_count
         FROM filtered_traces
         GROUP BY span_count
     ),
     total_traces AS (
         SELECT SUM(trace_count) AS total_trace_count
         FROM span_count_stats
     )
SELECT s.span_count,
       s.trace_count,
       ROUND((s.trace_count * 1.0 / t.total_trace_count) * 100, 2) AS percentage
FROM span_count_stats s,
     total_traces t
ORDER BY s.span_count DESC;

