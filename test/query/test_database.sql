create table test.otel_traces as default.otel_traces;

truncate table test.otel_traces;

select count(),min(Timestamp), max(Timestamp)
from test.otel_traces
;

-- 利用 TraceID 转储数据
insert into test.otel_traces
select *
from (with trace_ids as (
    select TraceId
    from default.otel_traces
    where length(TraceId) = 16
      and TraceId = SpanId -- Root Span 条件（之一，推荐）
)
      select default.otel_traces.*
      from default.otel_traces,
           trace_ids
      where default.otel_traces.Timestamp between '2025-01-10 11:55:00' and '2025-01-10 11:56:00'
        and trace_ids.TraceId = default.otel_traces.TraceId
         )
order by Timestamp
;


-- 重设 min timestamp 为 Unix timestamp 0，需要新开一段数据，因为不能直接更新键 timestamp

-- 让 Server Span 正好被 Client Span 包起来。
/*insert into test.otel_traces*/
select addMilliseconds(Timestamp, 0.01) as Timestamp -- timestamp 加 0.01ms（ms，后三位）
     , TraceId
     , hex(rand64())                    as SpanId
     , ParentSpanId                                  -- 要对 ParentSpanID 重设，不是一条查询能完成的。
     , TraceState
     , SpanName
     , 'SPAN_KIND_SERVER'               as SpanKind
     , ServiceName
     , ResourceAttributes
     , SpanAttributes
     , (Duration - 20000)               as Duration  -- duration（单位 ns，后九位）减 0.02ms。
     , StatusCode
     , StatusMessage
     , Events.Timestamp
     , Events.Name
     , Events.Attributes
     , Links.TraceId
     , Links.SpanId
     , Links.TraceState
     , Links.Attributes
from test.otel_traces
where TraceId = '703e127ba30ba1fc'
;
