create table test.otel_traces as default.otel_traces;

drop table test.otel_traces;

select count()
from test.otel_traces
;

-- 利用 TraceID 转储数据
insert into test.otel_traces
select *
from (with trace_ids as (
    select TraceId
    from otel_traces
    where length(TraceId) = 16
      and TraceId = SpanId -- Root Span 条件（之一，推荐）
)
      select otel_traces.*
      from otel_traces,
           trace_ids
      where otel_traces.Timestamp between '2025-01-10 11:55:00' and '2025-01-10 11:56:00'
        and trace_ids.TraceId = otel_traces.TraceId
         )
order by Timestamp
;


-- 重设 min timestamp 为 Unix timestamp 0
alter table test.otel_traces update Timestamp = Timestamp - (select min(Timestamp) from test.otel_traces) + 10 WHERE 1
;
