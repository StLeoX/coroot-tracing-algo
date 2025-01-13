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

alter
table
otel_traces
update Events.Name = ['server_recv', 'server_send'], Events.Timestamp =
        ['2025-01-10 11:55:33.814', '2025-01-10 11:55:39.778'], Events.Attributes = [map(), map()]
where SpanId = 'd557b7d50e909704';

alter
table
otel_traces
update Events = [['server_recv', 'server_send'],
    ['2025-01-10 11:55:33.814', '2025-01-10 11:55:39.778'], [map(), map()]]
where SpanId = 'd557b7d50e909704';

select *
from otel_traces
where SpanId = 'd557b7d50e909704';


-- 以下是对 Nested Table 的测试，结论是无法直接 Update。
CREATE TABLE test.my_table
(
    id Int32,
    Events Nested (
        Timestamp DateTime64(9),
        Name LowCardinality(String),
        Attributes Map( LowCardinality (String), String)
        ) CODEC (ZSTD(1))
) ENGINE = MergeTree()
      ORDER BY id;

-- okay
INSERT INTO test.my_table (id, Events.Timestamp, Events.Name, Events.Attributes)
VALUES (1,
        [toDateTime64('2024-01-01 12:00:00.123456789', 9)],
        ['initial_event'],
        [map('key1', 'value1')]);

select *
from test.my_table
where id = 1;

-- wrong
alter
table
test.my_table
update Events.Timestamp = toDateTime64('2024-01-01 12:00:00.123456789', 9),
    Events.Name = 'initial_event',
    Events.Attributes = map('key2', 'value2')
where id = 1;

-- wrong
alter
table
test.my_table
update Events = [
    [toDateTime64('2024-01-01 12:00:00.123456789', 9)],
    ['initial_event'],
    [map('key2', 'value2')]
    ]
where id = 1;
