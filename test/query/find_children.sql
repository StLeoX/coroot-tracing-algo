-- 核心 time range SQL 实例。

-- test DB
WITH time_range_ss AS (SELECT TgidRead, TgidWrite
                       FROM default.l7_events_ss
                       WHERE Timestamp > '2024-11-11 11:00:00.123456'
                         AND addNanoseconds(Timestamp, Duration) < '2024-11-11 11:00:09.123456')
SELECT DISTINCT SpanId
FROM time_range_ss,
     test.otel_traces
WHERE empty(ParentSpanId)
  AND SpanAttributes['net.host.name'] = '172.20.0.1'
  AND Timestamp
    > '2024-11-11 11:00:00.123456'
  AND addNanoseconds(Timestamp
          , Duration)
    < '2024-11-11 11:00:09.123456'
  AND (SpanAttributes['tgid_req_cs'] = TgidRead
    OR SpanAttributes['tgid_resp_cs'] = TgidWrite)
;

-- on-the-fly queries

WITH time_range_ss AS (SELECT TgidRead, TgidWrite
                       FROM default.l7_events_ss
                       WHERE Timestamp > '2025-01-06 07:58:22.820800'
                         AND addNanoseconds(Timestamp, Duration) < '2025-01-06 07:58:22.851530')
SELECT DISTINCT SpanId
FROM time_range_ss,
     default.otel_traces
WHERE empty(ParentSpanId)
  AND SpanAttributes['net.host.name'] = '172.24.0.9'
  AND Timestamp > '2025-01-06 07:58:22.820800'
  AND addNanoseconds(Timestamp, Duration) < '2025-01-06 07:58:22.851530'
  AND (SpanAttributes['tgid_req_cs'] = TgidRead OR SpanAttributes['tgid_resp_cs'] = TgidWrite);
