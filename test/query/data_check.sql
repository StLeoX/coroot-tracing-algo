-- 按 trace_id 查 span。
select TraceId,
       SpanId,
       ParentSpanId,
       Timestamp,
       Duration,
       ResourceAttributes['container.id'] AS ContainerID,
       SpanAttributes['net.host.name']    AS HostIP,
       SpanAttributes['net.peer.name']    AS PeerIP,
       SpanAttributes['tgid_req_cs']      AS TgidRead,
       SpanAttributes['tgid_resp_cs']     AS TgidWrite,
       SpanKind
from default.otel_traces
where TraceId = '5c3cfcbd5d1b35a7'
;


-- 按 span_id 查 span。
select TraceId,
       SpanId,
       ParentSpanId,
       Timestamp,
       Duration,
       ResourceAttributes['container.id'] AS ContainerID,
       SpanAttributes['net.host.name']    AS HostIP,
       SpanAttributes['net.peer.name']    AS PeerIP,
       SpanAttributes['tgid_req_cs']      AS TgidRead,
       SpanAttributes['tgid_resp_cs']     AS TgidWrite,
       SpanKind
from default.otel_traces
where SpanId = '4e1a4d2aa03136b7'
;

-- Root Span Filter.
select TraceId,
       SpanId,
       ParentSpanId,
       Timestamp,
       Duration,
       ResourceAttributes['container.id'] AS ContainerID,
       SpanAttributes['net.host.name']    AS HostIP,
       SpanAttributes['net.peer.name']    AS PeerIP
from default.otel_traces
where ParentSpanId = '' -- condition1: for ebpf traces and for otel traces
  and TraceId = SpanId  -- condition2: for ebpf traces (through tracing-algo)
  and length(TraceId) = 16 -- contained by condition2
;

-- 最新数据
select TraceId,
       SpanId,
       ParentSpanId,
       Timestamp,
       Duration,
       ResourceAttributes['container.id'] AS ContainerID,
       SpanAttributes['net.host.name']    AS HostIP,
       SpanAttributes['net.peer.name']    AS PeerIP,
       NetSockPeerAddr,
       ServiceName
from otel_traces
where not position(ContainerID, 'coroot')
order by Timestamp desc
limit 10;

-- 最新数据
select Timestamp
from l7_events_ss
order by Timestamp desc
limit 20;