from datetime import timedelta


class Span:
    def __init__(
            self,
            trace_id,
            span_id,
            start_timestamp,  # nanoseconds
            duration,  # nanoseconds
            caller,
            callee,
            container_id,
    ):
        self.span_id: str = span_id
        self.trace_id: str = trace_id
        self.parent_span_id: str = ''

        self.start_time = start_timestamp  # 单位 milliseconds，pandas 只支持微秒（6位）
        self.duration = duration // 1000  # 单位 milliseconds
        self.end_time = start_timestamp + timedelta(milliseconds=self.duration)
        self.caller = caller  # using network IP
        self.callee = callee  # using network IP
        self.container_id = container_id  # 全局唯一的 container_id，类似于 process_id。
        # self.span_kind = span_kind  # coroot's span always comes from client-side

        self.children_spans = []  # 暂时无用。完全使用DB中的ParentSpanId。
        self.references = ()  # 暂时无用，类似于节点的边？

    def __str__(self):
        return f"Span(trace_id={self.trace_id}, span_id={self.span_id}, start_time={self.start_time}, duration={self.duration}, caller={self.caller}, callee={self.callee}, container_id={self.container_id})"

    def GetId(self):
        return (self.trace_id, self.span_id)

    def AddChild(self, child_span_id):
        self.children_spans.append(child_span_id)

    def GetChildProcess(self, all_processes, all_spans):
        if self.callee:
            return self.callee

        assert len(self.children_spans) == 1
        # 保留 all_processes 的结构，但在使用时 trace_id 始终为空串。
        return all_processes[self.trace_id][
            all_spans[self.children_spans[0]].process_id
        ]

    def GetParentProcess(self, all_processes, all_spans):
        if self.caller:
            return self.caller

        # 针对 Root Span，返回的是自身信息。
        if self.parent_span_id == '':
            return all_processes[self.trace_id][
                all_spans[self.span_id].process_id
            ]

        assert len(self.references) == 1
        return all_processes[self.trace_id][
            all_spans[self.parent_span_id].process_id
        ]
