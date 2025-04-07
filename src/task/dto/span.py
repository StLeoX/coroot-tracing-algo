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
            span_name=""
    ):
        self.span_id: str = span_id
        self.trace_id: str = trace_id
        if trace_id == "None":
            return  # 针对 "Skip"，只构造相应的 trace_id 即可。
        self.parent_span_id: str = ''
        self.gt_parent_span_id: str = ''  # the GroundTruth parent span_id

        '''
        把 start_time 统一转成浮点数：
        dt = datetime(2025, 3, 14, 10, 30, 0, 123456)  # 包含微秒部分
        timestamp = dt.timestamp() # 转换为 Unix 时间戳，类型是 float
        print("原始时间戳:", timestamp, type(timestamp)) # 原始时间戳: 1741919400.123456 <class 'float'>
        '''
        # self.start_time = start_timestamp  # 单位微秒（microseconds），pandas 只支持微秒（6位）
        self.start_time = start_timestamp.timestamp()  # 单位秒，微秒保存在小数点后六位，类型 float。曾用名 start_mus。
        self.duration = (duration // 1000) / 1e6  # 单位秒，微秒保存在小数点后六位。曾用名 duration_mus。
        end_timestamp = start_timestamp + timedelta(milliseconds=duration // 1000)
        self.end_time = end_timestamp.timestamp()

        self.caller = caller  # using network IP
        self.callee = callee  # using network IP
        self.container_id = container_id  # 全局唯一的 container_id，类似于 process_id。
        self.span_kind = 'client'  # coroot's span always comes from client-side

        self.children_spans = []  # 暂时无用。完全使用DB中的ParentSpanId。
        self.references = ()  # 暂时无用，类似于节点的边？

    def __str__(self):
        if self.trace_id == "None":
            return f"Span(trace_id={self.trace_id}, span_id={self.span_id})"
        return f"Span(trace_id={self.trace_id}, span_id={self.span_id}, parent_span_id={self.gt_parent_span_id}, " \
               f"start_time={self.start_time}, duration={self.duration}, caller={self.caller}, callee={self.callee})"

    def SetGTPSId(self, gt):
        self.gt_parent_span_id = gt

    def GetId(self):
        return (self.trace_id, self.span_id)

    def AddChild(self, child_span_id):
        self.children_spans.append(child_span_id)

    # 拿到当前 span 的下游服务，目前保存在 callee ip 当中。
    def GetChildProcess(self, all_processes, all_spans):
        if self.callee:
            return self.callee

        assert len(self.children_spans) == 1
        # 保留 all_processes 的结构，但在使用时 trace_id 始终为空串。
        return all_processes[self.trace_id][
            all_spans[self.children_spans[0]].process_id
        ]

    # 拿到当前 span 的上游服务，目前保存在 caller ip 当中。
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

    # todo
    # 拿到当前 span 访问下游服务的 API。因为当前服务作为 client，直接返回当前 span http.url 等
    def GetChildAPI(self):
        pass

    def GetParentAPI(self):
        pass
