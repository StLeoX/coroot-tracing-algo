"""
TraceWeaver 公共函数。
"""

import copy

import networkx as nx


# 结构
class FindAssignmentsResult:
    def __init__(self, pred_assignments, pred_topk_assignments, acc, not_best_count):
        self.pred_assignments = pred_assignments
        self.pred_topk_assignments = pred_topk_assignments
        self.acc = acc
        self.not_best_count = not_best_count


# 用 in_spans 构建服务调用图
def FindOrder(all_spans, all_processes, in_span_partitions, out_span_partitions, sid_span_map):
    assert len(in_span_partitions) == 1

    ep_in, in_spans = list(in_span_partitions.items())[0]
    order = set()
    out_eps = list(out_span_partitions.keys())
    # 用于表示端点之间的依赖关系，图节点是端点的索引（i）。
    G = nx.DiGraph()
    # 用于表示服务之间的依赖关系，图是端点的名称（out_eps[i]）。
    G1 = nx.DiGraph()
    # 用于表示服务的开始和结束之间的依赖关系，图节点是端点的开始和结束状态（out_eps[i] + "-start" 和 out_eps[i] + "-end"）。
    G2 = nx.DiGraph()
    for i in range(len(out_eps)):
        G.add_node(i)
        G1.add_node(out_eps[i])
        G2.add_node(out_eps[i] + "-start")
        G2.add_node(out_eps[i] + "-end")
    for i in range(len(out_eps)):
        for j in range(len(out_eps)):
            if i != j:
                G.add_edge(i, j)
                G1.add_edge(out_eps[i], out_eps[j])
                G2.add_edge(out_eps[i] + "-start", out_eps[j] + "-start")
                G2.add_edge(out_eps[i] + "-start", out_eps[j] + "-end")
                G2.add_edge(out_eps[i] + "-end", out_eps[j] + "-start")
                G2.add_edge(out_eps[i] + "-end", out_eps[j] + "-end")

    for in_span in in_spans:
        outgoing_spans = []
        outgoing_eps = {}
        for out_ep in out_eps:
            # todo 不能用 true 计算
            # span = all_spans[true_assignments[out_ep][in_span.GetId()]]
            span = sid_span_map[in_span.span_id]

            # 一条span用一个tuple-4在向量中表示。
            outgoing_spans.append([span.start_time,  # 单位 milliseconds
                                   span.duration,  # 单位 milliseconds
                                   span.GetParentProcess(all_processes, all_spans),
                                   span.GetChildProcess(all_processes, all_spans)])
        outgoing_spans.sort(key=lambda x: x[0])

        for i, x in enumerate(outgoing_spans):
            outgoing_eps[i] = x[3]

        for i, x in enumerate(outgoing_spans):
            for j, y in enumerate(outgoing_spans):
                if i != j:
                    if x[0] + x[1] > y[0]:
                        if G.has_edge(i, j):
                            G.remove_edge(i, j)
                        if G1.has_edge(x[3], y[3]):
                            G1.remove_edge(x[3], y[3])
                        if G2.has_edge(x[3] + "-end", y[3] + "-start"):
                            G2.remove_edge(x[3] + "-end", y[3] + "-start")
                        if x[0] > y[0]:
                            if G2.has_edge(x[3] + "-start", y[3] + "-start"):
                                G2.remove_edge(x[3] + "-start", y[3] + "-start")
                    if x[0] + x[1] > y[0] + y[1]:
                        if G2.has_edge(x[3] + "-end", y[3] + "-end"):
                            G2.remove_edge(x[3] + "-end", y[3] + "-end")
                    if y[0] + y[1] > x[0]:
                        if G.has_edge(j, i):
                            G.remove_edge(j, i)
                        if G1.has_edge(y[3], x[3]):
                            G1.remove_edge(y[3], x[3])
                        if G2.has_edge(y[3] + "-end", x[3] + "-start"):
                            G2.remove_edge(y[3] + "-end", x[3] + "-start")
                        if y[0] > x[0]:
                            if G2.has_edge(y[3] + "-start", x[3] + "-start"):
                                G2.remove_edge(y[3] + "-start", x[3] + "-start")
                    if y[0] + y[1] > x[0] + x[1]:
                        if G2.has_edge(y[3] + "-end", x[3] + "-end"):
                            G2.remove_edge(y[3] + "-end", x[3] + "-end")

    sorted_grouped_order = topological_sort_grouped(G)
    service_order = copy.deepcopy(sorted_grouped_order)
    for i in range(len(sorted_grouped_order)):
        for j, service_id in enumerate(sorted_grouped_order[i]):
            service_order[i][j] = outgoing_eps[sorted_grouped_order[i][j]]

    return G1


def topological_sort_grouped(G):
    indegree_map = {v: d for v, d in G.in_degree() if d > 0}
    zero_indegree = [v for v, d in G.in_degree() if d == 0]
    grouped_list = []
    while zero_indegree:
        # yield zero_indegree
        grouped_list.append(zero_indegree)
        new_zero_indegree = []
        for v in zero_indegree:
            for _, child in G.edges(v):
                indegree_map[child] -= 1
                if not indegree_map[child]:
                    new_zero_indegree.append(child)
        zero_indegree = new_zero_indegree
    return grouped_list


def SortPartitionsByTraceId(span_partitions):
    for ep, part in span_partitions.items():
        part.sort(key=lambda x: x.trace_id)


def SortPartitionsByTime(span_partitions):
    for ep, part in span_partitions.items():
        part.sort(key=lambda x: (x.start_time, x.end_time))


def GetOutEpsInOrder(out_span_partitions):
    eps = []
    for ep, spans in out_span_partitions.items():
        assert len(spans) > 0
        eps.append((ep, spans[0].start_time))
    eps.sort(key=lambda x: x[1])
    return [x[0] for x in eps]


# 利用 trace_id 确定最准确的 mapping。
# 如果没有 trace_id，返回空。
def GetGroundTruth(in_span_partitions, out_span_partitions):
    assert len(in_span_partitions) == 1
    _, in_spans = list(in_span_partitions.items())[0]
    true_assignments = {ep: {} for ep in out_span_partitions.keys()}
    for in_span in in_spans:
        for ep in out_span_partitions.keys():
            for span in out_span_partitions[ep]:
                if span.trace_id and span.trace_id == in_span.trace_id:
                    true_assignments[ep][in_span.GetId()] = span.GetId()
                    break
    return true_assignments


def AccuracyForSpan(pred_assignments, true_assignments, in_span_id):
    correct = True
    for ep in true_assignments.keys():
        if isinstance(pred_assignments[ep][in_span_id], list):
            if len(pred_assignments[ep][in_span_id]) > 1:
                correct = False
            else:
                pred_assignments[ep][in_span_id] = pred_assignments[ep][in_span_id][0]
        correct = correct and (
                pred_assignments[ep][in_span_id]
                == true_assignments[ep][in_span_id]
        )
    return int(correct)


def TopKAccuracyForSpan(pred_topk_assignments, true_assignments, in_span_id):
    ep0 = list(true_assignments.keys())[0]
    correct = False
    for i in range(len(pred_topk_assignments[ep0][in_span_id])):
        correct = True
        for ep in true_assignments.keys():
            correct = correct and (
                    pred_topk_assignments[ep][in_span_id][i]
                    == true_assignments[ep][in_span_id]
            )
        if correct:
            break
    return int(correct)


def AccuracyForService(pred_assignments, true_assignments, in_span_partitions):
    if true_assignments is None:
        return -1
    assert len(in_span_partitions) == 1
    _, in_spans = list(in_span_partitions.items())[0]
    cnt = 0
    for in_span in in_spans:
        correct = True
        for ep in true_assignments.keys():
            if isinstance(pred_assignments[ep][in_span.GetId()], list):
                if len(pred_assignments[ep][in_span.GetId()]) > 1:
                    correct = False
                else:
                    pred_assignments[ep][in_span.GetId()] = pred_assignments[ep][in_span.GetId()][0]
            correct = correct and (
                    pred_assignments[ep][in_span.GetId()]
                    == true_assignments[ep][in_span.GetId()]
            )
        cnt += int(correct)
    return float(cnt) / len(in_spans)


def TopKAccuracyForService(pred_topk_assignments, true_assignments, in_span_partitions):
    assert len(in_span_partitions) == 1
    _, in_spans = list(in_span_partitions.items())[0]
    cnt = 0
    ep0 = list(true_assignments.keys())[0]
    for in_span in in_spans:
        for i in range(len(pred_topk_assignments[ep0][in_span.GetId()])):
            correct = True
            for ep in true_assignments.keys():
                correct = correct and (
                        pred_topk_assignments[ep][in_span.GetId()][i]
                        == true_assignments[ep][in_span.GetId()]
                )
            if correct:
                cnt += int(correct)
                break
    return float(cnt) / len(in_spans)


def AccuracyEndToEnd(
        pred_assignments_by_process, true_assignments_by_process, in_spans_by_process
):
    processes = true_assignments_by_process.keys()
    trace_acc = {}
    for process in processes:
        for in_span in in_spans_by_process[process]:
            if in_span.trace_id not in trace_acc:
                trace_acc[in_span.trace_id] = True
            true_assignments = true_assignments_by_process[process]
            pred_assignments = pred_assignments_by_process[process]
            for ep in true_assignments.keys():
                if (
                        true_assignments[ep][in_span.GetId()]
                        != pred_assignments[ep][in_span.GetId()]
                ):
                    trace_acc[in_span.trace_id] = False
    correct = sum(trace_acc[tid] for tid in trace_acc)
    return trace_acc, float(correct) / len(trace_acc)


def TopKAccuracyEndToEnd(
        pred_topk_assignments_by_process, true_assignments_by_process, in_spans_by_process
):
    processes = true_assignments_by_process.keys()
    trace_acc = {}
    for i, process in enumerate(processes):
        true_assignments = true_assignments_by_process[process]
        pred_topk_assignments = pred_topk_assignments_by_process[process]
        ep0 = list(true_assignments.keys())[0]
        for x, in_span in enumerate(in_spans_by_process[process]):
            if i != 0 and trace_acc[in_span.trace_id] == False:
                continue
            if len(pred_topk_assignments[ep0][in_span.GetId()]) < 1:
                trace_acc[in_span.trace_id] = False
                continue
            for j in range(len(pred_topk_assignments[ep0][in_span.GetId()])):
                trace_acc[in_span.trace_id] = True
                for ep in true_assignments.keys():
                    if (
                            true_assignments[ep][in_span.GetId()]
                            != pred_topk_assignments[ep][in_span.GetId()][j]
                    ):
                        trace_acc[in_span.trace_id] = False
                if trace_acc[in_span.trace_id] == True:
                    break
    correct = sum(trace_acc[tid] for tid in trace_acc)
    return trace_acc, float(correct) / len(trace_acc)


# 将全量 span 数据按照 service 进行聚合：in_spans 按 callee 聚合，out_spans 按 caller 聚合。
def AggregateSpans(spans, service_names):
    in_spans_by_process = {}
    out_spans_by_process = {}
    for span in spans:
        if span.caller == '' or span.callee == '':
            print(f"span with unknown service: {span.span_id}")
            continue

        # fixme 现在 caller 是 ip 表示的。
        # if span.caller not in service_names or span.callee not in service_names:
        #     continue

        if span.callee not in in_spans_by_process:
            in_spans_by_process[span.callee] = []
        in_spans_by_process[span.callee].append(span)

        if span.caller not in out_spans_by_process:
            out_spans_by_process[span.caller] = []
        out_spans_by_process[span.caller].append(span)

    return in_spans_by_process, out_spans_by_process


# 锁定某个 PID 进行计算，“处理一个进程”
def ComputeSingleProcess(process, in_spans_by_process, out_spans_by_process, all_processes, all_spans, sid_span_map,
                         predictor):
    # fixme 那么边界服务如何进行计算？边界服务：像 redis 这样没有下游服务的服务；像 nginx 这样没有上游服务的服务。
    if process not in in_spans_by_process or process not in out_spans_by_process:
        return None

    in_spans = copy.deepcopy(in_spans_by_process[process])
    out_spans = copy.deepcopy(out_spans_by_process[process])

    # 计算分区（partition）的模板
    def PartitionSpansByEndPoint(spans, endpoint_lambda):
        partitions = {}
        for span in spans:
            ep = endpoint_lambda(span)
            if ep not in partitions:
                partitions[ep] = []
            partitions[ep].append(span)
        for ep, part in partitions.items():
            part.sort(key=lambda x: (x.start_time, x.end_time))
        return partitions

    # 针对 in_spans，拿的是上游服务的ep。
    in_span_partitions = PartitionSpansByEndPoint(
        in_spans, lambda x: x.GetParentProcess(all_processes, all_spans)
    )
    # 针对 out_spans，拿的是下游服务的ep。
    out_span_partitions = PartitionSpansByEndPoint(
        out_spans, lambda x: x.GetChildProcess(all_processes, all_spans)
    )
    # 当前服务的上游服务不止一个（顶点的入度大于一）
    if len(in_span_partitions.keys()) > 1:
        print("Error DAG Struct")

    true_assignments = GetGroundTruth(in_span_partitions, out_span_partitions)

    call_graph = FindOrder(all_spans, all_processes, in_span_partitions, out_span_partitions, sid_span_map)

    instrumented_hops = []
    true_assignments = None

    result = predictor.FindAssignments(process, in_span_partitions, out_span_partitions, True, instrumented_hops,
                                       true_assignments)
    result.acc = AccuracyForService(result.pred_assignments, true_assignments, in_span_partitions)
    return result
