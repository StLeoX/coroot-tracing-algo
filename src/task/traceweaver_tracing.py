"""
TraceWeaver V2 追踪算法。
"""

import copy
import heapq
import math

import networkx as nx
import numpy as np
import scipy.stats
from prefect import task, states
from sklearn import mixture

import src.config as config
import src.task.traceweaver as tw
import src.task.utils as utils


@task(log_prints=True)
def update_children(time_batch_spans):
    if len(time_batch_spans) == 0:
        return states.Failed(message="Empty time batch")

    spans = time_batch_spans.values()

    service_names = tw.GetServiceNames(spans)

    twV2 = TraceWeaverV2(spans, service_names)

    in_spans_by_process, out_spans_by_process = tw.AggregateSpans(spans, service_names)

    # 遍历系统中的全体 process
    for process in service_names:
        result = tw.ComputeSingleProcess(process, in_spans_by_process, out_spans_by_process, service_names,
                                         spans, time_batch_spans, twV2)
        if result is None:
            print(f"Failed to compute process {process}")
            continue
        print(f"Computed process {process}", f", `not_best_count` is {result.not_best_count}")

        # 展开 assignment 结构
        # all_assignments[ep][in_spans[ind].GetId()] = ("NA", "NA")，所以 in_span 对应 parent span，out_span 对应 child span。
        for ep, mappings in result.pred_assignments.items():
            for parent_sid, child_sid in mappings.items():
                # todo 因为 ComputeDistParams 截断造成的 NA
                if child_sid[1] == 'NA':
                    continue
                utils.update_parent(time_batch_spans, child_sid[1], parent_sid[1])  # 下标 0 代表 trace_id。

    return states.Completed(message="Finished `update_children`.")


# 基本配置
VERBOSE = False


class TraceWeaverV1(object):
    def __init__(self, all_spans, all_processes):
        self.all_spans = all_spans
        self.all_processes = all_processes
        self.services_times = {}
        self.parallel = False
        self.instrumented_hops = []
        self.true_assignments = None
        self.normal = True

    # 通过 trace_id 先验知识，判断服务是否串行（sequential）。
    # 只要时间区间存在一个违背，那就判别为并行（parallel）。
    # 既然是通过 trace_id 先验知识判断，那也可以直接设置为 parallel 模式。
    def VerifySerialDependency(self, in_spans, out_eps, out_span_partitions):
        def FindSpanTraceId(trace_id, spans):
            for s in spans:
                if s.trace_id == trace_id:
                    return s
            return None

        for s in in_spans:
            trace_id = s.trace_id
            prev_time = s.start_time
            for ep in out_eps:
                out_span = FindSpanTraceId(trace_id, out_span_partitions[ep])
                assert out_span.start_time > prev_time
                prev_time = out_span.ent_time

    def GetOutEpsInOrder(self, out_span_partitions, call_graph=None):
        if call_graph:
            return list(nx.topological_sort(call_graph))
        eps = []
        for ep, spans in out_span_partitions.items():
            assert len(spans) > 0
            eps.append((ep, spans[0].start_time))
        eps.sort(key=lambda x: x[1])
        return [x[0] for x in eps]

    def ComputeEpPairDistParams(
            self,
            in_span_partitions,
            out_span_partitions,
            out_eps,
            in_span_start,
            in_span_end,
    ):
        # 计算分布中的参数值，比如泊松分布中的 lambda 参数。
        def ComputeDistParams(ep1, ep2, t1, t2):
            # 转成 timestamp 类型进行计算，datetime 不支持加法和 sum
            t1 = t1[in_span_start:in_span_end]
            t2 = t2[in_span_start:in_span_end]
            # print(len(t1), len(t2), in_span_start, in_span_end)
            # assert len(t1) == len(t2), f"{t1}\n{t2}"
            # fixme 暂时通过截断的方式处理一下“不对齐”的问题
            if len(t1) != len(t2):
                minLen = min(len(t1), len(t2))
                t1 = t1[:minLen]
                t2 = t2[:minLen]
            # # 区间 [start, end) 上的 mean
            # def mean_diff_microseconds(ts2, ts1, start, end):
            #     d = 0  # 单位微秒（microseconds）
            #     for i in range(start, end):
            #         d += (ts2[i] - ts1[i]).total_seconds() * 1e6
            #     return d / (end - start)

            # mean = mean_diff_microseconds(t2, t1, 0, len(t1))

            mean = (sum(t2) - sum(t1)) / len(t1)
            batch_means = []
            nbatches = 10
            batch_size = math.ceil(float(len(t1)) / nbatches)
            for i in range(nbatches):
                start = i * batch_size
                end = min(len(t1), (i + 1) * batch_size)
                if end - start > 0:
                    batch_means.append(
                        (sum(t2[start:end]) - sum(t1[start:end])) / (end - start)
                    )
            std = math.sqrt(batch_size) * scipy.stats.tstd(batch_means)
            if VERBOSE:
                print(
                    "Computing ep pair (%s, %s), distribution params: %f, %f"
                    % (ep1, ep2, mean, std)
                )
            # 计算均值 mean 和标准差 std
            self.services_times[(ep1, ep2)] = mean, std

        if self.parallel:
            for i in range(len(out_eps)):
                ep1 = list(in_span_partitions.keys())[0]
                ep2 = out_eps[i]
                t1 = sorted([s.start_time for s in in_span_partitions[ep1]])
                t2 = sorted([s.start_time for s in out_span_partitions[ep2]])
                ComputeDistParams(ep1, ep2, t1, t2)
        else:
            # between incoming -- first outgoing
            ep1 = list(in_span_partitions.keys())[0]
            ep2 = out_eps[0]
            t1 = sorted([s.start_time for s in in_span_partitions[ep1]])
            t2 = sorted([s.start_time for s in out_span_partitions[ep2]])
            ComputeDistParams(ep1, ep2, t1, t2)

            # between outgoing -- outgoing
            for i in range(len(out_eps) - 1):
                ep1 = out_eps[i]
                ep2 = out_eps[i + 1]
                t1 = sorted(
                    [s.end_time for s in out_span_partitions[ep1]]
                )
                t2 = sorted([s.start_time for s in out_span_partitions[ep2]])
                ComputeDistParams(ep1, ep2, t1, t2)

            # between last outgoing -- incoming
            ep1 = out_eps[-1]
            ep2 = list(in_span_partitions.keys())[0]
            t1 = sorted([s.end_time for s in out_span_partitions[ep1]])
            t2 = sorted([s.end_time for s in in_span_partitions[ep2]])
            ComputeDistParams(ep1, ep2, t1, t2)

    # 计算两个请求之间的概率（weight/cost）
    # ep1:ep1: 上下游端点
    # t1:t2: 相应的时间戳
    def GetEpPairCost(self, ep1, ep2, t1, t2, normalized=False):
        dist_value = self.services_times[(ep1, ep2)]
        if type(dist_value) is list:
            # 高斯核密度估计
            kde = scipy.stats.gaussian_kde(dist_value)
            p = kde.evaluate(t2 - t1)
            return p

        elif type(dist_value) is mixture._gaussian_mixture.GaussianMixture:
            # 高斯混合估计
            return dist_value.score(np.array([t2 - t1]).reshape(1, -1))

        else:
            # 泊松分布估计
            # 注意，通过覆盖率测试，知道现在走的是这个分布。
            mean, std = dist_value
            if std < 1.0e-12:
                std = 0.001
            if self.normal:
                if not normalized:
                    p = scipy.stats.norm.logpdf(t2 - t1, loc=mean, scale=std)
                else:
                    p = scipy.stats.norm.pdf(t2 - t1, loc=mean, scale=std)
            else:
                p = scipy.stats.expon.logpdf(t2 - t1, scale=mean)
            return p

    # sequential 代表了 API 之间的顺序关系，也就是相互依赖的。
    # 具体来说，A在接收到请求后，首先调用B，等待B的响应，然后再调用C，这种处理方式体现了代码执行中的顺序性。
    def ScoreAssignmentSequential(self, assignment, normalized=False):
        cost = 0
        for i in range(len(assignment)):
            curr_ep = (
                assignment[i].GetParentProcess(self.all_processes, self.all_spans)
                if i == 0
                else assignment[i].GetChildProcess(self.all_processes, self.all_spans)
            )
            curr_time = (
                assignment[i].start_time
                if i == 0
                else assignment[i].end_time
            )

            # 在 sequential 方式下，会利用 child span 之间的不重叠性，者带来了什么？
            next_i = (i + 1) % len(assignment)
            next_ep = (
                assignment[next_i].GetParentProcess(self.all_processes, self.all_spans)
                if next_i == 0
                else assignment[next_i].GetChildProcess(self.all_processes, self.all_spans)
            )
            next_time = (
                assignment[next_i].end_time
                if next_i == 0
                else assignment[next_i].start_time
            )
            if VERBOSE:
                print("Computing cost between", curr_ep, next_ep)
            cost += self.GetEpPairCost(curr_ep, next_ep, curr_time, next_time, normalized)

        if normalized:
            return cost / len(assignment)
        return cost

    # 区别于 sequential，parallel 代表了 API 之间的并发关系，也就是互不依赖的。
    # normalized：是否希望结果归一化。
    def ScoreAssignmentParallel(self, assignment, normalized=False):
        cost = 0
        for i in range(1, len(assignment)):
            curr_ep = assignment[0].GetParentProcess(self.all_processes, self.all_spans)
            curr_time = assignment[0].start_time

            next_ep = assignment[i].GetChildProcess(self.all_processes, self.all_spans)
            next_time = assignment[i].start_time
            if VERBOSE:
                print("Computing cost between", curr_ep, next_ep)
            cost += self.GetEpPairCost(curr_ep, next_ep, curr_time, next_time, normalized)

        if normalized:
            return cost / len(assignment)
        return cost

    # 搜索过程在 v1 中很简单：DFS 遍历全体 candidate，显然存在状态爆炸问题。
    # 或者 DFS 到预设的 best_score，显然这是个局部最优的结果，并且 best_score 在不同负载中需要重设。
    def FindMinCostAssignment(self, in_span, out_eps, out_span_partitions):
        global best_assignment
        global best_score
        best_assignment = None
        best_score = -1000000.0
        score_list = []

        def DfsTraverse(stack):
            global best_assignment
            global best_score
            i = len(stack)
            if VERBOSE:
                print("DAG Traverse", i, out_eps, f"{len(stack)}")
            last_span = stack[-1]
            # 出口计算 score
            if i == len(out_span_partitions) + 1:
                # 是否 parallel 是来源于用户的先验知识。
                # 值得思考的是，为何 score 方式会发生改变？
                if self.parallel:
                    score = self.ScoreAssignmentParallel(stack)
                else:
                    score = self.ScoreAssignmentSequential(stack)
                score_list.append(score)
                if best_score < score:
                    best_assignment = stack
                    best_score = score
            elif i in self.instrumented_hops:
                ep = out_eps[i - 1]
                span_id = self.true_assignments[ep][in_span.GetId()]
                for s in out_span_partitions[ep]:
                    if s.GetId() == span_id:
                        DfsTraverse(stack + [s])
                        break
            else:
                # !TODO: filter out branches that have high cost
                ep = out_eps[i - 1]
                for s in out_span_partitions[ep]:
                    # parallel eps
                    if self.parallel:
                        if (
                                in_span.start_time < s.start_time
                                and s.end_time
                                < in_span.end_time
                        ):
                            DfsTraverse(stack + [s])

                    # Sequential eps
                    else:
                        # first ep
                        if (
                                i == 1
                                and in_span.start_time < s.start_time
                                and s.end_time
                                < in_span.end_time
                        ):
                            DfsTraverse(stack + [s])
                        # all other eps
                        elif (
                                i <= len(out_eps)
                                and last_span.end_time < s.start_time
                                and s.end_time
                                < in_span.end_time
                        ):
                            DfsTraverse(stack + [s])

        DfsTraverse([in_span])
        # return a dictionary of {ep: span}
        ret = {}
        if best_assignment is not None:
            assert len(out_eps) == len(best_assignment) - 1
            ret = {out_eps[i]: best_assignment[i + 1] for i in range(len(out_eps))}

        return ret

    def AddAssignment(
            self,
            in_span,
            assignment,
            all_assignments,
            out_span_partitions,
            out_eps,
            delete_out_spans=False,
            skips=False
    ):
        # add assignment to all_assignments
        for ep in out_eps:
            if ep not in all_assignments:
                all_assignments[ep] = {}
            out_span = assignment.get(ep, None)
            if skips:  # 供 V3 使用
                if out_span is None:
                    out_span_id = ("NA", "NA")
                else:
                    out_span_id = out_span.GetId() if out_span.trace_id != "None" else ('Skip', 'Skip')
            else:
                out_span_id = out_span.GetId() if out_span is not None else ("NA", "NA")
            all_assignments[ep][in_span.GetId()] = out_span_id

        if delete_out_spans:
            # remove spans of this assignment so they can't be assigned again
            # !TODO: this implementation is not efficient
            for ep, span in assignment.items():
                if span.trace_id != "None":
                    # print(ep, in_span, span)
                    out_span_partitions[ep].remove(span)

    def AddTopKAssignments(
            self,
            in_span,
            topk_assignments,
            all_topk_assignments,
            out_span_partitions,
            out_eps,
            skips=False
    ):
        for i, ep in enumerate(out_eps):
            if ep not in all_topk_assignments:
                all_topk_assignments[ep] = {}
            all_topk_assignments[ep][in_span.GetId()] = []
            for assignment in topk_assignments:
                assignment = assignment[1]
                out_span = assignment[i + 1]
                if skips:
                    if out_span is None:
                        out_span_id = ("NA", "NA")
                    else:
                        out_span_id = out_span.GetId() if out_span.trace_id != "None" else ('Skip', 'Skip')
                else:
                    out_span_id = out_span.GetId() if out_span is not None else ("NA", "NA")
                all_topk_assignments[ep][in_span.GetId()].append(out_span_id)

    def FindAssignments(self, process, in_span_partitions, out_span_partitions, parallel, instrumented_hops,
                        true_assignments):
        assert len(in_span_partitions) == 1
        self.parallel = parallel
        self.instrumented_hops = instrumented_hops
        self.true_assignments = true_assignments
        _, in_spans = list(in_span_partitions.items())[0]
        out_eps = self.GetOutEpsInOrder(out_span_partitions)
        out_span_partitions_copy = copy.deepcopy(out_span_partitions)
        all_assignments = {}
        cnt = 0
        cnt_unassigned = 0
        batch_size = 100
        for in_span in in_spans:
            if cnt % batch_size == 0:
                self.ComputeEpPairDistParams(
                    in_span_partitions,
                    out_span_partitions,
                    out_eps,
                    in_span_start=cnt,
                    in_span_end=min(len(in_spans), cnt + batch_size),
                )
            # find the min-cost assignment for in_span
            min_cost_assignment = self.FindMinCostAssignment(
                in_span, out_eps, out_span_partitions_copy
            )
            self.AddAssignment(
                in_span,
                min_cost_assignment,
                all_assignments,
                out_span_partitions_copy,
                out_eps,
                delete_out_spans=True
            )
            cnt += 1
            cnt_unassigned += len(min_cost_assignment) == 0
            # !TODO: update mean, std of service times using EWMA
        if VERBOSE:
            print("V1 finished %d spans, unassigned spans: %d" % (cnt, cnt_unassigned))
        return all_assignments


class TraceWeaverV2(TraceWeaverV1):
    def __init__(self, all_spans, all_processes):
        super().__init__(all_spans, all_processes)
        self.all_spans = all_spans
        self.all_processes = all_processes
        self.process = ''
        self.services_times = {}
        self.parallel = False
        self.instrumented_hops = []
        self.true_assignments = None
        self.per_span_candidates = {}

    def AddToCandidatesList(self, stack):
        if stack[0].GetId() not in self.per_span_candidates:
            self.per_span_candidates[stack[0].GetId()] = 0

        self.per_span_candidates[stack[0].GetId()] += 1

    def FindTopKAssignments(self, in_span, out_eps, out_span_partitions, K):
        global top_assignments
        top_assignments = []

        def DfsTraverse(stack):
            global top_assignments
            i = len(stack)
            if VERBOSE:
                print("DAG Traverse", i, out_eps, f"{len(stack)}")
            last_span = stack[-1]
            # 出口计算 score
            if i == len(out_span_partitions) + 1:
                self.AddToCandidatesList(stack)
                if self.parallel:
                    score = self.ScoreAssignmentParallel(stack)
                else:
                    score = self.ScoreAssignmentSequential(stack)
                # 通过最小堆维护 topK 的 candidate mapping
                # min heap
                heapq.heappush(top_assignments, (score, stack))
                if len(top_assignments) > K:
                    heapq.heappop(top_assignments)
            elif i in self.instrumented_hops:
                ep = out_eps[i - 1]
                span_id = self.true_assignments[ep][in_span.GetId()]
                for s in out_span_partitions[ep]:
                    if s.GetId() == span_id:
                        DfsTraverse(stack + [s])
                        break
            else:
                ep = out_eps[i - 1]
                for s in out_span_partitions[ep]:
                    # parallel eps
                    if self.parallel:
                        # first ep
                        if (
                                i == 1
                                and in_span.start_time <= s.start_time
                                and s.end_time
                                <= in_span.end_time
                        ):
                            DfsTraverse(stack + [s])
                        # all other eps
                        elif (
                                i <= len(out_eps)
                                and last_span.start_time <= s.start_time
                                and s.end_time
                                <= in_span.end_time
                        ):
                            DfsTraverse(stack + [s])
                    # Sequential eps
                    else:
                        # first ep
                        if (
                                i == 1
                                and in_span.start_time <= s.start_time
                                and s.end_time
                                <= in_span.end_time
                        ):
                            DfsTraverse(stack + [s])
                        # all other eps
                        elif (
                                i <= len(out_eps)
                                and last_span.end_time <= s.start_time
                                and s.end_time
                                <= in_span.end_time
                        ):
                            DfsTraverse(stack + [s])

        DfsTraverse([in_span])
        top_assignments.sort(reverse=True)
        return top_assignments

    # type1：通过下标表示 out_ep；否则字符串表示。
    def GetSpanIDNotation(self, out_eps, assignment, type1):
        span_id_notation = []

        if type1:
            for i in range(1, len(assignment)):
                span_id_notation.append(assignment[i].GetId())
        else:
            for out_ep in out_eps:
                span_id_notation.append(assignment[out_ep].GetId())
        return span_id_notation

    def FindAssignments(self, process, in_span_partitions, out_span_partitions, parallel, instrumented_hops,
                        true_assignments):
        # 判断服务依赖关系是否允许计算。
        if len(in_span_partitions) == 0:
            print(f"{process} has no upstream.")
            return None
        if len(in_span_partitions) > 1:
            print(f"{process} has those more than one upstreams, they are {in_span_partitions.keys()}.")
            return None
        self.process = process
        self.parallel = parallel
        self.instrumented_hops = instrumented_hops
        self.true_assignments = true_assignments
        # 用来统计每个 sid 下 candidate 的数量
        self.per_span_candidates = {}

        span_to_top_assignments = {}
        ep, in_spans = list(in_span_partitions.items())[0]
        out_eps = self.GetOutEpsInOrder(out_span_partitions)
        out_span_partitions_copy = copy.deepcopy(out_span_partitions)
        # 用来触发 batch 计算，一个 batch 的大小。
        batch_size = config.tw_batch_size
        # 一个 optimization batch 的大小。这个真值是k个 trace 所有 span 的数量，并且注意到 k 至少为 1。
        batch_size_mis = config.tw_batch_size_mis
        # 最大 candidate mapping 数量。
        topK = config.tw_top_size
        # 用 cnt % batch_size 来触发 batch 计算。
        cnt = 0
        cnt_unassigned = 0
        # 统计预测失误的情况。
        not_best_count = 0
        # 返回结构类似：all_assignments[ep][in_spans[ind].GetId()] = ("NA", "NA")
        all_assignments = {}
        top_assignments = []
        batch_in_spans = []
        for in_span in in_spans:
            # Cut（切分）有证明在附录中。
            # Creating Optimization Batches, condition1
            if cnt % batch_size == 0:
                self.ComputeEpPairDistParams(
                    in_span_partitions,
                    out_span_partitions,
                    out_eps,
                    cnt,
                    min(len(in_spans), cnt + batch_size),
                )
                if VERBOSE:
                    print("V2 finished %d spans, unassigned spans: %d" % (cnt, cnt_unassigned))
            top_k = self.FindTopKAssignments(in_span, out_eps, out_span_partitions_copy, topK)
            span_to_top_assignments[in_span] = top_k
            top_assignments.append(top_k)
            batch_in_spans.append(in_span)
            cnt += 1

            # Creating Optimization Batches, condition2
            if cnt % batch_size_mis == 0:
                assignments = self.GetAssignmentsMIS(top_assignments)
                assert len(assignments) == len(top_assignments) == len(batch_in_spans)
                for ind in range(len(assignments)):
                    assignment = {}
                    if len(assignments[ind]) > 0:
                        assert len(out_eps) == len(assignments[ind]) - 1
                        for ii in range(len(out_eps)):
                            assignment[out_eps[ii]] = assignments[ind][ii + 1]
                    if len(span_to_top_assignments[batch_in_spans[ind]]) < 1 or not assignment:
                        not_best_count += 1
                    else:
                        best = self.GetSpanIDNotation(out_eps, span_to_top_assignments[batch_in_spans[ind]][0][1],
                                                      type1=True)
                        chosen = self.GetSpanIDNotation(out_eps, assignment, type1=False)
                        if best != chosen:
                            not_best_count += 1
                    self.AddAssignment(
                        batch_in_spans[ind],
                        assignment,
                        all_assignments,
                        out_span_partitions_copy,
                        out_eps,
                        delete_out_spans=True
                    )
                    cnt_unassigned += int(len(assignment) == 0)
                top_assignments = []
                batch_in_spans = []
        return tw.FindAssignmentsResult(all_assignments, top_assignments, None, not_best_count)

    # Create max independent set(MIS) based on top_assignments for each incoming span
    # Each assignment consists of an ordered list of spans, starting with the incoming span and the subsequent spans are outgoing spans in order of dependence
    # For the MIS instance
    #  - add one vertex for each possible assignment
    #  - for an incoming span s, add edges between the top assignments for s (since only one of them need to be chosen)
    #  - for an assignment a1 for incoming span1 and an assignment a2 for incoming span2, add an edge between a1 and a2 if the assignments a1 and a2 intersect
    def GetAssignmentsMIS(self, top_assignments):
        mis_assignments = [[]] * len(top_assignments)
        G = self.BuildMISInstance(top_assignments)
        if len(G.nodes) > 0:
            mis = self.GetMIS(G)
        else:
            return mis_assignments
        for in_span_ind, a_ind in mis:
            score, a = top_assignments[in_span_ind][a_ind]
            mis_assignments[in_span_ind] = a
        return mis_assignments

    def BuildMISInstance(self, top_assignments):
        G = nx.Graph()
        for ind1 in range(len(top_assignments)):
            for i1 in range(len(top_assignments[ind1])):
                aid1 = (ind1, i1)
                score = top_assignments[ind1][i1][0]
                G.add_node(aid1, weight=10000.0 + score)
                # add edges from previous assignments for the same incoming span
                for i0 in range(0, i1):
                    aid0 = (ind1, i0)
                    G.add_edge(aid0, aid1)
                # add edges from previous intersecting assignments for previous incoming spans
                for ind0 in range(0, ind1):
                    for i0 in range(len(top_assignments[ind0])):
                        if self.AssignmentIntersect(
                                top_assignments[ind0][i0][1],
                                top_assignments[ind1][i1][1],
                        ):
                            aid0 = (ind0, i0)
                            G.add_edge(aid0, aid1)
        return G

    # 判断 span 序列（也就是 assignment）相交
    def AssignmentIntersect(self, a1, a2):
        assert len(a1) == len(a2)
        for s1, s2 in zip(a1, a2):
            if s1.GetId() == s2.GetId():
                return True
        return False

    def GetMIS(self, G):
        '''
        mis = approximation.independent_set.maximum_independent_set(G)
        return mis
        '''
        best_mis = None
        best_score = -math.inf
        # 固定的迭代次数（求全局最优解的过程是 NP-hard 的）
        for i in range(config.tw_MIS_iterations):
            mis = nx.maximal_independent_set(G)
            # score 聚合的方式是 sum，也就是对数和（也就是积）
            score = sum([G.nodes[n]['weight'] for n in mis])
            if best_mis is None or score > best_score:
                best_mis = mis
                best_score = score
        return best_mis
