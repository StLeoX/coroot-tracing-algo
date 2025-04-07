"""
TraceWeaver V2 追踪算法。
"""

import bisect
import copy
import hashlib
import heapq
import math
import random
import string
import time

import gurobi_optimods.mwis as gurobi_mwis
import networkx as nx
import numpy as np
import scipy.sparse as sp
import scipy.stats
from networkx.algorithms import approximation
from prefect import task, states
from sklearn import mixture

import src.config as config
import src.task.traceweaver as tw
import src.task.utils as utils
from src.task.dto.span import Span


# Debug 视图
VERBOSE = False


@task(log_prints=False)
def update_children(time_batch_spans):
    """
    :param time_batch_spans: 是 sid_span_map。
    """

    if len(time_batch_spans) == 0:
        return states.Failed(message="Empty time batch")

    spans = time_batch_spans.values()

    service_names = tw.GetServiceNames(spans)

    twV3 = TraceWeaverV3(spans, service_names)
    twV3.AppendSidMap(time_batch_spans)

    in_spans_by_process, out_spans_by_process = tw.AggregateSpans(spans, service_names)

    # 遍历系统中的全体 process
    for process in service_names:
        result = tw.ComputeSingleProcess(process, in_spans_by_process, out_spans_by_process, service_names,
                                         spans, time_batch_spans, twV3)
        if result is None:
            print(f"Failed to compute process {process}")
            continue
        print(f"Computed process {process}", f", `not_best_count` is {result.not_best_count}")

        # 展开 assignment 结构
        # all_assignments[ep][in_spans[ind].GetId()] = ("NA", "NA")，所以 in_span 对应 parent span，out_span 对应 child span。
        for ep, mappings in result.pred_assignments.items():
            for parent_sid, child_sid in mappings.items():
                # fixme 因为 ComputeDistParams 截断造成的 NA
                if child_sid[1] == 'NA':
                    print("[deb] Found an NA child for span:", parent_sid)
                    continue
                # utils.update_parent_mock(time_batch_spans, child_sid[1], parent_sid[1])  # 下标 0 代表 trace_id。
                utils.update_parent(time_batch_spans, child_sid[1], parent_sid[1])  # 下标 0 代表 trace_id。

    return states.Completed(message="Finished `update_children`.")


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
        # datetime 转成 float 类型进行计算，因为 datetime 不支持 add 或 sum。
        def ComputeDistParams(ep1, ep2, t1, t2):
            t1 = t1[in_span_start:in_span_end]
            t2 = t2[in_span_start:in_span_end]

            # fixme 暂时通过截断的方式处理一下 in_spans 与 out_spans “不对齐”
            if len(t1) != len(t2):
                minLen = min(len(t1), len(t2))
                t1 = t1[:minLen]
                t2 = t2[:minLen]
                print(f"Excluded {max(len(t1), len(t2)) - minLen} spans during `ComputeDistParams`")

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
                        true_assignments, call_graph):
        # FindAssignments 参数检查，判断服务依赖关系是否允许计算。
        if len(in_span_partitions) == 0:
            print(f"{process} has no upstream.")
            return None
        if len(in_span_partitions) > 1:
            print(f"{process} has those more than one upstreams, they are {in_span_partitions.keys()}.")
            return None

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
                        true_assignments, call_graph):
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
        # 用来统计每个 sid 下 candidate 的数量，暂时不去使用。
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


class TraceWeaverV3(TraceWeaverV1):
    def __init__(self, all_spans, all_processes):
        super().__init__(all_spans, all_processes)
        self.all_spans = all_spans
        self.all_processes = all_processes
        self.process = ''
        self.services_times = {}
        self.start_end = {}
        self.parallel = False
        self.normal = True
        self.instrumented_hops = []
        self.true_assignments = None
        self.distribution_values = {}
        self.distribution_values_true = {}
        # 异常 delay
        self.large_delay = None
        self.per_span_candidates = {}
        self.time_windows = []
        self.span_windows = []
        self.skip_count_per_window = {}
        self.available_skips_per_window = {}
        # 是否为 true_assignment 添加相应的 skip spans。
        self.true_skips = False
        # 是否维护 distribution_values_true 表（并列于 distribution_values）
        self.true_dist = False
        self.overall_skip_budget = {}
        self.sub_scores = {}
        self.pick_first = False
        self.dynamism = False

    def AppendSidMap(self,sid_span_map):
        self.sid_span_map=sid_span_map

    def ContainsSkip(self, assignment):
        for i in assignment:
            if i.trace_id == "None":
                return True
        return False

    def GenerateRandomID(self):
        x = "skip"+''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(12))
        return x

    def BuildTrueDistributions(self, in_span_partitions, out_span_partitions, in_eps, out_eps, true_assignments):
        for in_ep in in_eps:
            for in_span in in_span_partitions[in_ep]:
                per_ep_gt = {}
                prev_index = 0
                prev_span = None
                for depth, out_ep in enumerate(out_eps):
                    out_span_id = true_assignments[out_ep][in_span.GetId()]
                    if out_span_id[0] == "Skip":
                        if depth == len(out_eps) - 1:
                            if prev_span != None and prev_index != 0:
                                if (prev_ep, in_ep) not in self.distribution_values_true:
                                    self.distribution_values_true[(prev_ep, in_ep)] = []
                                self.distribution_values_true[(prev_ep, in_ep)].append(
                                    (in_span.start_time + in_span.duration) - (
                                            prev_span.start_time + prev_span.duration))
                    else:
                        for out_span in out_span_partitions[out_ep]:
                            if out_span.GetId() == out_span_id:
                                if prev_index == 0:
                                    if (in_ep, out_ep) not in self.distribution_values_true:
                                        self.distribution_values_true[(in_ep, out_ep)] = []
                                    self.distribution_values_true[(in_ep, out_ep)].append(
                                        out_span.start_time - in_span.start_time)
                                    prev_span = copy.deepcopy(out_span)
                                    prev_ep = copy.deepcopy(out_ep)
                                    prev_index += 1
                                else:
                                    if (prev_ep, out_ep) not in self.distribution_values_true:
                                        self.distribution_values_true[(prev_ep, out_ep)] = []
                                    self.distribution_values_true[(prev_ep, out_ep)].append(
                                        out_span.start_time - (prev_span.start_time + prev_span.duration))
                                    prev_span = copy.deepcopy(out_span)
                                    prev_ep = copy.deepcopy(out_ep)
                                    prev_index += 1

                                if depth == len(out_eps) - 1:
                                    if (prev_ep, in_ep) not in self.distribution_values_true:
                                        self.distribution_values_true[(prev_ep, in_ep)] = []
                                    self.distribution_values_true[(prev_ep, in_ep)].append(
                                        (in_span.start_time + in_span.duration) - (
                                                prev_span.start_time + prev_span.duration))

                                break

        for key in self.distribution_values_true.keys():
            self.services_times[key] = np.mean(self.distribution_values_true[key]), np.std(
                self.distribution_values_true[key])

    def BuildDistributions(self, process, in_span_partitions, out_span_partitions, in_eps, out_eps):

        spans = []
        for in_ep in in_eps:
            for span in in_span_partitions[in_ep]:
                span.ep = span.GetParentProcess(self.all_processes, self.all_spans)
            spans.extend(in_span_partitions[in_ep])
        for out_ep in out_eps:
            for span in out_span_partitions[out_ep]:
                span.ep = span.GetChildProcess(self.all_processes, self.all_spans)
            spans.extend(out_span_partitions[out_ep])
        spans.sort(key=lambda x: x.start_time)
        self.large_delay = max([span.duration for in_ep in in_eps for span in in_span_partitions[in_ep]])
        out_ep_order = {k: v for v, k in enumerate(out_eps)}

        for i, span in enumerate(spans):
            if span.span_kind == "client":
                sent_mus = span.start_time
                duration = span.duration
                parent_span = None
                parent_type = None
                for j, preceding_span in reversed(list(enumerate(spans[:i]))):
                    if (sent_mus + duration) - preceding_span.start_time > self.large_delay:
                        break
                    if preceding_span.span_kind == "server":
                        parent_span = preceding_span
                        parent_type = "server"
                        break
                    if ((preceding_span.span_kind == "client") and
                            (preceding_span.start_time + preceding_span.duration < span.start_time) and
                            (out_ep_order[preceding_span.ep] < out_ep_order[span.ep])):
                        parent_span = preceding_span
                        parent_type = "client"
                        break
                if parent_span is not None:
                    if (parent_span.ep, span.ep) not in self.distribution_values:
                        self.distribution_values[(parent_span.ep, span.ep)] = []
                    if parent_type == "server":
                        self.distribution_values[(parent_span.ep, span.ep)].append(sent_mus - parent_span.start_time)
                    elif parent_type == "client":
                        self.distribution_values[(parent_span.ep, span.ep)].append(
                            sent_mus - (parent_span.start_time + parent_span.duration))

            elif span.span_kind == "server":
                sent_mus = span.start_time
                duration = span.duration
                parent_span = None
                for j, preceding_span in reversed(list(enumerate(spans[:i]))):
                    if (sent_mus + duration) - preceding_span.start_time > self.large_delay:
                        break
                    if ((preceding_span.span_kind == "client") and
                            (
                                    preceding_span.start_time + preceding_span.duration < span.start_time + span.duration)):
                        parent_span = preceding_span
                        parent_type = "client"
                        break
                if parent_span is not None:
                    if (parent_span.ep, span.ep) not in self.distribution_values:
                        self.distribution_values[(parent_span.ep, span.ep)] = []
                    if parent_type == "client":
                        self.distribution_values[(parent_span.ep, span.ep)].append(
                            (sent_mus + duration) - (parent_span.start_time + parent_span.duration))
                if (span.ep, span.ep) not in self.distribution_values:
                    self.distribution_values[(span.ep, span.ep)] = []
                self.distribution_values[(span.ep, span.ep)].append(duration)

        for key in self.distribution_values.keys():
            self.services_times[key] = np.mean(self.distribution_values[key]), np.std(self.distribution_values[key])

    def AddToCandidatesList(self, stack):
        if (stack[0].trace_id, stack[0].span_id) not in self.per_span_candidates:
            self.per_span_candidates[(stack[0].trace_id, stack[0].span_id)] = 0

        self.per_span_candidates[(stack[0].trace_id, stack[0].span_id)] += 1

    # 针对 CG 变化的情况，指定 CG 然后计算 GMM 的 score
    def ScoreAssignmentAsPerCallGraph(self, assignment, call_graph, out_eps, sub_scores,
                                      normalized=False):

        def AllSkip2(assignment):
            for i in assignment[1:]:
                if i[1].trace_id != "None":
                    return False
            return True

        if AllSkip2(assignment):
            return 0

        def FindValidAncestor(ep):

            before_eps = call_graph.in_edges(ep)
            if len(before_eps) == 0:
                return None

            valid_spans = []
            invalid_spans = []
            for (before_ep, self_ep) in before_eps:

                ep_index = out_eps.index(before_ep)
                b_ep = assignment[ep_index + 1][0]
                b_span = assignment[ep_index + 1][1]
                assert b_ep == before_ep

                if b_span.trace_id != "None":
                    valid_spans.append((b_ep, b_span))
                else:
                    invalid_spans.append((b_ep, b_span))

            if len(valid_spans) > 0:
                return valid_spans
            else:
                next_layer_spans = []
                for (ep, span) in invalid_spans:
                    x = FindValidAncestor(ep)
                    if x != None:
                        next_layer_spans.append(x)
                return next_layer_spans

        def AlsoNonPrimaryAncestor(before_ep, current_ep):
            all_paths = list(nx.all_simple_paths(call_graph, source=before_ep, target=current_ep, cutoff=2))
            if not all_paths:
                assert False
            else:
                for i, path in enumerate(all_paths):
                    path_length = len(path) - 1
                    if path_length > 1:
                        return True
            return False

        cost = 0
        num_mappings = 0
        first_ep, first_span = assignment[0]

        assignment_without_skips = []
        for a in assignment:
            if a[1].trace_id != "None":
                assignment_without_skips.append(a)

        last_ep, last_span = max(assignment_without_skips[1:], key=lambda x: x[1].start_time + x[1].duration)

        for (current_ep, current_span) in assignment[1:]:
            before_eps = call_graph.in_edges(current_ep)

            if current_span.trace_id == "None":
                continue

            for (before_ep, self_ep) in before_eps:

                ep_index = out_eps.index(before_ep)
                b_ep = assignment[ep_index + 1][0]
                b_span = assignment[ep_index + 1][1]
                assert b_ep == before_ep

                if not AlsoNonPrimaryAncestor(before_ep, current_ep):

                    if b_span.trace_id == "None":
                        valid_spans = FindValidAncestor(b_ep)
                        if valid_spans == None:
                            sub_cost = self.GetEpPairCost(first_ep, current_ep, first_span.start_time,
                                                          current_span.start_time, normalized)
                            cost += sub_cost
                            num_mappings += 1
                        else:
                            latest = max(valid_spans, key=lambda x: x[1].start_time + x[1].duration)
                            sub_cost = self.GetEpPairCost(latest[0], current_ep, latest[1].start_time,
                                                          current_span.start_time, normalized)
                            cost += sub_cost
                            num_mappings += 1

                        continue

                    sub_cost = self.GetEpPairCost(before_ep, current_ep, b_span.start_time + b_span.duration,
                                                  current_span.start_time, normalized)
                    cost += sub_cost
                    num_mappings += 1

            if len(call_graph.in_edges(current_ep)) == 0:
                sub_cost = self.GetEpPairCost(first_ep, current_ep, first_span.start_time, current_span.start_time,
                                              normalized)
                cost += sub_cost
                num_mappings += 1

            if current_ep == last_ep:
                sub_cost = self.GetEpPairCost(current_ep, first_ep,
                                              current_span.start_time + current_span.duration,
                                              first_span.start_time + first_span.duration, normalized)
                cost += sub_cost
                num_mappings += 1

        if normalized:
            return cost / num_mappings, sub_scores
        return cost, sub_scores

    def ScoreAssignmentWithSkip(self, assignment, normalized=False):
        def AllSkip(assignment):
            for i in assignment[1:]:
                if i.trace_id != "None":
                    return False
            return True

        if AllSkip(assignment):
            return 0

        cost = 0
        num_mappings = 0
        prev_ep, prev_time = None, None

        for i in range(len(assignment) + 1):

            if i == len(assignment):
                curr_ep = assignment[0].GetParentProcess(self.all_processes, self.all_spans)
                curr_time = assignment[0].start_time + assignment[0].duration
                cost += self.GetEpPairCost(prev_ep, curr_ep, prev_time, curr_time, normalized)
            else:
                if assignment[i].trace_id != "None":
                    num_mappings += 1
                    if i != 0:
                        curr_ep = assignment[i].GetChildProcess(self.all_processes, self.all_spans)
                        curr_time = assignment[i].start_time
                        cost += self.GetEpPairCost(prev_ep, curr_ep, prev_time, curr_time, normalized)

                    prev_ep = (
                        assignment[i].GetParentProcess(self.all_processes, self.all_spans)
                        if i == 0
                        else assignment[i].GetChildProcess(self.all_processes, self.all_spans)
                    )
                    prev_time = (
                        assignment[i].start_time
                        if i == 0
                        else assignment[i].start_time + assignment[i].duration
                    )

        return cost / (num_mappings)

    def FindTopKAssignments(self, in_eps, in_span, out_eps, out_span_partitions, K, call_graph, id_x,
                            preprocess_phase=False, count_candidates_phase=True):

        def FindCutoffs(in_span, out_span_partitions, call_graph):

            cutoff_points = {}
            # Initialize cutoff points for outgoing partitions
            for ep in out_span_partitions.keys():
                cutoff_points[ep] = [len(out_span_partitions[ep]) - 1, 0]

            # Create a reverse topological order of nodes in the call graph
            reverse_top_order = list(reversed(list(nx.topological_sort(call_graph))))

            # Iterate through each node in the reverse topological order
            for node in reverse_top_order:
                # Initialize the future start time
                early_exit_time = in_span.start_time + in_span.duration

                # Iterate through each outgoing edge of the current node
                for _, neighbor in call_graph.out_edges(node):
                    # Update the future start time based on the neighbor's cutoff point
                    early_exit_time = min(early_exit_time,
                                          out_span_partitions[neighbor][cutoff_points[neighbor][1]].start_time)

                # Find the start and end indices of the subset of spans using binary search
                start_index = bisect.bisect_left(
                    out_span_partitions[node],
                    in_span.start_time,
                    key=lambda span: span.start_time
                )
                end_index = bisect.bisect_right(
                    out_span_partitions[node],
                    early_exit_time,
                    key=lambda span: span.start_time
                )

                cutoff_points[node][0] = start_index
                cutoff_points[node][1] = end_index - 1

            return cutoff_points

        cutoff_points = FindCutoffs(in_span, out_span_partitions, call_graph)

        global top_assignments
        top_assignments = []

        normalized = False
        if not preprocess_phase:
            for ep in out_eps:
                if self.overall_skip_budget[ep] > 0:
                    normalized = True
                    break

        if not preprocess_phase:
            if self.true_skips == False:
                for ep in out_eps:
                    out_span_partitions[ep].append(None)

        def DfsTraverse3(stack, call_graph):
            global top_assignments
            i = len(stack)
            if VERBOSE:
                print("DFSTraverse3", i, out_eps, len(stack))
            if i == len(out_span_partitions) + 1:
                stack2 = []
                for s in stack:
                    stack2.append(s[1])
                if count_candidates_phase:
                    self.AddToCandidatesList(stack2)
                # 含有K个元素的最小堆，存储在 top_assignments 数组占
                # min heap
                heapq.heappush(top_assignments, stack)
                if len(top_assignments) > K and K != -1:
                    heapq.heappop(top_assignments)
            else:
                ep = out_eps[i - 1]

                for x, s in enumerate(out_span_partitions[ep]):
                    if cutoff_points[ep][0] > x:
                        continue
                    if cutoff_points[ep][1] < x:
                        break
                    before_eps = call_graph.in_edges(ep)
                    candidate = True

                    if (
                            in_span.start_time > s.start_time or
                            s.start_time + s.duration > in_span.start_time + in_span.duration
                    ):
                        candidate = False
                        continue

                    b_span = "None"
                    for (before_ep, self_ep) in before_eps:

                        idx = next((i for i, (v, *_) in enumerate(stack) if v == before_ep), None)
                        assert idx != None
                        b_ep = stack[idx][0]
                        b_span = stack[idx][1]
                        assert b_ep == before_ep

                        if b_span.trace_id == "None":
                            continue

                        if (
                                b_span.start_time + b_span.duration > s.start_time
                        ):
                            candidate = False
                            continue

                    if candidate:
                        DfsTraverse3(stack + [(ep, s)], call_graph)

        def DfsTraverseX(stack, call_graph):
            global top_assignments
            done = False
            i = len(stack)
            if VERBOSE:
                print("DFSTraverseX", i, out_eps, stack)
            if i == len(out_span_partitions) + 1:
                stack2 = []
                for s in stack:
                    stack2.append(s[1])
                if count_candidates_phase:
                    self.AddToCandidatesList(stack2)
                score, self.sub_scores = self.ScoreAssignmentAsPerCallGraph(stack, call_graph, out_eps,
                                                                            self.sub_scores, normalized)
                heapq.heappush(top_assignments, (score, stack))
                if len(top_assignments) > K and K != -1:
                    heapq.heappop(top_assignments)
            else:
                ep = out_eps[i - 1]
                if self.true_skips == True and self.true_assignments[ep][in_span.GetId()][0] == "Skip":
                    new_span_id = self.GenerateRandomID()
                    skip_span = Span("None", new_span_id, "None", "None", "None", "None", "None")
                    DfsTraverseX(stack + [(ep, skip_span)], call_graph)
                else:
                    for x, s in enumerate(out_span_partitions[ep]):
                        if not self.dynamism:
                            if cutoff_points[ep][0] > x and not done:
                                continue
                            if cutoff_points[ep][1] < x and not done:
                                break
                        if self.true_skips == False and s == None:
                            skip_span = self.FetchSkipFromWindow(ep, in_span.start_time)
                            if skip_span != None:
                                DfsTraverseX(stack + [(ep, skip_span)], call_graph)
                        else:
                            before_eps = call_graph.in_edges(ep)
                            candidate = True
                            if (
                                    in_span.start_time > s.start_time or
                                    s.start_time + s.duration > in_span.start_time + in_span.duration
                            ):
                                candidate = False
                                continue
                            b_span = "None"
                            for (before_ep, self_ep) in before_eps:
                                idx = next((i for i, (v, *_) in enumerate(stack) if v == before_ep), None)
                                assert idx != None
                                b_ep = stack[idx][0]
                                b_span = stack[idx][1]
                                assert b_ep == before_ep
                                if b_span.trace_id == "None":
                                    continue
                                if (
                                        b_span.start_time + b_span.duration > s.start_time
                                ):
                                    candidate = False
                                    continue
                            if candidate:
                                if b_span == "None":
                                    b_span = stack[0][1]
                                DfsTraverseX(stack + [(ep, s)], call_graph)

        def DfsTraverse(stack, depth, l_non_skip_depth, l_start, l_duration):
            global top_assignments
            i = len(stack)
            if VERBOSE:
                print("DFSTraverse", i, out_eps, stack)
            last_span = stack[-1]
            if i == len(out_span_partitions) + 1:
                if count_candidates_phase:
                    self.AddToCandidatesList(stack)
                if self.ContainsSkip(stack):
                    score = self.ScoreAssignmentWithSkip(stack, normalized)
                elif self.parallel:
                    score = self.ScoreAssignmentParallel(stack, normalized)
                else:
                    score = self.ScoreAssignmentSequential(stack, normalized)
                heapq.heappush(top_assignments, (score, stack))
                if len(top_assignments) > K:
                    heapq.heappop(top_assignments)
            else:
                ep = out_eps[i - 1]
                if self.true_skips == True and self.true_assignments[ep][in_span.GetId()][0] == "Skip":
                    new_span_id = self.GenerateRandomID()
                    skip_span = Span(
                        "None",
                        new_span_id,
                        "None",
                        "None",
                        "None",
                        "None",
                        "None",
                    )
                    DfsTraverse(stack + [skip_span], depth + 1, l_non_skip_depth, last_span.start_time,
                                last_span.duration)
                else:
                    for x, s in enumerate(out_span_partitions[ep]):
                        if self.true_skips == False and s == None:
                            skip_span = self.FetchSkipFromWindow(ep, in_span.start_time)
                            if skip_span != None:
                                DfsTraverse(stack + [skip_span], depth + 1, l_non_skip_depth, last_span.start_time,
                                            last_span.duration)
                        else:
                            # parallel eps
                            if self.parallel:
                                if (
                                        in_span.start_time <= s.start_time
                                        and s.start_time + s.duration
                                        <= in_span.start_time + in_span.duration
                                ):
                                    DfsTraverse(stack + [s], depth + 1, l_non_skip_depth + 1, None, None)

                            # Sequential eps
                            else:
                                if last_span.trace_id == "None":
                                    if (
                                            l_non_skip_depth == 1
                                            and in_span.start_time <= s.start_time
                                            and s.start_time + s.duration
                                            <= in_span.start_time + in_span.duration
                                    ):
                                        DfsTraverse(stack + [s], depth + 1, l_non_skip_depth + 1, None, None)
                                    # all other eps
                                    elif (
                                            l_non_skip_depth <= len(out_eps)
                                            and l_start + l_duration <= s.start_time
                                            and s.start_time + s.duration
                                            <= in_span.start_time + in_span.duration
                                    ):
                                        DfsTraverse(stack + [s], depth + 1, l_non_skip_depth + 1, None, None)
                                else:
                                    # first ep
                                    if (
                                            i == 1
                                            and in_span.start_time <= s.start_time
                                            and s.start_time + s.duration
                                            <= in_span.start_time + in_span.duration
                                    ):
                                        DfsTraverse(stack + [s], depth + 1, l_non_skip_depth + 1, None, None)
                                    # all other eps
                                    elif (
                                            i <= len(out_eps)
                                            and last_span.start_time + last_span.duration <= s.start_time
                                            and s.start_time + s.duration
                                            <= in_span.start_time + in_span.duration
                                    ):
                                        DfsTraverse(stack + [s], depth + 1, l_non_skip_depth + 1, None, None)

        if preprocess_phase:
            in_ep = in_eps[0]
            DfsTraverse3([(in_ep, in_span)], call_graph)
            top_assignments.sort(reverse=True)
            return top_assignments

        else:

            if self.parallel:
                DfsTraverse([in_span], 1, 1, None, None)
                top_assignments.sort(reverse=True)
                if self.true_skips == False:
                    for ep in out_eps:
                        out_span_partitions[ep].pop()
                return top_assignments

            # fixme 从 `in_eps` 的变量命名可以看出来，设计上是想做多个上游服务的，但实现上只用了 in_eps[0]，所以 FindAssignments 参数检查依然生效。
            in_ep = in_eps[0]
            DfsTraverseX([(in_ep, in_span)], call_graph)
            top_assignments2 = []
            for assignment in top_assignments:
                s_assignment = (assignment[0], [s[1] for s in assignment[1]])
                top_assignments2.append(s_assignment)
            top_assignments2.sort(reverse=True)
            if self.true_skips == False:
                for ep in out_eps:
                    out_span_partitions[ep].pop()
            return top_assignments2

    def GetSpanIDNotation(self, out_eps, assignment, type1):
        span_id_notation = []

        if type1:
            for i in range(1, len(assignment)):
                span_id_notation.append(assignment[i].GetId())
        else:
            for out_ep in out_eps:
                span_id_notation.append(assignment[out_ep].GetId())
        return span_id_notation

    def ComputeEpPairDistParams(
            self,
            in_span_partitions,
            out_span_partitions,
            out_eps,
            in_span_start,
            in_span_end,
    ):
        def ComputeDistParams(ep1, ep2, t1, t2):
            t1 = t1[in_span_start:in_span_end]
            t2 = t2[in_span_start:in_span_end]

            # fixme 暂时通过截断的方式处理一下 in_spans 与 out_spans “不对齐”
            if len(t1) != len(t2):
                minLen = min(len(t1), len(t2))
                t1 = t1[:minLen]
                t2 = t2[:minLen]
                if VERBOSE:
                    print(f"Excluded {max(len(t1), len(t2)) - minLen} spans during `ComputeDistParams`")

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
                    [s.start_time + s.duration for s in out_span_partitions[ep1]]
                )
                t2 = sorted([s.start_time for s in out_span_partitions[ep2]])
                ComputeDistParams(ep1, ep2, t1, t2)

            # between last outgoing -- incoming
            ep1 = out_eps[-1]
            ep2 = list(in_span_partitions.keys())[0]
            t1 = sorted([s.start_time + s.duration for s in out_span_partitions[ep1]])
            t2 = sorted([s.start_time + s.duration for s in in_span_partitions[ep2]])
            ComputeDistParams(ep1, ep2, t1, t2)

    def ComputeEpPairDistParams2(
            self,
            in_span_partitions,
            out_span_partitions,
            out_eps,
            in_span_start,
            in_span_end,
    ):

        def SetStartEnd(ep1, ep2, t1, t2):
            if in_span_start == 0:
                out_span_start = 0
            else:
                out_span_start = self.start_stop[(ep1, ep2)][3] + 1

            global in_span_end
            t1 = t1[in_span_start:in_span_end]
            t1_sorted_finish_times = sorted([s.start_time + s.duration for s in t1])
            last_span = t1_sorted_finish_times[-1]

            out_span_end = None
            for i, span in enumerate(t2[out_span_start:]):
                if span.start_time + span.duration > last_span.start_time + last_span.duration:
                    break
                else:
                    out_span_end = i
            if out_span_end != None:
                t2 = t2[out_span_start:out_span_end]
            else:
                assert (False)

            x = in_span_end - in_span_start
            y = out_span_end - out_span_start
            diff = x - y
            if diff > 0:
                in_span_end -= diff
            else:
                out_span_end += diff
            self.start_end[(ep1, ep2)] = [in_span_start, in_span_end, out_span_start, out_span_end]

    def ComputeEpPairDistParams3(
            self,
            in_span_partitions,
            out_span_partitions,
            out_eps,
            in_span_start,
            in_span_end,
            call_graph
    ):

        def ComputeDistParams(ep1, ep2, t1, t2):
            t1 = t1[in_span_start:in_span_end]
            t2 = t2[in_span_start:in_span_end]
            assert len(t1) == len(t2)
            mean = (sum(t2) - sum(t1)) / len(t1)
            if len(t1) == 0:
                print("len(t1)")
                input()
            batch_means = []
            nbatches = 10
            batch_size = math.ceil(float(len(t1)) / nbatches)
            if nbatches == 0:
                print("nbatches")
                input()
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
            self.services_times[(ep1, ep2)] = mean, std

        in_ep = list(in_span_partitions.keys())[0]

        for out_ep in out_span_partitions.keys():

            if len(call_graph.in_edges(out_ep)) == 0:
                t1 = sorted([s.start_time for s in in_span_partitions[in_ep]])
                t2 = sorted([s.start_time for s in out_span_partitions[out_ep]])
                ComputeDistParams(in_ep, out_ep, t1, t2)

            before_eps = call_graph.in_edges(out_ep)

            for (before_ep, self_ep) in before_eps:

                if not self.AlsoNonPrimaryAncestor(before_ep, self_ep, call_graph):

                    if before_ep == in_ep:
                        t1 = sorted([s.start_time for s in in_span_partitions[before_ep]])
                        t2 = sorted([s.start_time for s in out_span_partitions[self_ep]])
                        ComputeDistParams(before_ep, self_ep, t1, t2)

                    else:
                        t1 = sorted([s.start_time + s.duration for s in out_span_partitions[before_ep]])
                        t2 = sorted([s.start_time for s in out_span_partitions[self_ep]])
                        ComputeDistParams(before_ep, self_ep, t1, t2)

            t1 = sorted([s.start_time + s.duration for s in out_span_partitions[out_ep]])
            t2 = sorted([s.start_time + s.duration for s in in_span_partitions[in_ep]])
            ComputeDistParams(out_ep, in_ep, t1, t2)

    def ComputeEpPairDistParams4(
            self,
            in_span_partitions,
            out_span_partitions,
            out_eps,
            in_span_start,
            in_span_end,
            call_graph
    ):

        def ComputeDistParams(ep1, ep2, t1, t2):
            t1 = t1[in_span_start:in_span_end]
            t2 = t2[in_span_start:in_span_end]
            assert len(t1) == len(t2)
            batch_means = []
            batch_size = 50
            nbatches = math.ceil(float(len(t1)) / batch_size)
            if nbatches == 0:
                print("no batches")
                input()
            for i in range(nbatches):
                start = i * batch_size
                end = min(len(t1), (i + 1) * batch_size)
                if end - start > 0:
                    batch_means.append(
                        (sum(t2[start:end]) - sum(t1[start:end])) / (end - start)
                    )
            self.services_times[(ep1, ep2)] = batch_means

        in_ep = list(in_span_partitions.keys())[0]

        for out_ep in out_span_partitions.keys():

            if len(call_graph.in_edges(out_ep)) == 0:
                t1 = sorted([s.start_time for s in in_span_partitions[in_ep]])
                t2 = sorted([s.start_time for s in out_span_partitions[out_ep]])
                ComputeDistParams(in_ep, out_ep, t1, t2)

            before_eps = call_graph.in_edges(out_ep)

            for (before_ep, self_ep) in before_eps:

                if not self.AlsoNonPrimaryAncestor(before_ep, self_ep, call_graph):

                    if before_ep == in_ep:
                        t1 = sorted([s.start_time for s in in_span_partitions[before_ep]])
                        t2 = sorted([s.start_time for s in out_span_partitions[self_ep]])
                        ComputeDistParams(before_ep, self_ep, t1, t2)

                    else:
                        t1 = sorted([s.start_time + s.duration for s in out_span_partitions[before_ep]])
                        t2 = sorted([s.start_time for s in out_span_partitions[self_ep]])
                        ComputeDistParams(before_ep, self_ep, t1, t2)

            t1 = sorted([s.start_time + s.duration for s in out_span_partitions[out_ep]])
            t2 = sorted([s.start_time + s.duration for s in in_span_partitions[in_ep]])
            ComputeDistParams(out_ep, in_ep, t1, t2)

    def ComputeEpPairDistParams5(
            self,
            in_span_partitions,
            out_span_partitions,
            call_graph,
            all_assignments,
            true_assignments
    ):
        true_durations = []
        assignments = [true_assignments, all_assignments]

        def ComputeDistParams(ep1, ep2, mapping_type, in_ep, assignments, iteration):

            global true_durations

            durations = []
            if mapping_type == 1:
                for in_span in in_span_partitions[ep1]:
                    # edge service
                    if ep2 not in assignments:
                        continue
                    out_span_id = assignments[ep2][in_span.GetId()]
                    if out_span_id == ("NA", "NA") or out_span_id == ('Skip', 'Skip'):
                        continue
                    else:
                        if VERBOSE:
                            print(out_span_id)
                        out_span = self.sid_span_map[out_span_id[1]]
                        durations.append(out_span.start_time - in_span.start_time)

            elif mapping_type == 2:
                for in_span in in_span_partitions[in_ep]:
                    t1 = None
                    t2 = None

                    out_span_id_1 = assignments[ep1][in_span.GetId()]
                    if out_span_id_1 == ("NA", "NA") or out_span_id_1 == ('Skip', 'Skip'):
                        continue
                    else:
                        out_span_1 = self.all_spans[out_span_id_1]
                        t1 = out_span_1.start_time + out_span_1.duration

                    out_span_id_2 = assignments[ep2][in_span.GetId()]
                    if out_span_id_2 == ("NA", "NA") or out_span_id_2 == ('Skip', 'Skip'):
                        continue
                    else:
                        out_span_2 = self.all_spans[out_span_id_2]
                        t2 = out_span_2.start_time

                    if t1 != None and t2 != None:
                        durations.append(t2 - t1)

            elif mapping_type == 3:
                for in_span in in_span_partitions[ep2]:
                    # edge service
                    if ep1 not in assignments:
                        continue
                    out_span_id = assignments[ep1][in_span.GetId()]
                    if out_span_id == ("NA", "NA") or out_span_id == ('Skip', 'Skip'):
                        continue
                    else:
                        out_span = self.sid_span_map[out_span_id[1]]
                        durations.append(
                            (in_span.start_time + in_span.duration) - (out_span.start_time + out_span.duration))

            self.services_times[(ep1, ep2)] = durations

            durations = np.array(durations).reshape(-1, 1)
            if len(durations) == 0:
                self.services_times[(ep1, ep2)] = (0, 0)
            else:
                max_n = min(len(np.unique(durations)), 5)
                n_components = np.arange(1, max_n + 1)
                models = []
                n_comps = []
                for n in n_components:
                    try:
                        model = mixture.GaussianMixture(n_components=n, covariance_type='diag').fit(durations)
                        models.append(model)
                        n_comps.append(n)
                    except ValueError as e:
                        print(f"Failed to fit GMM with {n} components: {e}")
                        continue
                n_selected = n_comps[np.argmin([m.bic(durations) for m in models])]
                if VERBOSE:
                    print("Edge:", ep1, ep2)
                    print("No. of Gaussians selected: ", n_selected)

                g = mixture.GaussianMixture(n_components=n_selected, random_state=100)
                g.fit(durations)
                self.services_times[(ep1, ep2)] = g

                if ep1 == "client_ComposeReview" and ep2 == "movie-id-service" and iteration == 0:
                    true_durations = durations
                elif ep1 == "client_ComposeReview" and ep2 == "movie-id-service" and iteration == 1 and true_durations != []:
                    print("Self score: ", scipy.stats.wasserstein_distance(true_durations, true_durations))
                    print("Score: ", scipy.stats.wasserstein_distance(true_durations, durations))
                    t_statistic, p_value = scipy.stats.ttest_ind(true_durations, true_durations)
                    print("t-statistic: ", t_statistic, "p-value: ", p_value)

        for i in range(2):

            if VERBOSE:
                print("STARTING ITERATION: ", i+1)

            in_ep = list(in_span_partitions.keys())[0]
            for out_ep in out_span_partitions.keys():

                if len(call_graph.in_edges(out_ep)) == 0:
                    ComputeDistParams(in_ep, out_ep, 1, in_ep, assignments[i], i)

                before_eps = call_graph.in_edges(out_ep)

                for (before_ep, self_ep) in before_eps:

                    if not self.AlsoNonPrimaryAncestor(before_ep, self_ep, call_graph):

                        if before_ep == in_ep:
                            ComputeDistParams(before_ep, self_ep, 1, in_ep, assignments[i], i)

                        else:
                            ComputeDistParams(before_ep, self_ep, 2, in_ep, assignments[i], i)

                ComputeDistParams(out_ep, in_ep, 3, in_ep, assignments[i], i)

    def FetchSkipFromWindow(
            self,
            ep,
            start_time
    ):

        def FindWindow(key, windows):
            return windows.index(max(i for i in windows if i <= key))

        self.time_windows.sort(key=lambda x: x[0])
        index = FindWindow(start_time, [x[0] for x in self.time_windows])
        if index == None:
            assert False
        window = self.time_windows[index][:2]

        if len(self.available_skips_per_window[ep][window]) <= 0:
            return None

        minval = min(self.available_skips_per_window[ep][window], key=lambda x: x[1])
        pos = self.available_skips_per_window[ep][window].index(minval)
        self.available_skips_per_window[ep][window][pos][1] += 1

        if VERBOSE:
            print("[deb] Used skip", self.available_skips_per_window[ep][window][pos][0])
        return self.available_skips_per_window[ep][window][pos][0]

    def DetectBoundaries(
            self,
            in_span_partitions,
            out_span_partitions,
            in_eps,
            out_eps
    ):
        pass

    def TallySkipSpans(
            self,
            in_span_partitions,
            out_span_partitions,
            in_eps,
            out_eps,
            batch_size_mis
    ):

        def WaterFill(window_diffs, window_counts, skip_budget, ep):

            if skip_budget <= 0:
                return

            num_windows = len(window_diffs)
            window_keys = copy.deepcopy(sorted(self.time_windows, key=lambda x: x[0]))

            index_to_key = {}
            for i, window_key in enumerate(window_keys):
                index_to_key[i] = window_key[:2]

            existing_spans = np.zeros(len(window_counts))
            expected_spans = np.zeros(len(window_counts))
            for i in range(len(window_counts)):
                existing_spans[i] = copy.deepcopy((window_counts[index_to_key[i]]))
                expected_spans[i] = copy.deepcopy((window_keys[i][2]))

            # Sort the windows in decreasing order of existing spans
            sorted_indices = np.argsort(existing_spans)[::-1]
            sorted_existing_spans = existing_spans[sorted_indices]

            # Initialize resource allocation vector
            skip_allocation = np.zeros(num_windows)

            # Calculate the max window span count (i.e., water level in waterfilling)
            lambda_ = 0

            for i in range(num_windows):
                lambda_ = (skip_budget + np.sum(sorted_existing_spans[:i + 1])) // (i + 1)
                total_remaining = (skip_budget + np.sum(sorted_existing_spans[:i + 1])) % (i + 1)
                if lambda_ <= sorted_existing_spans[i]:
                    break

            # Allocate additional resources to windows based on water-filling
            remaining = 0
            for i in range(num_windows):
                remaining += max(lambda_ - sorted_existing_spans[i], 0) - min(
                    max(lambda_ - sorted_existing_spans[i], 0), expected_spans[i] - sorted_existing_spans[i])
                skip_allocation[sorted_indices[i]] = min(max(lambda_ - sorted_existing_spans[i], 0),
                                                         expected_spans[i] - sorted_existing_spans[i])
            total_remaining += remaining

            while total_remaining > 0:
                no_change = True
                for i in reversed(range(num_windows)):
                    if total_remaining > 0 and skip_allocation[sorted_indices[i]] < (
                            expected_spans[i] - sorted_existing_spans[i]):
                        skip_allocation[sorted_indices[i]] += 1
                        no_change = False
                        total_remaining -= 1
                if no_change:
                    break

            for i in range(len(window_counts)):
                self.skip_count_per_window[ep][index_to_key[i]] = skip_allocation[i]

            return

        def TackleMismatch(ep):

            # skip 填充量预算
            skip_budget = self.overall_skip_budget[ep]

            self.skip_count_per_window[ep] = {}
            self.available_skips_per_window[ep] = {}
            window_counts = {}
            window_diffs = {}

            for (window_start, window_end, expected_count) in self.time_windows:

                if (window_start, window_end) not in self.skip_count_per_window[ep]:
                    self.skip_count_per_window[ep][(window_start, window_end)] = 0

                count = 0
                # TODO: calculate mean_span_time per window
                mean_span_time = np.mean([i.duration for i in out_span_partitions[ep]])
                for span in out_span_partitions[ep]:
                    if span.start_time > window_start and span.start_time <= window_end:
                        count += 1
                window_diffs[(window_start, window_end)] = max(expected_count - count, 0)
                window_counts[(window_start, window_end)] = count

            skip_budget_copy = skip_budget

            WaterFill(window_diffs, window_counts, skip_budget_copy, ep)

            for (window_start, window_end, _) in self.time_windows:

                if (window_start, window_end) not in self.available_skips_per_window[ep]:
                    self.available_skips_per_window[ep][(window_start, window_end)] = []

                # 构造 skip 跨度
                # 可以看到，skip 是通过 time_window 进行索引的（保存在 available_skips_per_window），所以并没有准确的 timestamp。
                # 因为 skip 的 timestamp、duration 都是空的，所以它并不参与 dist 计算？因此在 ComputeDistParams 直接截断就可以了？
                for i in range(int(self.skip_count_per_window[ep][(window_start, window_end)])):
                    new_span_id = self.GenerateRandomID()
                    skip_span = Span(
                        "None",
                        new_span_id,
                        "None",
                        "None",
                        "None",
                        "None",
                        "None",
                    )
                    # todo skip 是如何加入到 out_spans 当中的？
                    # 其实并没有直接添加到原始的、真实的 out_spans，而是在 Traverse 过程中被使用，作为连接真实 span 的中间 span。
                    self.available_skips_per_window[ep][(window_start, window_end)].append([skip_span, 0])

        self.skip_count_per_window = {}

        for ep in in_eps:
            in_span_partitions[ep].sort(key=lambda x: float(x.start_time))
        for ep in out_eps:
            out_span_partitions[ep].sort(key=lambda x: float(x.start_time))
            self.overall_skip_budget[ep] = len(in_span_partitions[in_eps[0]]) - len(out_span_partitions[ep])

        window_start = in_span_partitions[in_eps[0]][0].start_time
        final_span = sorted(in_span_partitions[in_eps[0]], key=lambda x: float(x.start_time) + float(x.duration))[-1]
        final_window_end = final_span.start_time + final_span.duration

        len_spans = len(in_span_partitions[in_eps[0]])
        for i in range(0, len_spans):
            if (i != 0 and i != len_spans - 1 and i % batch_size_mis == 0):
                window_end = in_span_partitions[in_eps[0]][i].start_time + in_span_partitions[in_eps[0]][i].duration
                self.time_windows.append((window_start, window_end, batch_size_mis))
                window_start = window_end
            elif i == len_spans - 1:
                window_end = final_window_end
                self.time_windows.append((window_start, window_end, batch_size_mis))

        for ep in out_eps:
            TackleMismatch(ep)

    def CreateWindows(self, in_span_partitions, in_eps, max_size, threshold):

        windows = []
        current_count = 1

        for i, span in enumerate(in_span_partitions[in_eps[0]]):

            if i != 0:
                if i == len(in_span_partitions[in_eps[0]]) - 1:
                    current_count = 0
                    window_end = i
                    windows.append((window_start, window_end))
                elif (in_span_partitions[in_eps[0]][i + 1].start_time - span.start_time) > threshold:
                    current_count = 0
                    window_end = i
                    windows.append((window_start, window_end))
                    window_start = i + 1
                elif current_count == max_size:
                    current_count = 0
                    window_end = i
                    windows.append((window_start, window_end))
                    window_start = i + 1
            else:
                window_start = i

            current_count += 1

        return windows

    def CreateWindows2(self, in_span_partitions, in_eps, out_span_partitions, out_eps, call_graph, max_size):

        prev_index = 0

        def PerfectCut(i):
            global prev_index
            if i == 1:
                prev_index = 0
            else:
                if ((in_span_partitions[in_eps[0]][i - 1].start_time + in_span_partitions[in_eps[0]][
                    i - 1].duration) >=
                        (in_span_partitions[in_eps[0]][prev_index].start_time + in_span_partitions[in_eps[0]][
                            prev_index].duration)
                ):
                    prev_index = i - 1

            condition1 = (set(candidates_array[prev_index]).isdisjoint(candidates_array[i]))
            condition2 = ((in_span_partitions[in_eps[0]][prev_index].start_time + in_span_partitions[in_eps[0]][
                prev_index].duration
                           <= in_span_partitions[in_eps[0]][i].start_time + in_span_partitions[in_eps[0]][
                               i].duration)
            )

            return condition1 and condition2

        candidates_array = []

        for i, in_span in enumerate(in_span_partitions[in_eps[0]]):

            candidates = self.FindTopKAssignments(in_eps, in_span, out_eps, out_span_partitions, -1, call_graph,
                                                  i, True, False)
            candidates_array.append([])

            for candidate in candidates:
                for span in candidate[1:]:
                    candidates_array[i].append(span[1].GetId())

        windows = []
        current_count = 1

        for i, in_span in enumerate(in_span_partitions[in_eps[0]]):

            if i != 0:
                if i == len(in_span_partitions[in_eps[0]]) - 1:
                    current_count = 0
                    window_end = i
                    windows.append((window_start, window_end))
                elif PerfectCut(i):
                    current_count = 0
                    window_end = i - 1
                    windows.append((window_start, window_end))
                    window_start = i
                elif current_count == max_size:
                    current_count = 0
                    window_end = i
                    windows.append((window_start, window_end))
                    window_start = i + 1
            else:
                window_start = i

            current_count += 1

        return windows

    def CreateWindows3(self, in_span_partitions, in_eps, max_size):
        window_ends = []
        for start in range(0, len(in_span_partitions[in_eps[0]]), max_size):
            end = min(start + max_size - 1, len(in_span_partitions[in_eps[0]]) - 1)
            window_ends.append((start, end))
        return window_ends

    def FindAssignments(self, process, in_span_partitions, out_span_partitions, parallel, instrumented_hops,
                        true_assignments, call_graph):
        # FindAssignments 参数检查，判断服务依赖关系是否允许计算。
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
        # 用来统计每个 sid 下 candidate 的数量，暂时不去使用。
        self.per_span_candidates = {}
        span_to_top_assignments = {}
        in_eps, in_spans = list(in_span_partitions.items())[0]
        in_eps = [in_eps] if isinstance(in_eps, str) else in_eps
        out_eps = self.GetOutEpsInOrder(out_span_partitions, call_graph)
        out_span_partitions_copy = copy.deepcopy(out_span_partitions)
        out_span_partitions_copy_2 = copy.deepcopy(out_span_partitions)
        sorted_durations = [i.duration for i in sorted(in_span_partitions[in_eps[0]], key=lambda s: s.duration)]

        batch_size = config.tw_batch_size
        batch_size_mis = config.tw_batch_size_mis
        topK = config.tw_top_size
        self.normal = True
        # if method == "MaxScoreBatchParallelWithoutPerfectCuts":
        #     self.span_windows = self.CreateWindows3(in_span_partitions, in_eps, 10)
        #     window_ends = [i[1] for i in self.span_windows]
        # else:
        self.span_windows = self.CreateWindows2(in_span_partitions, in_eps, out_span_partitions, out_eps,
                                                call_graph, batch_size_mis)
        window_ends = [i[1] for i in self.span_windows]

        if VERBOSE:
            print("Len(window ends): ", len(window_ends))
            print("Max batch size: ", max([x[1] - x[0] for x in self.span_windows]))
        cnt = 0
        cnt_unassigned = 0
        not_best_count = 0
        all_assignments = {}
        all_topk_assignments = {}
        top_assignments = []
        batch_in_spans = []
        self.sub_scores = {}

        # count = 0
        # for span in in_span_partitions[in_eps[0]]:
        #     for ep in out_eps:
        #         if self.true_assignments[ep][span.GetId()][0] == "Skip":
        #             count += 1
        # print("True skips: ", count, "\n")

        # Tally：处理
        self.TallySkipSpans(in_span_partitions, out_span_partitions, in_eps, out_eps, batch_size_mis)

        equal_eps = []
        for ep in out_eps:
            if VERBOSE:
                print("Current process:",process + ", ", "Out endpoint:", ep + ", ", "Count of spans:", len(out_span_partitions[ep]))
            if self.overall_skip_budget[ep] == 0:
                equal_eps.append(ep)
            else:
                self.dynamism = True

        if self.true_dist:
            self.BuildTrueDistributions(in_span_partitions, out_span_partitions, in_eps, out_eps, true_assignments)
        else:
            pass # always this way
            # self.BuildDistributions(process, in_span_partitions, out_span_partitions, in_eps, out_eps)

        # if method == "MaxScoreBatchParallelWithoutIterations":
        #     self.parallel = True
        #     iterations = 1
        if len(equal_eps) != len(out_eps):
            iterations = 1
        else:
            iterations = 2

        # hint iterations 取 1 和 2 的区别：
        # ComputeEpPairDistParams5 中有个 assignments = [true_assignments, all_assignments]，会用 iterations 作为下标。

        for iteration in range(iterations):
            start_time = time.time()
            if VERBOSE:
                print("iteration: ", iteration)
            cnt = 0
            cnt_unassigned = 0
            not_best_count = 0
            all_assignments = {}
            all_topk_assignments = {}
            top_assignments = []
            batch_in_spans = []
            self.sub_scores = {}
            sum_t = 0
            out_span_partitions_copy = copy.deepcopy(out_span_partitions_copy_2)
            for id_x, in_span in enumerate(in_spans):
                if cnt % batch_size == 0:
                    if iteration == 0:
                        if self.parallel:
                            self.ComputeEpPairDistParams(in_span_partitions, out_span_partitions, out_eps, cnt,
                                                         min(len(in_spans), cnt + batch_size))
                        if len(equal_eps) == len(out_eps):
                            self.ComputeEpPairDistParams3(in_span_partitions, out_span_partitions, out_eps, cnt,
                                                          min(len(in_spans), cnt + batch_size), call_graph)
                    if VERBOSE:
                        print("V3 finished %d spans, unassigned spans: %d" % (cnt, cnt_unassigned))

                start_t = time.time()
                top_k = self.FindTopKAssignments(in_eps, in_span, out_eps, out_span_partitions_copy, topK,
                                                 call_graph, id_x, False, True)
                stop_t = time.time()
                sum_t = sum_t + (stop_t - start_t)
                top_k_2 = self.FindTopKAssignments(in_eps, in_span, out_eps, out_span_partitions, topK,
                                                   call_graph, id_x, False, False)
                self.AddTopKAssignments(in_span, top_k_2, all_topk_assignments, out_span_partitions_copy, out_eps,
                                        skips=True)
                span_to_top_assignments[in_span] = top_k
                top_assignments.append(top_k)
                batch_in_spans.append(in_span)
                cnt += 1

                if (cnt - 1) in window_ends:
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
                            delete_out_spans=True,
                            skips=True
                        )
                        cnt_unassigned += int(len(assignment) == 0)
                    top_assignments = []
                    batch_in_spans = []

            if iterations > 1:
                self.ComputeEpPairDistParams5(in_span_partitions, out_span_partitions, call_graph,
                                              all_assignments, true_assignments)
            acc = tw.AccuracyForService(all_assignments, true_assignments, in_span_partitions)

            print("Accuracy at iteration %d for process %s: %.2f" % (iteration, process, acc * 100))
            # print("Iteration time: %.2f seconds" % (time.time() - start_time))
            # print("Candidate Finder Time: %.2f seconds" % sum_t)

        return tw.FindAssignmentsResult(all_assignments, all_topk_assignments, None, not_best_count)

    # Create max independent set(MIS) based on top_assignments for each incoming span
    # Each assignment consists of an ordered list of spans, starting with the incoming span and the subsequent spans are outgoing spans in order of dependence
    # For the MIS instance
    #  - add one vertex for each possible assignment
    #  - for an incoming span s, add edges between the top assignments for s (since only one of them need to be chosen)
    #  - for an assignment a1 for incoming span1 and an assignment a2 for incoming span2, add an edge between a1 and a2 if the assignments a1 and a2 intersect
    def GetAssignmentsMIS(self, top_assignments):
        mis_assignments = [[]] * len(top_assignments)
        G = self.BuildMISInstance(top_assignments)
        if len(G.nodes) != 0:
            mis = self.Gurobi_MIS(G)
            for in_span_ind, a_ind in mis:
                score, a = top_assignments[in_span_ind][a_ind]
                mis_assignments[in_span_ind] = a
        return mis_assignments

    def generate_candidate_id(self, candidate):
        sid_list = [span.trace_id + span.sid for span in candidate]
        concatenated_sids = ''.join(map(str, sid_list))
        return hashlib.md5(concatenated_sids.encode()).hexdigest()

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
        for i in range(config.tw_MIS_iterations):
            try:
                mis = nx.maximal_independent_set(G)  # 20000 iterations
            except:
                assert False
            score = sum([G.nodes[n]['weight'] for n in mis])
            if best_mis is None or score > best_score:
                best_mis = mis
                best_score = score
        return best_mis

    def GetWeightedMIS(self, G, weight):
        vcover = approximation.min_weighted_vertex_cover(G, weight=weight)
        return set(G.nodes()).difference(set(vcover))

    def exact_MWIS(self, graph, pi, b_score=0):
        ''' compute maximum weighted independent set (recursively) using python
        networkx package. Input items are:
        - graph, a networkx graph
        - pi, a dictionary of dual values attached to node (primal constraints)
        - b_score, a bestscore (if non 0, it pruned some final branches)
        It returns:
        - mwis_set, a MWIS as a sorted tuple of nodes
        - mwis_weight, the sum over n in mwis_set of pi[n]'''
        global best_score
        graph_copy = graph.copy()
        # mwis weight is stored as a 'score' graph attribute
        graph_copy.graph['score'] = 0
        best_score = b_score

        def get_mwis(G):
            '''
            Based on "A column generation approach for graph coloring" from
            Mehrotra and Trick, 1995'''
            global best_score
            # score stores the best score along the path explored so far
            key = tuple(sorted(G.nodes()))
            ub = sum(pi[n] for n in G.nodes())
            score = G.graph['score']
            # if graph is composed of singletons, leave now
            if G.number_of_edges == 0:
                if score + ub > best_score + config.tw_MIS_score_epsilon:
                    best_score = score + ub
                return key, ub
            # compute highest priority node (used in recursion to choose {i})
            node_iter = ((n, deg * pi[n]) for (n, deg) in G.degree())
            node_chosen, _ = max(node_iter, key=lambda x: x[1])
            pi_chosen = pi[node_chosen]
            node_chosen_neighbors = list(G[node_chosen])
            pi_neighbors = sum(pi[n] for n in node_chosen_neighbors)
            G.remove_node(node_chosen)
            # Gh = G - {node_chosen} union {anti-neighbors{node-chosen}}
            # For Gh, ub decreases by value of pi over neighbors of {node_chosen}
            # and value of pi over {node_chosen} as node_chosen is disconnected
            # For Gh, score increases by value of pi over {node_chosen}
            Gh = G.copy()
            Gh.remove_nodes_from(node_chosen_neighbors)
            mwis_set_h, mwis_weight_h = tuple(), 0
            if Gh:
                ubh = ub - pi_neighbors - pi_chosen
                if score + pi_chosen + ubh > best_score + config.tw_MIS_score_epsilon:
                    Gh.graph['score'] += pi_chosen
                    mwis_set_h, mwis_weight_h = get_mwis(Gh)
                del Gh
            mwis_set_h += (node_chosen,)
            mwis_weight_h += pi_chosen
            # Gp = G - {node_chosen}
            # For Gp, ub decreases by value of pi over {node_chosen}
            # For Gh, score does not increase
            mwis_set_p, mwis_weight_p = tuple(), 0
            if G:
                ubp = ub - pi_chosen
                if score + ubp > best_score + config.tw_MIS_score_epsilon:
                    mwis_set_p, mwis_weight_p = get_mwis(G)
                del G
            # select case with maximum score
            if mwis_set_p and mwis_weight_p > mwis_weight_h + config.tw_MIS_score_epsilon:
                mwis_set, mwis_weight = mwis_set_p, mwis_weight_p
            else:
                mwis_set, mwis_weight = mwis_set_h, mwis_weight_h
            # increase score
            score += mwis_weight
            if score > best_score + config.tw_MIS_score_epsilon:
                best_score = score
            # return set and weight
            key = tuple(sorted(mwis_set))
            return key, mwis_weight

        best_mis = None
        best_score_1 = -math.inf

        for i in range(1):
            try:
                mis, w = get_mwis(copy.deepcopy(graph_copy))
            except:
                assert False
            score = sum([graph_copy.nodes[n]['weight'] for n in mis])
            # score = len(mis)
            if best_mis is None or score > best_score_1:
                best_mis = mis
                best_score_1 = score

        return best_mis
        # return get_mwis(graph_copy)

    def Gurobi_MIS(self, G):

        '''
        mwis = gurobi_optimods.mwis.maximum_weighted_independent_set(adjacency_matrix, weights)
        return mwis
        '''
        best_mwis = None
        best_score = -math.inf
        # Graph adjacency matrix (upper triangular) as a sparse matrix.
        adjacency_matrix = sp.triu(nx.to_scipy_sparse_array(G))
        # Vertex weights
        weights = np.array([G.nodes[n]['weight'] for n in G.nodes])
        nodes_list = np.array(list(G.nodes()))

        for i in range(1):
            try:
                mwis = gurobi_mwis.maximum_weighted_independent_set(adjacency_matrix, weights, verbose=False)
            except:
                assert not "Gurobi MIS error!"
            score = sum([G.nodes[tuple(nodes_list[n])]['weight'] for n in mwis])
            if best_mwis is None or score > best_score:
                best_mwis = [tuple(nodes_list[n]) for n in mwis]
                best_score = score
        return best_mwis
