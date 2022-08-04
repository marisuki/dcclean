import pyspark as spark
from pyspark import RDD

import database
from vertex_cover import ApproximateVC
# import CleanInterface
import constraint_graph
from topologicDC import topologicDC

inf = 0x3f3f3f3f


def edge_atom_construct_sp(conflict, dc):
    edge = []
    if dc[0]:  # single tuple, one conflict tuple
        for pred in dc[1]:  # (int, int/double, string(op), type=1,2,3)
            if pred[3] == 3:
                edge.append([(conflict, pred[0]), (-1, pred[1]), pred[2]])
            else:
                edge.append([(conflict, pred[0]), (conflict, pred[1]), pred[2]])
    elif not dc[0] and len(conflict) == 2:
        for pred in dc[1]:
            if pred[3] == 3:
                edge.append([(conflict[0], pred[0]), (-1, pred[1]), pred[2]])
                edge.append([(conflict[1], pred[0]), (-1, pred[1]), pred[2]])
            elif pred[3] == 2:
                edge.append([(conflict[0], pred[0]), (conflict[0], pred[1]), pred[2]])
                edge.append([(conflict[1], pred[0]), (conflict[1], pred[1]), pred[2]])
            else:
                edge.append([(conflict[0], pred[0]), (conflict[1], pred[1]), pred[2]])
    return edge



def edge_const(edge, cover_s, table_locc):
    rc = []
    for opx in edge:
        if opx[0] in cover_s:
            if opx[1][0] != -1: rc.append((opx[0], table_locc[opx[1][0]][1][opx[1][1]], database.reverse(opx[2])))
            else: rc.append((opx[0], opx[1], database.reverse(opx[2])))
        elif opx[1] in cover_s:
            if opx[0][0]!= -1: rc.append((opx[1], table_locc[opx[0][0]][1][opx[0][1]], database.swap_op(database.reverse(opx[2]))))
            else: rc.append((opx[1], opx[0], database.swap_op(database.reverse(opx[2]))))
    return rc


def context_parse(context):
    # [(op, comp_val(s))]
    #print(context)
    op_val: dict = {">": -inf, ">=": -inf, "<": inf, "<=": inf, "=": None, "!=": []}
    exist = {">": False, ">=": False, "<": False, "<=": False, "=": False, "!=": False}
    legal_flag = True
    for i in range(len(context)):
        op, val = context[i]
        if type(val) == type((1,2)): continue
        exist[op] = True
        if op == ">": op_val[op] = max(val, op_val[op])
        elif op == ">=": op_val[op] = max(val, op_val[op])
        elif op == "<": op_val[op] = min(op_val[op], val)
        elif op == "<=": op_val[op] = min(op_val[op], val)
        elif op == "!=": op_val[op].append(val)
        else:
            if op_val[op]: legal_flag = False
            else:
                op_val[op] = val
    if not legal_flag: return None
    ans = []
    for k in op_val:
        if exist[k]:
            ans.append((k, op_val[k]))
    return ans


def dom_extension(context, attr_type):
    ext_dom = []
    for opx in context:
        if opx[0] == "=" or opx[0] == ">=" or opx[0] == "<=":
            ext_dom.append(opx[1])
        elif opx[0] == ">":
            ext_dom.append(opx[1] + attr_type)
        elif opx[0] == "<":
            ext_dom.append(opx[1] - attr_type)
        elif opx[0] == "!=":
            ext_dom.append(opx[1] + attr_type)
            ext_dom.append(opx[1] - attr_type)
    return ext_dom


def solve_possible_values(context: [(str, )], attr_type: (str, float), candidates,  dom_ext):
    # attr_type: ("Value"/"Enumerate", "Float/Integer"/"")
    dom = list(candidates)
    if dom_ext and attr_type[0] == "Value":
        dom += dom_extension(context, attr_type[1])
    for opx in context:
        if opx[0] == "!=":
            for restrict_val in opx[1]:
                dom = database.dom_filter(dom, restrict_val, opx[0], attr_type[1])
        else:
            dom = database.dom_filter(dom, opx[1], opx[0], attr_type[1])
    return dom


def find_nearest(curr_val: float, candidate):
    if candidate:
        dist = abs(candidate[0] - curr_val)
        rep = candidate[0]
        for cand_val in candidate:
            d = abs(cand_val - rep)
            if d < dist:
                dist = d
                rep = cand_val
        return rep
    return None


def distance_cluster(tuple_i, cluster, curr_tup):
    return sum([abs(curr_tup[i]-tuple_i[i]) for i in cluster])


def solve_st_repair(table, related_cell: [(int, int)], clu_lst, covid):
    related = []
    for cell_i in related_cell: related += clu_lst[cell_i[1]]
    related = set(related)
    min_tup = database.minimal_distance_projection(table, related, table[related_cell[0][0]][1], covid)
    return [((related_cell[0][0], i), min_tup[i]) for i in related]


def isfd(dc):
    return database.isfd(dc)

def dc2fd(dcs):
    return database.dc2fd(dcs)

def v_repair_packXYA2dc(fd1, fd2):
    ans, tmp = [], []
    for A in fd1[0]:
        tmp.append((A, A, "=", 1))
    ans.append(tmp)
    tmp = []
    for A in fd2[0]:
        tmp.append((A, A, "=", 1))
    ans.append(tmp)
    ans.append([(fd1[1][0], fd2[1][0], "!=", 1)])
    return ans

def v_repair_packXYA(fd1, fd2, viol):
    ans = []
    ans += [(viol[0], A) for A in set(fd1[0]+fd2[0])]
    ans += [(viol[1], A) for A in set(fd1[0]+fd1[1])]
    ans += [(viol[2], A) for A in set(fd2[0]+fd2[1])]
    return ans

def v_repair_packXYAB2dc(fd1, fd2):
    ans = []
    ans.append([(A, A, "=", 1) for A in fd1[0]])
    ans.append([(fd1[1][0], fd1[1][0], "=", 1)])
    ans.append([(A, A, "=", 1) for A in set(fd2[0])-set(fd1[1])])
    ans.append([(fd2[1][0], fd2[1][0], "!=", 1)])
    return ans

def v_repair_pack1FD(fd, viol):
    ans = []
    ans += [(viol[0], A) for A in set(fd[0]+fd[1])]
    ans += [(viol[1], A) for A in set(fd[0]+fd[1])]
    return ans

def v_repair_packXYAB(fd1, fd2, viol):
    ans = []
    ans += list(set([(viol[0], Attr) for Attr in set(fd1[0]+fd2[0]+fd1[1])]) - set([(viol[0], Attr) for Attr in set(fd1[1])]))
    ans += [(viol[1], Attr) for Attr in set(fd1[0]+fd1[1])]
    ans += [(viol[2], Attr) for Attr in set(fd2[0]+fd2[1])]
    return ans 

def exe_vio_loc(table_loc, dc, forgive, die, precision, construct_edge=True):
    if dc[0] == False or dc[0] == True: dc = dc[1]
    viol = database.exe_vio(table_loc, dc, forgive, die, precision)
    if construct_edge:
        return [edge_atom_construct_sp(x, dc) for x in viol]
    else:
        return viol

def variant_predicates(attr_cluster, dcs, dcid_related, dcx, schema, fd=True):
    if fd: return variant_predicates_fd(attr_cluster, dcs, dcid_related, dcx, schema)
    candidate, and_pred = [], []
    for dc in dcs:
        if dc[0]: # single tuple or constant
            for attr1 in attr_cluster:
                candidate.append((attr1, attr1, "=", 2))
                for attr2 in attr_cluster:
                    candidate.append((attr1, attr2, ">", 2))
    for predx in candidate:
            res = topo.isTrivial(predx)
            if res[0] and res[1]: ans_pred.append(predx)
    return [], candidate
       
def variant_predicates_fd(attr_cluster, dcs, dcid_related, dcx, schema):
    dcx_preds = dcx[1]
    freq = {attr: 0 for attr in list(attr_cluster)}
    attr2pred = {attr: [] for attr in database.attr_dc(dcx_preds)}
    ans_del, ans_pred = [], []
    for dc_index in dcid_related:
        for attr in database.attr_dc(dcs[dc_index]):
            freq[attr] += 1
    for pred_id in range(len(dcx_preds)):
        pred = dcx_preds[pred_id]
        attr2pred[pred[0]].append(pred_id)
        attr2pred[pred[1]].append(pred_id)
    attr2pred = {attr: list(set(attr2pred[attr])) for attr in attr2pred}
    for attr in attr2pred:
        if freq[attr] == 1:
            if len(attr2pred[attr]) == 1: 
                ans_del.append(attr2pred[attr][0])
    if isfd(dcx_preds):
        #for attr in freq:
        #    if freq[attr] > 1 and attr not in attr2pred:
        #        ans_pred.append((attr, attr, "=", 1))
        for attr in range(int(len(schema)/2)):
            ans_pred.append((attr, attr, '=', 1))
    else:
        candidate = []
        flg = 1
        if dcx[0]: return [], [] 
        for attr in freq:
            if freq[attr] > 1:
                if schema[attr][1] == "Value":
                    candidate += [(attr, attr, "<", flg), (attr, attr, "=", flg), (attr, attr, ">", flg)]
                else:
                    candidate += [(attr, attr, "=", flg)]
        topo = topologicDC(dcx_preds)
        for predx in candidate:
            res = topo.isTrivial(predx)
            if res[0] and res[1]: ans_pred.append(predx)
    return ans_del, ans_pred


def dc_modify(dcs, schema):
    # (int, int/double, string(op), type=1,2,3)
    ans = [[] for i in range(len(dcs))]
    # ans: [index i -> {op: (add_num, del_num)}]
    modify = {">=": [['>', '='], ['=', '>']], "<=": [['<', '='], ['=', "<"]], "!=": [['>', '<'], ['<', '>']]}
    for i in range(len(dcs)):
        # modify
        for pid in range(len(dcs[i][1])):
            pred = dcs[i][1][pid]
            if pred[2] in modify.keys() and (schema[pred[1]][1] == "Value" or schema[pred[0]][1] == "Value"):
                for op in modify[pred[2]]:
                    ans[i].append((pid, tuple(op)))
    return ans


class DCClean:
    def __init__(self, db: database.database):
        self.db = db
        self.sparkConf = spark.SparkConf()
        self.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        self.sparkConf.set("spark.scheduler.mode", "FAIR")
        self.sparkConf.setMaster("local[*]")
        self.sparkConf.setAppName("DCClean")
        spark.SparkContext.setSystemProperty('spark.executor.memory', '12g')
        self.sparkContext = spark.SparkContext.getOrCreate(self.sparkConf)
        self.table = self.db.table
        # self.table_p = self.sparkContext.parallelize(self.db.table)
        self.dcs = self.db.dcs
        self.schema = self.db.schema
        self.attr_cluster: constraint_graph.constraintGraph = self.db.disjoint_cluster
    
    def table_parallel(self):
        table_loc = self.table
        return self.sparkContext.parallelize(table_loc).map(lambda x: x[1])

    def exe_vio(self, table_locc, osingle, omultiple, dc, forgive, die, precision, collect=False, construct_edge=True):
        if construct_edge:
            return database.exe_vio_accel_spark(table_locc, osingle, omultiple, dc[1], forgive, die, precision, collect=False).map(lambda x: edge_atom_construct_sp(x, dc))
        else:
            return database.exe_vio_accel_spark(table_locc, osingle, omultiple, dc[1], forgive, die, precision, collect=False)

    def detect_vrepair(self, loc=True):
        forgive = set(self.db.emptyCell + self.db.fv)
        conflict = []
        dcs = self.db.dcs
        precision = self.db.data_precision
        table_loc = self.table
        coverage = {}
        fds, fd_index = dc2fd(dcs)
        if not loc:
            o1 = self.sparkContext.parallelize(range(len(table_loc)))
            o2 = o1.cartesian(o1).filter(lambda x: x[0]<x[1])
        conflicting_tup = []
        # t1t2
        for fdx, i in zip(fds, fd_index):
            if not loc:
                viol = self.exe_vio(table_loc, o1, o2, dcs[i], forgive, coverage, precision, collect=False, construct_edge=False)
                conflicting_tup.append((fdx, viol.collect()))
                conflict.append(viol.map(lambda x: v_repair_pack1FD(fdx, x)))
            else:
                # (table_loc, dc, forgive, die, precision, construct_edge=True)
                viol = exe_vio_loc(table_loc, dcs[i], forgive, coverage, precision, construct_edge=False)
                conflicting_tup.append((fdx, viol))
                conflict.append([v_repair_pack1FD(fdx, x) for x in viol])
        if not conflict: return None, None, None
        # inv = hyper_edge.flatMap(lambda x: x)
        # t1t2t3
        for fd1, i1 in zip(fds, fd_index):
            for fd2, i2 in zip(fds, fd_index):
                if i1 >= i2: continue
                if set(fd1[1]) == set(fd2[1]):
                    if not loc:
                        tmp = []
                        for dc in v_repair_packXYA2dc(fd1, fd2):
                            viol = self.exe_vio(table_loc, o1, o2, [False, dc], forgive, coverage, precision, collect=False, construct_edge=False)
                            tmp.append(viol)
                        res = tmp[0].join(tmp[1]).map(lambda x: (x[1], x[0])).join(tmp[2].map(lambda x: (x, True)))
                        if res: 
                            res = res.map(lambda x: (x[1][0], x[0][0], x[0][1])).map(lambda x: v_repair_packXYA(fd1, fd2, x))
                            conflict.append(res)
                    else:
                        tmp = []
                        for dc in v_repair_packXYA2dc(fd1, fd2):
                            viol = exe_vio_loc(table_loc, [False, dc], forgive, coverage, precision, construct_edge=False)
                            viol_d = dict()
                            for vp in viol:
                                if vp[0] not in viol_d: viol_d[vp[0]] = []
                                viol_d[vp[0]].append(vp[1])
                            tmp.append(viol_d)
                        res = []
                        for t1 in tmp[0]:
                            if t1 not in tmp[1]: continue
                            for t2 in tmp[0][t1]:
                                for t3 in tmp[1][t1]:
                                    if t2 in tmp[2] and t3 in tmp[2][t2]:
                                        res.append(t1,t2,t3)
                        if res:
                            conflict.append([v_repair_packXYA(fd1, fd2, x) for x in res])
                elif fd1[1][0] in set(fd2[0]):
                    if not loc:
                        tmp = []
                        for dc in v_repair_packXYAB2dc(fd1, fd2):
                            viol = self.exe_vio(table_loc, o1, o2, [False, dc], forgive, coverage, precision, collect=False, construct_edge=False)
                            tmp.append(viol)
                        res_13 = tmp[2].intersection(tmp[3]).map(lambda x: (x, True))
                        res = tmp[0].map(lambda x: (x[1], x[0])).join(tmp[1]).map(lambda x: (x[1], x[0])) #((1, 3), 2)
                        res = res.join(res_13).map(lambda x: (x[0][0], x[1][0], x[0][1])) #(1,2,3)
                        if res:
                            res =res.map(lambda x: v_repair_packXYAB(fd1, fd2, x))
                            conflict.append(res)
                    else:
                        tmp, res = [], []
                        for dc in v_repair_packXYAB2dc(fd1, fd2):
                            viol = exe_vio_loc(table_loc, [False, dc], forgive, coverage, precision, construct_edge=False)
                            viol_d = dict()
                            for vp in viol:
                                if vp[0] not in viol_d: viol_d[vp[0]] = []
                                viol_d[vp[0]].append(vp[1])
                            tmp.append(viol_d)
                        for t1 in tmp[0]:
                            if t1 not in tmp[2] or t1 not in tmp[3]: continue
                            t3cand = set(tmp[2][t1]).intersection(tmp[3][t1])
                            if not t3cand: continue
                            for t2 in tmp[0][t1]:
                                if t2 not in tmp[1]: continue
                                for t3 in t3cand:
                                    if t3 in tmp[1][t2]:
                                        res.append((t1,t2,t3))
                        if res:
                            conflict.append([v_repair_packXYAB(fd1, fd2, x) for x in res])
        if not conflict: return None, None, None
        hyperedge = conflict[0]
        for i in range(1, len(conflict)):
            if not loc: hyperedge = hyperedge.union(conflict[i])
            else: hyperedge += conflict[i]
        if not loc: return hyperedge.collect(), conflicting_tup, fds
        else: return hyperedge, conflicting_tup, fds

    def detect(self, coverage=None, dc_index=None, construct_edge=True, sample=None):
        forgive = set(self.db.emptyCell + self.db.fv)
        if not coverage: coverage = {}
        covid = set([x[0] for x in coverage])
        conflict = []
        if not dc_index: dcs = self.db.dcs
        elif type(dc_index) == type(1): dcs = [self.db.dcs[dc_index]]
        else: dcs = [dc_index]

        precision = self.db.data_precision
        if not sample:
            table_loc = self.table
        else:
            table_loc = self.table[:sample]
        osingle = self.sparkContext.parallelize(range(len(table_loc)))

        if not coverage:
            part = int(int((int(len(table_loc)*len(table_loc)/25000)*1.0+0.2)/10)*2)
            if len(table_loc) <= 1000:
                omultiple = self.sparkContext.parallelize([[(tup1, tup2) for tup1 in range(len(table_loc))] for tup2 in range(len(table_loc))])#, numSlices=max(part, 10))
                omultiple = omultiple.flatMap(lambda x: x)
            else:
                omultiple = self.sparkContext.parallelize([tup for tup in range(len(table_loc))])
                omultiple = omultiple.cartesian(omultiple)
        else:
            part = int(int((int(len(table_loc)*len(coverage)/25000)*1.0+0.2)/10)*10*2)
            if len(table_loc) <= 5000:
                omultiple = [[(tup1, tup2) for tup1 in covid] for tup2 in range(len(table_loc))]
                omultiple = omultiple + [[(tup1, tup2) for tup1 in range(len(table_loc))] for tup2 in covid]
                omultiple = self.sparkContext.parallelize(omultiple).flatMap(lambda x: x) #, numSlices=max(part, 10)
            else:
                tableid_p = self.sparkContext.parallelize(range(len(table_loc)))
                covid_p = self.sparkContext.parallelize([tup for tup in covid])
                left = tableid_p.cartesian(covid_p)
                right = covid_p.cartesian(tableid_p)
                omultiple = left.union(right)
        
        for i in range(len(dcs)):
            viol = self.exe_vio(table_loc, osingle, omultiple, dcs[i], forgive, coverage, precision, collect=False, construct_edge=construct_edge)
            conflict.append(viol)
        
        if conflict:
            edge_res: spark.RDD = conflict[0]
            for res in conflict:
                edge_res = edge_res.union(res)
            # print(edge_res.collect())
            return edge_res
        else:
            return None

    def dc_repair(self, cover, dom_ext=True, cover_fv=True, holistic_temp=False):
        # should consider: single, multiple with conditions and detect filter
        table_loc = self.table
        table_p = self.sparkContext.parallelize(table_loc)
        data_precision_loc = self.db.data_precision
        schema_loc = self.db.schema
        if not holistic_temp:
            hyper_edges = self.detect(cover).repartition(max(10, int(len(cover)/10))) # edge: [[cell1, cell2/val_cmp, op],...]
        else:
            hyper_edges = cover[0].repartition(max(10, int(len(cover[1])/10)))
            cover = cover[1]
        
        cov_tup_id = {x[0] for x in cover}
        cover_attr_id = {x[1] for x in cover}
        domain_loc = self.db.dom
        schema_loc = {x: (schema_loc[x][1], data_precision_loc[x]) for x in range(len(data_precision_loc))}
        # print(domain_loc,  schema_loc)

        # compare_val = table_p.map(lambda x: [((x[0], i), x[1][i]) for i in range(len(x[1]))]).flatMap(lambda x: x)
        context = hyper_edges.map(lambda edge: edge_const(edge, cover, table_loc)).filter(lambda x: len(x) != 0).flatMap(lambda x: x)
        #print(context.collect())
        context = context.map(lambda x: (x[0], (x[2], x[1]))).filter(lambda x: x[1][0]!=-1).groupByKey()
        #print(context.collect())
        # context_const = context.filter(lambda x: x[0][0] == -1).map(lambda x: (x[1][0], (x[1][1], x[0][1])))
        # context = context.join(compare_val).map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1])))
        # context = context.union(context_const).groupByKey()
        context = context.map(lambda x: (x[0], context_parse(list(x[1]))))
        uns = context.filter(lambda x: not x[1])
        context = context.filter(lambda x: x[1])

        # candidate_val = compare_val.filter(lambda x: x[0][0] not in cov_tup_id and x[0][1] in cover_attr_id)
        # candidate_val = candidate_val.map(lambda x: (x[0][1], x[1])).groupByKey()
        context_candi_inc = context.map(lambda x: (x[0], (x[1], domain_loc[x[0][1]], schema_loc[x[0][1]])))
        # context_candi_inc: (cell, (context, domain, (type, precision_val)))
        # print(context_candi_inc.collect())
        # context_candi_attrinfo_inc = context_candi_inc.join(schema).map(lambda x: x[1])
        # context_candi_attrinfo: x[0], -->x[1] (attr_id, (((cell, parsed_context), dom: [...]),schema:(str, float)))
        # print(context_candi_attrinfo_inc.collect())

        # context_candi_attrinfo_inc = context_candi_attrinfo_inc.map(lambda x: (x[0][0][0], (x[0][0][1], list(x[0][1]), x[1])))
        vf_fv_repair = context_candi_inc.map(lambda x: (x[0], solve_possible_values(x[1][0], x[1][2], x[1][1], dom_ext)))
        vf_fv_repair = vf_fv_repair.map(lambda x: (x[0], find_nearest(table_loc[x[0][0]][1][x[0][1]], x[1])))
        # vf_fv_repair = context_candi_attrinfo_inc.map(lambda x: (x[0], solve_possible_values(x[1][0], x[1][2], x[1][1], dom_ext)))
        # vf_fv_repair = vf_fv_repair.join(compare_val).map(lambda x: (x[0], find_nearest(x[1][1], x[1][0])))
        call_p = vf_fv_repair.count()

        if cover_fv:
            solved = vf_fv_repair.filter(lambda x: x[1])
            unsolved = vf_fv_repair.filter(lambda x: not x[1])
            unsolved = unsolved.union(uns).map(lambda x: (x[0][0], x[0])).groupByKey()
            call_n = unsolved.count()
            # print(unsolved.collect())
            clu: dict = {attr: self.attr_cluster.extract_cluster(attr) for attr in cover_attr_id}
            unsolved = unsolved.map(lambda x: solve_st_repair(table_loc, list(x[1]), clu, cov_tup_id)).flatMap(lambda x: x)
            # print(solved.collect(), unsolved.collect())
            # solved = solved.union(unsolved).map(lambda x: (x[0], (table_loc[x[0][0]][1][x[0][1]], x[1]))).filter(lambda x: x[1][0] != x[1][1]).collect()
            # print(solved.collect(), unsolved.collect())
            solved = [(x[0], (table_loc[x[0][0]][1][x[0][1]], x[1])) for x in solved.collect() + unsolved.collect()]
            # print(solved)
            return solved, [], (call_p, call_n)
        else:
            # uns = uns.map(lambda x: (x[0], (None, None)))
            # repair = vf_fv_repair.join(compare_val).map(lambda x: (x[0], (x[1][1], x[1][0]))).union(uns)
            # rep = repair.filter(lambda x: x[1][1]).filter(lambda x: x[1][0] != x[1][1]).collect()
            # fv = repair.filter(lambda x: not x[1][1]).map(lambda x: x[0]).collect()
            rep = vf_fv_repair.filter(lambda x: x[1]).collect()
            call_n = 0
            rep = [(x[0], (table_loc[x[0][0]][1][x[0][1]], x[1])) for x in rep]
            fv = vf_fv_repair.filter(lambda x: not x[1]).map(lambda x: x[0]).collect() + uns.map(lambda x: x[0]).collect()
            return rep, fv, (call_p, call_n)

    def vertex_cover(self, hyper_edges, weighted=True):
        vcf = ApproximateVC()
        edges = []
        mapper = {}
        reverse = {}
        for edge in hyper_edges:
            e = []
            for predicate in edge:
                if predicate[0] not in mapper and predicate[0][0] != -1:
                    mapper[predicate[0]] = len(mapper)
                if predicate[1] not in mapper and predicate[1][0] != -1:
                    mapper[predicate[1]] = len(mapper)
                if predicate[0] in mapper:
                    e.append(mapper[predicate[0]])
                if predicate[1] in mapper:
                    e.append(mapper[predicate[1]])
            edges.append(e)
        for k in mapper:
            reverse[mapper[k]] = k
        if weighted: weight: dict = {k: self.db.weight(k) for k in mapper}
        else: weight: dict = {k: 1 for k in mapper}
        return set([reverse[i] for i in vcf.vertex_cover(edges, weight)])
    
    def fd_vertex_cover(self, hyper_edges):
        vcf = ApproximateVC()
        edges = []
        mapper, reverse, weight = {}, {}, {}
        for edge in hyper_edges:
            e = []
            for cel in edge:
                if cel not in mapper:
                    mapper[cel] = len(mapper)
                    reverse[len(reverse)] = cel
                    weight[mapper[cel]] = 0.0
                weight[mapper[cel]] += 1.0
                e.append(mapper[cel])
            edges.append(e)
        weight = {cel:1.0/(weight[cel]+1) for cel in weight}
        return set([reverse[i] for i in vcf.vertex_cover(edges, weight)])

    def violation_free(self, no_fv=True, dom_ext=True, info_fetch=True):
        # asks for parallel set cover alg
        hyper_edges = self.detect(None).collect()
        if len(hyper_edges) == 0:
            print("Input DB is Already Clean...Abort")
            return self.db, 0, {}
        else: print("[VFree/Vfree+] Violation detected.")
        cover = self.vertex_cover(hyper_edges)
        # print(cover)
        rep, fv, call = self.dc_repair(cover, cover_fv=no_fv, dom_ext=dom_ext)
        # print(rep, fv)
        self.db.merge_repair({rep[i][0]: rep[i][1] for i in range(len(rep))}, set(fv))
        self.db.persist()
        # self.table_p = self.sparkContext.parallelize(self.db.table)
        self.table = self.db.table
        # print(self.detect(None).collect())
        if info_fetch: return self.db, call[0]+call[1], cover
        return self.db, call[0]+call[1], cover
    
    def holistic(self, no_fv=True, dom_ext=True):
        if no_fv: iter_x = 15
        else: iter_x = 15
        cal = [0, 0]
        history_cells = set()
        while iter_x:
            hyper_edges = self.detect(None)
            hyper_edge_col = hyper_edges.collect()
            if not hyper_edge_col: break
            cover = self.vertex_cover(hyper_edge_col)
            rep, fv, call = self.dc_repair((hyper_edges, cover), dom_ext=dom_ext, cover_fv=no_fv, holistic_temp=True)
            print("Iter {}: repair {}, fv {}, call_p {}, call_n {}".format(iter_x, len(rep), len(fv), call[0], call[1]))
            cal[0] += call[0]
            cal[1] += call[1]
            if not no_fv or no_fv:
                repp = []
                for cp in rep:
                    if cp[0] in history_cells:
                        fv.append(cp[0])
                    else:
                        history_cells |= {cp[0]}
                        repp.append(cp)
                rep = repp
            self.db.merge_repair({rep[i][0]: rep[i][1] for i in range(len(rep))}, set(fv))
            self.db.persist()
            self.table = self.db.table
            iter_x -= 1
            # hyper_edges = self.detect(None)
            # hyper_edge_col = hyper_edges.count()
            # iter_x += 1
        return self.db, cal[0]+cal[1], history_cells
    
    def v_repair(self):
        cal = 0
        history_cells = set()
        hyper_edges, conflicts, fds = self.detect_vrepair(loc=True)
        hyper_edge_col = hyper_edges
        if not hyper_edge_col: 
            print("Cleaned database")
            return self.db, cal, {}
        #cover = self.vertex_cover(hyper_edge_col)
        cover = self.fd_vertex_cover(hyper_edges)
        #print(cover)
        cover, sz = set(cover), len(cover)
        prev = 0
        change = dict()
        vis = dict()
        for vi in cover:
            if vi[0] not in vis: vis[vi[0]] = []
            vis[vi[0]].append(vi[1])
        rep = dict()
        while cover:
            if prev == len(cover): break
            prev = len(cover)
            tc = []
            for fdx, tps in conflicts:
                tmp = []
                for tp in tps:
                    plc, src = None, None
                    if tp[0] in vis and len(vis[tp[0]]) == 1 and tp[1] not in vis:
                        plc, src = (tp[0], vis[tp[0]][0]), (tp[1], vis[tp[0]][0])
                    elif tp[1] in vis and len(vis[tp[1]]) == 1 and tp[0] not in vis:
                        plc, src = (tp[1], vis[tp[1]][0]), (tp[0], vis[tp[1]][0])
                    if not plc: 
                        tmp.append(tp)
                        continue
                    if plc[1] == fdx[1][0]:
                        rep[plc] = (self.table[plc[0]][1][plc[1]], self.table[src[0]][1][src[1]])
                        cover = cover-set([plc])
                tc.append((fdx, tmp))
            conflicts = tc
        #fv = cover
        #print(rep, fv)
        fv = []
        self.db.merge_repair(rep, fv)
        self.db.persist()
        vis_viol = self.detect(None).collect()
        if vis_viol:
            fv = self.vertex_cover(vis_viol)
            self.db.merge_repair({}, fv)
            self.db.persist()
        self.table = self.db.table
        return self.db, len(cover), set(set(rep.keys()).union(fv))
    
    def dc_variance(self, theta, del_bias=-0.5, metric='unit', vfree=True, no_fv=True, dom_ext=True, info_fetch=True, dc_variant_candidate=None, max_del=None):
        dcs = self.db.dcs
        #print(dcs)
        forgive = set(self.db.emptyCell + self.db.fv)
        conflicts = dict() #{(ti, tj) -> [dc_index]}
        dc_clu = dict() # attrs -> [dc_index1, ...]
        modify = dc_modify(dcs, self.schema)
        if not dc_variant_candidate: dc_variant_candidate = (range(len(dcs)), range(len(dcs)))
        variant_candidate = []
        data_precision_loc = self.db.data_precision
        original_sz = dict()
        sample_sz = 1000
        for dc_id in dc_variant_candidate[1]:
            viol = self.detect(dc_index=dc_id,construct_edge=False, sample=sample_sz).collect()
            original_sz[dc_id] = len(viol)
            for tup in viol:
                if tup not in conflicts: conflicts[tup] = []
                conflicts[tup].append(dc_id) 
            padd = []
            if database.isfd(self.dcs[dc_id]) or not self.dcs[dc_id][0]:
                # here for fd variant
                l, r = database.fdfromdc(self.dcs[dc_id][1])
                for attr in range(len(data_precision_loc)):
                    if attr in l or attr in r: continue
                    padd.append((attr, attr, '=', 1))
            else:
                # add code here for general dcs.: add preds
                lx = database.attr_dc(self.dcs[dc_id])
                for attr in range(len(data_precision_loc)):
                    if attr in lx: continue
                    for attr2 in range(len(data_precision_loc)):
                        if attr2 in lx: continue
                        padd += [(attr, attr2, op, 2) for op in ["=", ">", "<"]]
            score, operation = 0, None
            dc_original = dcs[dc_id][1]
            for add_pred in padd:
                if len(self.table) < 1e4:
                    viol_minus = exe_vio_loc(self.table[:sample_sz], [dcs[dc_id][0], [add_pred]+dc_original], forgive, {}, self.db.data_precision, construct_edge=False)
                else:
                    viol_minus = self.detect(dc_index=[dcs[dc_id][0], [add_pred]+dc_original], construct_edge=False, sample=sample_sz).collect()
                tmp_sc = len(viol_minus)
                if tmp_sc == 0: continue # remove trivial dc, another way: use constraint_graph for avoiding trivial dcs
                #if tmp_sc > score: score, operation = tmp_sc, ('add', dc_id, add_pred, tmp_sc)
                variant_candidate.append(('add', dc_id, add_pred, tmp_sc))
            #if operation: variant_candidate.append(operation)
            conflicts = dict()
        """for dc_index in dc_variant_candidate:
            viol = self.detect(dc_index=dc_index,construct_edge=False).collect()
            for tup in viol:
                if tup not in conflicts: conflicts[tup] = []
                conflicts[tup].append(dc_index)
            related = tuple(sorted(self.attr_cluster.extract_cluster(dcs[dc_index][1][0][0])))
            if related not in dc_clu: dc_clu[related] = []
            dc_clu[related].append(dc_index)
        for related_attrs in dc_clu:
            dcid_related = dc_clu[related_attrs]
            for dc_index in dc_clu[related_attrs]:
                dc_original = dcs[dc_index][1]
                pdel, padd = variant_predicates(attr_cluster=related_attrs, dcs=dcs, dcid_related=dcid_related, 
                                                dcx=dcs[dc_index], schema=self.schema) 
                # [pid], [pred]
                pmodif = modify[dc_index] # [(pid, (new_op, minus_op))]
                print(pmodif, pdel, padd)
                score, operation = 0, None
                del_sc, del_pid = -inf, None
                for mod_p in pmodif:
                    predx = dc_original[mod_p[0]]
                    newop, minusop = mod_p[1]
                    pred_modify = (predx[0], predx[1], minusop, predx[3])
                    viol_minus = exe_vio_loc(self.table, [dc_original[0], [pred_modify]], forgive, {}, self.db.data_precision, construct_edge=False)
                    tmp_sc = 0.
                    for tup in viol_minus:
                        if tup not in conflicts: continue
                        tmp_sc += 1.0/len(conflicts[tup])
                    if tmp_sc > score: score, operation = tmp_sc, ('modify', dc_index, mod_p[0], (predx[0], predx[1], newop, predx[3]), tmp_sc)
                for add_pred in padd:
                    viol_minus = exe_vio_loc(self.table, [dc_original[0], [add_pred]], forgive, {}, self.db.data_precision, construct_edge=False)
                    tmp_sc = 0.
                    for tup in viol_minus:
                        if tup not in conflicts: continue
                        tmp_sc += 1.0/len(conflicts[tup])
                    if tmp_sc > score: score, operation = tmp_sc, ('add', dc_index, add_pred, tmp_sc)
                for del_id in pdel:
                    pred_del = dc_original[del_id]
                    additional_violdc_pred = [(pred_del[0], pred_del[1], database.reverse(pred_del[2]), pred_del[3])]
                    additional_violdc_pred += dc_original[:del_id] + dc_original[del_id+1:]
                    viol_additional = exe_vio_loc(self.table, [dc_original[0], additional_violdc_pred], forgive, {}, self.db.data_precision, construct_edge=False)
                    tmp_sc = 0.
                    for tup in viol_additional:
                        if tup not in conflicts: tmp_sc += 1.0
                        else: tmp_sc += 1.0/len(conflicts[tup])
                    if tmp_sc > del_sc: del_sc, del_pid = tmp_sc, ('del', dc_index, del_id, tmp_sc)
                if del_pid: variant_candidate.append(del_pid)
                if operation: variant_candidate.append(operation)
                """
        #print(variant_candidate)
        vadd = list(filter(lambda x: x[0]!='del', variant_candidate))
        vadd = sorted(vadd, key=lambda x: x[-1])
        #vdel = list(filter(lambda x: x[0]=='del', variant_candidate))
        # vdel = sorted(vdel, key=lambda x: x[-1])
        # ('add', dc_id, add_pred, tmp_sc)
        vdel = []
        for dc_id in dc_variant_candidate[0]:
            del_candidate = []
            if len(self.table) < 1e4:
                orig = exe_vio_loc(self.table[:sample_sz], dcs[dc_id], forgive, {},
                                         self.db.data_precision, construct_edge=False)
            else:
                orig = self.detect(dc_index=dcs[dc_id], construct_edge=False,
                                         sample=sample_sz).collect()
            for pred_id in range(len(self.dcs[dc_id][1])):
                rmv_preds = []
                for i in range(pred_id): rmv_preds.append(self.dcs[dc_id][1][i])
                for i in range(pred_id+1, len(self.dcs[dc_id][1])): rmv_preds.append(self.dcs[dc_id][1][i])
                if len(self.table) < 1e4:
                    viol_minus = exe_vio_loc(self.table[:sample_sz], [dcs[dc_id][0], rmv_preds], forgive, {}, self.db.data_precision, construct_edge=False)
                else:
                    viol_minus = self.detect(dc_index=[dcs[dc_id][0], rmv_preds], construct_edge=False, sample=sample_sz).collect()
                del_candidate.append(('del', dc_id, rmv_preds, len(viol_minus)-len(orig)))
            vdel.append(sorted(del_candidate, key=lambda x: x[-1])[0])
        vdel = sorted(vdel, key=lambda x: x[-1])
        del_pos = None
        modified_pos = set()
        import copy
        dc_tmp = copy.deepcopy(self.dcs)
        #print(original_sz)
        if vdel and max_del:
            curr, ptr_d, rmw = theta, 0, 0
            while ptr_d < len(vdel) and max_del > rmw:
                if metric == 'unit': rmw_ite = abs(del_bias)
                else: rmw_ite = abs(del_bias*(vdel[ptr_d][-1]*1.0/(len(self.db.table))**2))
                if rmw + rmw_ite < max_del:
                    rmw += rmw_ite
                    dc_tmp[vdel[ptr_d][1]] = (self.dcs[vdel[ptr_d][1]][0], vdel[ptr_d][2])
                else: break
                ptr_d += 1
            theta += theta + rmw
        curr, ptr = theta, 0
        while ptr < len(vadd) and curr > 0:
            pred_cur = vadd[ptr]
            curr_minus = curr
            if metric == 'unit':
                if pred_cur[0] == 'add': curr_minus -= 1.0
                elif pred_cur[0] == 'del': curr_minus -= abs(del_bias)
                elif pred_cur[0] == 'modify': curr_minus -= (1.0 - abs(del_bias))
                else: print("[Inner error] Error operation in variant candidate.")
            else: 
                if pred_cur[0] == 'add': curr_minus -= (abs(vadd[ptr][-1])*1.0/((len(self.db.table))))
                else: curr_minus -= abs(del_bias)*(abs(vadd[ptr][-1])*1.0/((len(self.db.table))))
            if curr_minus < 0: break
            print("apply: " + str(pred_cur))
            if pred_cur[0] == 'modify':
                if del_pos and del_pos == (pred_cur[1], pred_cur[2]): continue
                dc_tmp[pred_cur[1]][1][pred_cur[2]] = pred_cur[3]
                curr = curr_minus
            elif pred_cur[0] == 'add':
                dc_tmp[pred_cur[1]][1].append(pred_cur[2])
                curr = curr_minus
            else: print("[Inner program error] Error in dc_variant.")
            ptr += 1
        if del_pos:
            dc_tmp[del_pos[0]][1].remove(self.dcs[del_pos[0]][1][del_pos[1]])
        self.db.dcs = dc_tmp
        self.dcs = dc_tmp
        self.db.disjoint_cluster = constraint_graph.constraintGraph(len(self.db.data_precision))
        for dci in self.dcs: self.db.disjoint_cluster.add_connection(database.attr_dc(dci))
        self.attr_cluster = self.db.disjoint_cluster
        # def violation_free(self, no_fv=True, dom_ext=True, info_fetch=False):
        print(self.dcs)
        if vfree: return self.violation_free(no_fv=no_fv, dom_ext=dom_ext, info_fetch=info_fetch)
        else: return self.holistic(no_fv=no_fv, dom_ext=dom_ext)

    def dc_variant(self, theta, del_bias=-0.5, metric='unit', vfree=True, no_fv=True, dom_ext=True, info_fetch=True, dc_variant_candidate=None, fd=True):
        dcs = self.db.dcs
        #print(dcs)
        forgive = set(self.db.emptyCell + self.db.fv)
        conflicts = dict() #{(ti, tj) -> [dc_index]}
        dc_clu = dict() # attrs -> [dc_index1, ...]
        modify = dc_modify(dcs, self.schema)
        if not dc_variant_candidate: dc_variant_candidate = range(len(dcs))
        for dc_index in dc_variant_candidate:
            viol = self.detect(dc_index=dc_index,construct_edge=False).collect()
            for tup in viol:
                if tup not in conflicts: conflicts[tup] = []
                conflicts[tup].append(dc_index)
            related = tuple(sorted(self.attr_cluster.extract_cluster(dcs[dc_index][1][0][0])))
            if related not in dc_clu: dc_clu[related] = []
            dc_clu[related].append(dc_index)
        variant_candidate = []
        for related_attrs in dc_clu:
            dcid_related = dc_clu[related_attrs]
            for dc_index in dc_clu[related_attrs]:
                dc_original = dcs[dc_index][1]
                pdel, padd = variant_predicates(attr_cluster=related_attrs, dcs=dcs, dcid_related=dcid_related, 
                                                dcx=dcs[dc_index], schema=self.schema, fd=fd) 
                # [pid], [pred]
                pmodif = modify[dc_index] # [(pid, (new_op, minus_op))]
                print(pmodif, pdel, padd)
                score, operation = 0, None
                del_sc, del_pid = -inf, None
                for mod_p in pmodif:
                    predx = dc_original[mod_p[0]]
                    newop, minusop = mod_p[1]
                    pred_modify = (predx[0], predx[1], minusop, predx[3])
                    viol_minus = exe_vio_loc(self.table, [dc_original[0], [pred_modify]], forgive, {}, self.db.data_precision, construct_edge=False)
                    tmp_sc = 0.
                    for tup in viol_minus:
                        if tup not in conflicts: continue
                        tmp_sc += 1.0/len(conflicts[tup])
                    if tmp_sc > score: score, operation = tmp_sc, ('modify', dc_index, mod_p[0], (predx[0], predx[1], newop, predx[3]), tmp_sc)
                for add_pred in padd:
                    viol_minus = exe_vio_loc(self.table, [dc_original[0], [add_pred]], forgive, {}, self.db.data_precision, construct_edge=False)
                    tmp_sc = 0.
                    for tup in viol_minus:
                        if tup not in conflicts: continue
                        tmp_sc += 1.0/len(conflicts[tup])
                    if tmp_sc > score: score, operation = tmp_sc, ('add', dc_index, add_pred, tmp_sc)
                """
                for del_id in pdel:
                    pred_del = dc_original[del_id]
                    additional_violdc_pred = [(pred_del[0], pred_del[1], database.reverse(pred_del[2]), pred_del[3])]
                    additional_violdc_pred += dc_original[:del_id] + dc_original[del_id+1:]
                    viol_additional = exe_vio_loc(self.table, [dc_original[0], additional_violdc_pred], forgive, {}, self.db.data_precision, construct_edge=False)
                    tmp_sc = 0.
                    for tup in viol_additional:
                        if tup not in conflicts: tmp_sc += 1.0
                        else: tmp_sc += 1.0/len(conflicts[tup])
                    if tmp_sc > del_sc: del_sc, del_pid = tmp_sc, ('del', dc_index, del_id, tmp_sc)
                if del_pid: variant_candidate.append(del_pid)
                if operation: variant_candidate.append(operation)
                """
        #print(variant_candidate)
        vadd = list(filter(lambda x: x[0]!='del', variant_candidate))
        vadd = sorted(vadd, key=lambda x: x[-1])
        vdel = list(filter(lambda x: x[0]=='del', variant_candidate))
        # vdel = sorted(vdel, key=lambda x: x[-1])
        vdel = []
        import copy
        dc_tmp = copy.deepcopy(self.dcs)
        curr, ptr, vmodif = theta, 0, []
        del_pos = None
        modified_pos = set()
        if vdel:
            if metric == 'unit': curr -= del_bias
            else: curr -= del_bias*(vdel[0][-1]*1.0/(len(self.db.table))**2)
            vmodif = [vdel[0]]
            del_pos = (vdel[0][1], vdel[0][2])
            modified_pos |= {del_pos}
        while ptr < len(vadd) and curr > 0:
            pred_cur = vadd[ptr]
            curr_minus = 0.
            if metric == 'unit':
                if pred_cur[0] == 'add': curr_minus -= 1.0
                else: curr_minus -= (1.0+del_bias)
            else: 
                if pred_cur[0] == 'add': curr_minus -= (vadd[ptr][-1]*1.0/(len(self.db.table))**2)
                else: curr_minus -= (1.0+del_bias)*(vadd[ptr][-1]*1.0/(len(self.db.table))**2)
            if curr < curr_minus: break  
            if pred_cur[0] == 'modify':
                if del_pos and del_pos == (pred_cur[1], pred_cur[2]): continue
                dc_tmp[pred_cur[1]][1][pred_cur[2]] = pred_cur[3]
                curr -= curr_minus
            elif pred_cur[0] == 'add':
                dc_tmp[pred_cur[1]][1].append(pred_cur[2])
                curr -= curr_minus
            else: print("[Inner program error] Error in dc_variant.")
            ptr += 1
        if del_pos:
            dc_tmp[del_pos[0]][1].remove(self.dcs[del_pos[0]][1][del_pos[1]])
        self.db.dcs = dc_tmp
        self.dcs = dc_tmp
        self.db.disjoint_cluster = constraint_graph.constraintGraph(len(self.db.data_precision))
        for dci in self.dcs: self.db.disjoint_cluster.add_connection(database.attr_dc(dci))
        self.attr_cluster = self.db.disjoint_cluster
        # def violation_free(self, no_fv=True, dom_ext=True, info_fetch=False):
        print(self.dcs)
        if vfree: return self.violation_free(no_fv=no_fv, dom_ext=dom_ext, info_fetch=info_fetch)
        else: return self.holistic(no_fv=no_fv, dom_ext=dom_ext)

    def unified(self):
        return None