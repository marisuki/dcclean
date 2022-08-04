import database
from constraint_graph import disjointSet


def edge_atom_construct_sp(conflict, dc):
    edge = []
    # print(dc)
    if dc[0]:  # single tuple, one conflict tuple
        for pred in dc[1]:  # (int, int/double, string(op), type=1,2,3)
            if pred[3] == 3:
                edge.append([(conflict, pred[0]), (-1, pred[1]), pred[2]])
            else:
                edge.append([(conflict, pred[0]), (conflict, pred[1]), pred[2]])
    else:
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


def degree(graph):
    deg = {} # [cell: int, int] -> degval
    for edge in graph:
        for pred in edge:
            if pred[0] in deg:
                deg[pred[0]] = deg[pred[0]] + 1
            else:
                deg[pred[0]] = 1
            if pred[1][0] != -1:
                if pred[1] in deg:
                    deg[pred[1]] = deg[pred[1]] + 1
                else:
                    deg[pred[1]] = 1
    return deg


class DCClean:
    # single core dc clean.
    def __init__(self, db: database.database):
        self.db = db
        self.table = self.db.table

    def cluster_repair(self, fv_cell: (int, int), mask: {int}):
        cluster_attr = self.db.disjoint_cluster.extract_cluster(fv_cell[1])
        homono = self.table[fv_cell[0]][1]
        replace = database.minimal_distance_projection(self.table, cluster_attr, fv_cell[0], mask)
        repair = {(fv_cell[0], attr): (homono[attr], replace[attr]) for attr in cluster_attr}
        return repair

    def context_repair(self, cell: (int, int), vertex_cover: [(int, int)], suspect, dom_ext: bool=False):
        repair_context = []
        for edge in suspect:
            for opx in edge:
                if opx[0] == cell:
                    repair_context.append((opx[1], database.reverse(opx[2])))
                elif opx[1] == cell:
                    repair_context.append((opx[0], database.swap_op(database.reverse(opx[2]))))
        dom_x = []
        for tup in self.table:
            if (tup[0], cell[1]) not in vertex_cover and (tup[0], cell[1]) not in self.db.emptyCell:
                dom_x.append(tup[1][cell[1]])
        dom_x = list(set(dom_x))
        # print(repair_context)
        if dom_ext and self.db.schema[cell[1]][1] == "Value":
            for pred in repair_context:
                if pred[1] == ">":
                    dom_x.append(self.table[pred[0][0]][1][pred[0][1]] + database.bias)
                elif pred[1] == "<":
                    dom_x.append(self.table[pred[0][0]][1][pred[0][1]] - database.bias)
                elif pred[1] == ">=" or pred[1] == "<=" or pred[1] == "=":
                    dom_x.append(self.table[pred[0][0]][1][pred[0][1]])
                elif pred[1] == "!=":
                    dom_x += [self.table[pred[0][0]][1][pred[0][1]] - database.bias,
                              self.table[pred[0][0]][1][pred[0][1]] + database.bias]
                else:
                    print("type error.")
        for rest in repair_context:
            # print(rest, self.table[-1], rest[1], self.data_precision[cell[1]])
            dom_x = database.dom_filter(dom_x, self.table[rest[0][0]][1][rest[0][1]], rest[1],
                                        self.db.data_precision[cell[1]])
        ans, ori = -1, self.table[cell[0]][1][cell[1]]
        dis = 0x3f3f3f3f
        if len(dom_x) == 0:
            return cell, -1, -1, -1
        else:
            for x in dom_x:
                if dis - abs(x - ori) > database.equiv_bias:
                    dis = abs(x - ori)
                    ans = x
            return cell, ori, ans, 1

    def cover_repair(self, vertex_cover: {(int, int)}, dom_ext: bool):  # in: tupId, attrId
        if len(self.db.modify) != 0:
            self.db.persist()
        hyper_graph = self.execute_conflict_graph(vertex_cover, single_only=False, edge_build=True)
        repair = {}  # cell(int, int) -> (float: oldVal, float: newVal)
        fv = []
        for cell in vertex_cover:
            suspect = hyper_graph
            res = self.context_repair(cell, vertex_cover, suspect, dom_ext)
            if res[-1] < 0:
                fv.append(res[0])
            else:
                repair[res[0]] = (res[1], res[2])
        return repair, fv

    def execute_conflict_graph(self, coverage=None, single_only=False, edge_build=True):
        if coverage is None:
            coverage = {}
        if len(self.db.modify) != 0:
            self.db.persist()
        exe_dc_s = []
        exe_dc_m = []
        for i in range(len(self.db.dcs)):
            if self.db.dcs[i][0]:
                exe_dc_s.append(i)
            else:
                exe_dc_m.append(i)
        hyper_graph = {}
        forgive = set(self.db.emptyCell + self.db.fv)
        if single_only:
            for dc_index in exe_dc_s:
                if dc_index in hyper_graph:
                    hyper_graph[dc_index] += self.db.exe_vio(self.db.dcs[dc_index][1], forgive, coverage)
                else:
                    hyper_graph[dc_index] = self.db.exe_vio(self.db.dcs[dc_index][1], forgive, coverage)
        else:
            for i in range(len(self.db.dcs)):
                print(str(i) + "of" + str(len(self.db.dcs)))
                if i in hyper_graph:
                    hyper_graph[i] += self.db.exe_vio(self.db.dcs[i][1], forgive, coverage)
                else:
                    hyper_graph[i] = self.db.exe_vio(self.db.dcs[i][1], forgive, coverage)
        if edge_build:
            return self.edge_build(hyper_graph, -1)
        else:
            return hyper_graph

    def edge_build(self, hyper_graph, attention=-1):
        edges = []
        for k in hyper_graph.keys():
            for conflict_id in range(len(hyper_graph[k])):
                if attention == -1:
                    edges.append(self.edge_atom_construct(k, conflict_id, hyper_graph))
                else:
                    conflict = hyper_graph[k][conflict_id]
                    if len(conflict) == 1 and conflict == attention:
                        edges.append(self.edge_atom_construct(k, conflict_id, hyper_graph))
                    elif len(conflict) == 2 and (conflict[0] == attention or conflict[1] == attention):
                        edges.append(self.edge_atom_construct(k, conflict_id, hyper_graph))
            # print(edges)
        return edges

    def edge_atom_construct(self, dc_id, conflict_index, hyper_graph):
        conflict = hyper_graph[dc_id][conflict_index]
        return edge_atom_construct_sp(conflict, self.db.dcs[dc_id])

    def vertex_cover(self, graphs):
        from vertex_cover import ApproximateVC
        avc = ApproximateVC()
        mapper = dict()
        gx = []
        for edge in graphs:
            e = []
            for pred in edge:
                if pred[0] not in mapper: mapper[pred[0]] = len(mapper)
                if pred[1] not in mapper: mapper[pred[1]] = len(mapper)
                e += [mapper[pred[0]], mapper[pred[1]]]
            gx.append(e)
        rev = {mapper[x]: x for x in mapper}
        return set([rev[x] for x in avc.vertex_cover(gx, {x: self.db.weight(x) for x in mapper})])

    def violation_free(self, single_only=False, domain_extension=False, no_fv=True, persist=False):
        gra = self.execute_conflict_graph(single_only=single_only)
        # print("conflict graph")
        # print(gra)
        cover = self.vertex_cover(gra)
        print(cover)
        repair, fvs = self.cover_repair(cover, domain_extension)
        # print(repair, fvs)
        self.db.merge_repair(repair, [])
        if no_fv:
            repair_add = {}
            tup = {x[0] for x in fvs}
            for cell in fvs:
                rep = self.cluster_repair(cell, tup)
                for k in rep.keys():
                    repair_add[k] = rep[k]
            self.db.merge_repair(repair_add, [])
            # print(repair_add)
        else:
            self.db.merge_repair({}, fvs)
        if persist: self.db.persist()
        print("modified")
        print(self.db.modifyHistory)
        return self.db
