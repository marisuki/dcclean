from copy import copy

"""
 A minimum-weighted vertex cover generator for hyper graph (precise answer, NP-hard!)
 refer to "Efficient Algorithms for Dualizing Large-Scale Hypergraphs"

 input (self.vertex_cover): hyper graph: [edge], edge: [int: cell_hash]
 output (self.vertex_cover): a minimum-weighted vertex cover(weight, cell_hash set)

 limitation: |hyper-graph| < 20, |edge| < 10 ~~.
"""


class Precise_MWVC:
    def __init__(self):
        self.tree = dict()
        self.weight = dict()

    def traverse(self, k: set, plc: dict):
        if not k: return plc
        if len(plc) == 0:
            plc = {cell: {} for cell in k}
            return plc
        for x in plc:
            if x in k:
                continue
            temp = copy(k)
            for i in plc:
                if i in temp:
                    temp.remove(i)
            if len(plc[x]) == 0:
                plc[x] = {cell: dict() for cell in temp}
            else:
                plc[x] = self.traverse(temp, plc[x])
        return plc

    def minimum_path(self, plc: dict):
        min_w = 0x7f7f7f7f
        path = []
        for k in plc:
            if len(plc[k]) == 0:
                if k in self.weight:
                    min_w, path = self.weight[k], [k]
                else:
                    min_w, path = 1.0, [k]
            else:
                w, p = self.minimum_path(plc[k])
                if k in self.weight:
                    w += self.weight[k]
                else:
                    w += 1.0
                if w < min_w:
                    p.append(k)
                    path = p
                    min_w = w
        return min_w, path

    def vertex_cover(self, hyper_graph: [[int]], weight: {int: float}):
        pos = 0
        for edge in hyper_graph:
            # print("position: " + str(pos) + "/" + str(len(hyper_graph)))
            pos += 1
            self.tree = self.traverse(set(edge), self.tree)
        if not weight: weight = dict()
        self.weight = weight
        min_w, path = 0x7f7f7f7f, []
        for k in self.tree:
            w, p = self.minimum_path(self.tree[k])
            if k in self.weight:
                w += self.weight[k]
            else:
                w += 1.0
            if w < min_w:
                p.append(k)
                path = p
                min_w = w
        return min_w, path


class ApproximateVC:
    def __init__(self):
        self.vc = set()

    def subset_cover(self, subset_edge: [int], hyper_graph: [[int]], weight_func: {int: float}):
        deg = {}
        cover = []
        temp = subset_edge
        while temp:
            for i in temp:
                edge = hyper_graph[i]
                for cell in edge:
                    if cell in deg:
                        deg[cell] += 1
                    else:
                        deg[cell] = 1
            v, vm = -1, -1
            for c in deg:
                if deg[c] > vm:
                    vm = deg[c]
                    v = c
                elif deg[c] == vm:
                    if weight_func[c] < weight_func[v]:
                        v = c
            cover.append(v)
            tmp = []
            for i in temp:
                edge = hyper_graph[i]
                if v not in edge:
                    tmp.append(i)
            temp = tmp
        return cover

    def cluster_detection(self, gra: [[int]]):
        from constraint_graph import disjointSet
        disjoint = disjointSet(int(1e6))
        for edge in gra:
            for i in range(1, len(edge)):
                disjoint.combine(edge[i - 1], edge[i])
        cluster = dict()
        for i in range(len(gra)):
            res = disjoint.find(gra[i][0])
            if res in cluster:
                cluster[res].append(i)
            else:
                cluster[res] = [i]
        return [cluster[x] for x in cluster]

    def vertex_cover(self, hyper_graph, weight, all_in=-1):
        for edge in hyper_graph:
            for cell in edge:
                if cell not in weight:
                    weight[cell] = 1.0
        if all_in != -1:
            self.vc = set(self.subset_cover(range(len(hyper_graph)), hyper_graph, weight))
            return self.vc
        clu = self.cluster_detection(hyper_graph)
        vcx = []
        for i in range(len(clu)):
            component = clu[i]
            # print("component:" + str(i) + "/" + str(len(clu)))
            vcx += self.subset_cover(component, hyper_graph, weight)
        self.vc = set(vcx)
        # print(self.vc)
        return self.vc


"""
approximate set cover with (1+eps)*ln Delta
Sets [S_i], any i
Domain D: S_i \subset D
Delta = max |S_i|
"""


class ApproximateVC_1peps:
    def __init__(self):
        self.vc = set()


# test:
if __name__ == "__main__":
    import random

    node = [i for i in range(100000)]
    graph = [random.sample(node, random.randint(4, 16)) for x in range(10000)]
    avc = ApproximateVC()
    print(graph)
    print('start build...')
    import time

    st = time.time()
    vc = avc.vertex_cover(graph, {})
    vc = sorted(list(vc))
    print("finish.")
    print(time.time() - st)
    print(vc)
