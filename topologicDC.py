
def swap_op(op):
    if op == ">":
        return "<"
    elif op == ">=":
        return "<="
    elif op == "<":
        return ">"
    elif op == "<=":
        return ">="
    elif op == "=":
        return "="
    elif op == "!=":
        return "!="
    else:
        print("error op.")


def infer(op): # infer A ? C for A opx B op C, return {opx: op_next ?}
    if op == ">":
        return {">=": ">", ">": ">", "=": ">"}
    elif op == ">=":
        return {">=": ">=", "=": ">=", ">": ">"}
    elif op == "<":
        return {"<=": "<", "<": "<", "=": "<"}
    elif op == "<=":
        return {"<=": "<=", "=": "<=", "<": "<"}
    elif op == "=":
        return {"=": "=", ">=": ">=", "<=": "<=", "<": "<", ">": ">"}
    elif op == "!=":
        return {"!=": "!="}
    else:
        print("error op.")
        return None

def conflict(op):# pred: A op B + pred(dc) ==> trivial e.g., =, >
    if op == ">":
        return {"<=", "<", "="}
    elif op == ">=":
        return {"<", "!="}
    elif op == "<":
        return {">=", ">", "="}
    elif op == "<=":
        return {"!=", ">"}
    elif op == "=":
        return {"<", ">", "!="}
    elif op == "!=":
        return {"="}
    else:
        print("error op.")
        return None

class topologicDC:
    """
    topologicDC: find non-trivial dc and check the deduction of multiple dcs.
    """
    # (int, int, str, type=1): t1.A \phi t2.B, (int, int, str, type=2): t1.A \phi t1.B, (int, double, str, type=3): t.A \phi c
    def __init__(self, dc: [], opers=['>', '<', '=']):
        self.vertex = set()
        self.edge = dict()
        self.dc = dc
        self.single = 0
        self.combined = False
        self.opers = opers
        if dc: self.append_dc(dc)
    
    def append_dc(self, dc):
        # print(dc)
        for pred in dc:
            if pred[3] == 1: self.single -= 1
            elif pred[3] == 2: self.single += 1
            elif pred[3] == 3: self.single += 1
        if self.single == len(dc): self.single = True
        elif self.single == -1*len(dc): self.single = False
        else: 
            self.single, self.combined = False, True
        for pred in dc:
            if pred[3] == 1:
                self.vertex |= {(1, pred[0]), (2, pred[1])}
                self.vertex |= {(2, pred[0]), (1, pred[1])}
            elif pred[3] == 2:
                self.vertex |= {(1, pred[0]), (1, pred[1])}
                self.vertex |= {(2, pred[0]), (2, pred[1])}
            elif pred[3] == 3:
                self.vertex |= {(1, pred[0]), (0, pred[1])}
                self.vertex |= {(2, pred[0]), (0, pred[1])}
        for v in self.vertex: self.edge[v] = []
        for pred in dc:
            if pred[3] == 1:
                self.edge[(1, pred[0])].append((pred[2], (2, pred[1])))
                self.edge[(2, pred[1])].append((swap_op(pred[2]), (1, pred[0])))
                self.edge[(2, pred[0])].append((pred[2], (1, pred[1])))
                self.edge[(1, pred[1])].append((swap_op(pred[2]), (2, pred[0])))
            elif pred[3] == 2:
                self.edge[(1, pred[0])].append((pred[2], (1, pred[1])))
                self.edge[(1, pred[1])].append((swap_op(pred[2]), (1, pred[0])))
                self.edge[(2, pred[0])].append((pred[2], (2, pred[1])))
                self.edge[(2, pred[1])].append((swap_op(pred[2]), (2, pred[0])))
            elif pred[3] == 3:
                self.edge[(1, pred[0])].append((pred[2], (0, pred[1])))
                self.edge[(0, pred[1])].append((swap_op(pred[2]), (1, pred[0])))
                self.edge[(2, pred[0])].append((pred[2], (0, pred[1])))
                self.edge[(0, pred[1])].append((swap_op(pred[2]), (2, pred[0])))
    
    def dfs(self, start, end):
        path = []
        que = [(start, [])]
        if start not in self.vertex or end not in self.vertex: return None
        vis = [start]
        while len(que) != 0:
            now, pth = que[0]
            que = que[1:]
            if now == end: 
                path.append(pth)
                continue
            if now not in self.edge or len(self.edge[now]) == 0: continue
            for op, nxt in self.edge[now]:
                if nxt in vis: continue
                vis.append(nxt)
                cur = pth + [op]
                que.append((nxt, cur))
        if len(path) != 0: return path
        else: return None
    
    def deductive(self, st, ed):
        paths = self.dfs(st, ed)
        ans = {op: True for op in self.opers}
        if not paths: return ans
        for opx in ans:
            if not ans[opx]: continue
            for pth in paths:
                glb = pth[0]
                for op in pth:
                    if glb in infer(op): glb = infer(op)[glb]
                    else:
                        glb = None
                        break
                if glb: ans[opx] = ans[opx] & (opx != glb) & (opx not in conflict(glb))
        return ans

    def isTrivial(self, pred): 
        # 1. trivial to dc: added pred could be inferred by current preds: add this pred is useless to current dc
        # For example (1): dc contains A >= B = C > D, a pred: A > D is useless.
        # when a pred is trivial to dc, return ans[0] = False
        # 2. make current dc trivial: added pred make the dc becoming trivial
        # For example (2): dc contains A >= B = C > D, a pred: A < D ==> if added, no data will make all preds satisfied.
        # when a pred makes current dc trivial, return ans[1] = False
        triple = []
        if pred[3] == 1:
            triple.append([(1, pred[0]), (2, pred[1]), pred[2]])
            triple.append([(2, pred[0]), (1, pred[1]), pred[2]])
        elif pred[3] == 2:
            triple.append([(1, pred[0]), (1, pred[1]), pred[2]])
            triple.append([(2, pred[0]), (2, pred[1]), pred[2]])
        elif pred[3] == 3:
            triple.append([(1, pred[0]), (0, pred[1]), pred[2]])
            triple.append([(2, pred[0]), (0, pred[1]), pred[2]])
        ans = [True, True]
        for tup in triple:
            tmp = self.path_examine(tup[0], tup[1], pred[2])
            ans = [ans[0]&tmp[0], ans[1]&tmp[1]]
        return ans
    
    def path_examine(self, head, tail, op):
        paths = self.dfs(head, tail)
        ans = [True, True]
        if not paths: return ans
        for pth in paths:
            glb = pth[0]
            for op in pth:
                if glb in infer(op): glb = infer(op)[glb]
                else: 
                    glb = None
                    break
            if glb: ans = [ans[0]&(op != glb), ans[1]&(op not in conflict(glb))]
        return ans
    
    def nonTrivial_pred(self, schema, single_dc=True):# support binary, single
        ans = []
        candidate = []
        print(schema)
        if single_dc:
            for al in schema:
                for ar in schema:
                    if al >= ar: continue
                    for op in self.opers:
                        candidate += [(al, ar, op, 2)]
        else:
            for al in schema:
                for ar in schema:
                    if al > ar: continue
                    for op in self.opers:
                        candidate += [(al, ar, op, 1)]
        #print(candidate)
        #print(self.single, self.combined)
        for check_iter in candidate:
            res = self.isTrivial(check_iter)
            #self.deductive(check_iter[0][0], check_iter[0][1])
            if res[1]: ans.append(check_iter)
        return ans
    
    def type_filter(self, pred_space, data_precision):
        # pred_space: [[(1, attr1), (2, attr2), op], [...], ...]
        # dataprecision from db, attr id -> float: precision of certain attrs.
        # type_cluster: [[attr_i1,...attr_in], [attr_j1, ...]] \bigcup_mn attr_mn \subseteq schema.
        ans = []
        for pred in pred_space:
            if data_precision[pred[0][1]] == data_precision[pred[1][1]] > 0.5:
                if pred[2] == "=" or pred[2] == '!=': ans.append(pred)
            elif data_precision[pred[0][1]] == data_precision[pred[1][1]] < 0.5:
                ans.append(pred)
        return ans
    
    def predSpace2Preds(self, pred_space):
        # convert predspace: [[(1, attr1), (2, attr2), op], [...], ...] to [(attr1, attr2, op, type)]
        # (int, int, str, type=1): t1.A \phi t2.B, (int, int, str, type=2): t1.A \phi t1.B, (int, double, str, type=3): t.A \phi c
        ans = []
        for pred in pred_space:
            if pred[0][0] == pred[1][0]: pred_t = 2
            elif pred[1][0] == 0: pred_t = 3
            elif pred[0][0] != pred[1][0]: pred_t = 1
            else: print("PredSpace type error.")
            ans.append((pred[0][1], pred[1][1], pred[2], pred_t))
        ans = list(set(ans))
        return ans

