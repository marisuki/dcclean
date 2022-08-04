import database
import random
import CleanInterface_spk

"""
<class> CleanAdapter: Optimize cleaning process with block and merging.
                      Could enhance the following conditions:
                      1. large db: by blocking and repair some ahead to avoid large hyper graph
                                   construction, by database.exe_vio_accel with pyspark to detect error
                                   large decision: in self.process(), set: > 2k, could be changed
                      2. high error rate.
                      
                      Result in: higher PRF score and less time consumption.
                      Best fit for: 
                      make_up_fv=True: high redundancy.
                      extend_domain=True: continuous data.
<func> __init__: @input: noisy database; blocking_size: int, -1 when self-adapting blocks;
                         fv_eliminate: bool = True: use attribute cluster to repair (constraint_graph).
                         rec: blocking_size = 300 for error rate 0.01~0.05 and lower for higher error rate.
                        DB
                1/2 DB      1/2 DB
            1/4     1/4     1/4     1/4
            ...
            /|\ upgrade: random combination.
             |
        1/2^n ... ...
        leaf blocking=blocking_size, height = log|DB|    e.g. when participate based on 2.
        could be used part = 2, 10...
<func> blocking_adapter: consume db, when blocking_size == -1, output optimum blocking_size, 
                        blocking_size is generated with the max_deg and size of conflict hyper graph.
<func> atom_repair: @input: index_range: [start, end) (int*2 as tuple) @output: repair, fv
                    size: end-start may exceed time limit: (end-start)^2 ~ 1M
                    here, use pyspark to optimize large db conflict examination.
<func> dispatch_combine: random generate inner cell of a higher level in conflict reducing tree

<variable> noisy_db --> cleaned_db, blocking_size, fv_eliminate, (repair, fv): keep updating
"""


class TreeCleanAdapter:
    def __init__(self, db: database.database, blocking_size: int=-1, eliminate_fv: bool=False):
        self.db = db
        if blocking_size == -1:
            self.block = self.blocking_adapter(10, 10)
        else:
            self.block = blocking_size
        print("Block with: "+str(self.block))
        self.fv_eliminate = eliminate_fv
        self.ca = CleanInterface

    def blocking_adapter(self, deg_thr, cg_thr):
        max_deg, cg_size = 0, 0
        start = 100
        while max_deg < deg_thr and cg_size < cg_thr and start < 1000:
            samp = random.sample(range(len(self.db.table)), start)
            cg = self.ca.execute_conflict_graph()
            degree = self.ca.degree(cg)
            for k in degree:
                if max_deg < degree[k]:
                    max_deg = degree[k]
            cg_size = len(cg)
            start *= 2
        return start

    def process(self, blk_l, blk_r, queue):
        # print(self.db.table)
        # db_cpy = self.db.copy_with_partial_db(blk_l, blk_r)
        # print(len(db_cpy.table))
        # db_cpy = violation_free.violation_free(db_cpy
        #                                       , make_up_fv=self.fv_eliminate
        #                                       , large_db_acc=blk_r-blk_l > 2000)
        db_cpy = self.db.copy_with_partial_db(blk_l, blk_r)
        db_cpy = CleanInterface.dcClean(db_cpy).violation_free(make_up_fv=True, persist=True)
        if queue:
            queue.put((db_cpy.modifyHistory, db_cpy.fv, blk_l))
        return db_cpy

    def clean_by_block(self):
        from multiprocessing import Process, Queue
        atom_size = self.block
        while atom_size < len(self.db.table):
            queue = Queue()
            rep, fv = {}, {}
            blocks = len(self.db.table)*1.0/atom_size*1.0
            para = int(blocks)
            if blocks - para > 1e-5:
                para += 1
            result = []
            for i in range(para):
                result.append(Process(target=self.process
                                      , args=(atom_size*i, min(atom_size*(i+1), len(self.db.table)), queue)))
                result[-1].start()
            for i in range(para):
                result[i].join()
            for i in range(para):
                r, f, st = queue.get()
                rep[i] = {(x[0]+st, x[1]): r[x] for x in r}
                fv[i] = [(x[0]+st, x[1]) for x in f]
            for i in range(para):
                self.db.merge_repair(rep[i], fv[i])
            self.db.persist()
            atom_size *= 2
        if atom_size > len(self.db.table) > atom_size/2:
            res_db = self.process(-1, -1, None)
            r, f = res_db.modifyHistory, res_db.fv
            self.db.merge_repair(r, f)
            self.db.persist()
        return self.db

    def evaluate(self):
        print(self.db.evaluate())
