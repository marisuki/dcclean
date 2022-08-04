import database
import CleanInterface
import CleanInterface_spk
import CDDDiscovery

if __name__ == "__main__":
    schema=[('age', 'Enumerate'), ('CLS', 'Enumerate'), ('detailed industry recode', 'Enumerate'), 
             ('detailed occupation recode', 'Enumerate'), 
             ('ED', 'Enumerate'), ('wage per hour', 'Enumerate'), (' enroll in edu inst last wk', 'Enumerate'), 
             ('MR', 'Enumerate'), ('major industry code', 'Enumerate'), ('OCC', 'Enumerate'), ('race', 'Enumerate'), 
             ('hispanic origin', 'Enumerate'), ('GEN', 'Enumerate'), ('member of a labor union', 'Enumerate'), 
             ('reason for unemployment', 'Enumerate'), ('full or part time employment stat', 'Enumerate'), 
             ('capital gains', 'Enumerate'), ('capital losses', 'Enumerate'), ('dividends from stocks', 'Enumerate'), 
             ('tax filer stat', 'Enumerate'), ('region of previous residence', 'Enumerate'), ('state of previous residence', 'Enumerate'), 
             ('detailed household and family stat', 'Enumerate'), ('detailed household summary in household', 'Enumerate'), 
             ('migration code-change in msa', 'Enumerate'), ('migration code-change in reg', 'Enumerate'), 
             ('migration code-move within reg', 'Enumerate'), ('live in this house 1 year ago', 'Enumerate'), 
             ('migration prev res in sunbelt', 'Enumerate'), ('num persons worked for employer', 'Enumerate'), 
             ('family members under 18', 'Enumerate'), ('country of birth father', 'Enumerate'), ('country of birth mother', 'Enumerate'), 
             ('country of birth self', 'Enumerate'), ('citizenship', 'Enumerate'), ('own business or self employed', 'Enumerate'), 
             ("fill inc questionnaire for veteran's admin", 'Enumerate'), ('veterans benefits', 'Enumerate'), ('NUL', 'Enumerate'), 
             ('weeks worked in year', 'Value'), ('year', 'Enumerate'), ('SAL', 'Enumerate'), ('Income', 'Value'), ('Tax', 'Value')]

    
    db = database.database()
    data_size = 1000
    db.add_table("dataset/cleanedCensus10w1206.csv",
             schema,
             ",", False, data_size)
    db.value_type([("Income", "Float"), ("Tax", "Float"), ('weeks worked in year', 'Int')])

    # print(db.table)
    #print(db.value_map)
    #print(db.schema)
    #print(db.data_precision)
    db.add_dc(dcs=[[("t1", "Income", ">", "t2", "Income"), ("t1", "Tax", "<", "t2", "Tax")],
                    #[("t1", )]
                    [("t", "Income", "<", "t", "Tax")]])

    #db.export_proj("buffer/census-IncTax-cleaned.csv", proj_attrs=[len(schema)-1, len(schema)-2])
    db.add_error(0.02, related=True)
    # print(db.table)
    print(db.error_change)


    print(db.dcs)

    Csp = CleanInterface_spk.DCClean(db)
    # Csp.detect().foreach(print)
    import time
    start = time.time()
    #db, call = Csp.holistic(no_fv=False, dom_ext=False)
    db, call, cover = Csp.violation_free(no_fv=True, dom_ext=False, info_fetch=True)
    #db, call, cover = Csp.dc_variant(theta=1.0, no_fv=False, dom_ext=False, info_fetch=True)
    print(database.set_prf_calc(truth={x[0] for x in db.error_change}, x=cover))
    print(database.set_prf_calc(truth={x[0][0] for x in db.error_change}, x={x[0] for x in cover}))
    print("time cost:" + str(time.time() - start))
    db.persist()
    print(db.modifyHistory, db.fv, call)
    db.persist_file("buffer/cleanedCensustest10w1211")
    #tp = Csp.table_parallel()
    #import statistical
    #aver = statistical.average(tp)
    #vari = statistical.variance(tp, aver)
    print(db.evaluate())
    print(Csp.detect(None).collect())
    
    # disc = CDDDiscovery.DCDiscovery(db)
    # print(disc.dd2((4, 5), 20, 0.6))
    # print(disc.single_row_cdd((4, 5), 3, 20, 0.6))
    # print(disc.single_row_dd((4, 5), 20, 0.6))
    
