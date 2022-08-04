import database
import CleanInterface_spk
import dc_discovery

def main(error_rate = 0.01): 
    db = database.database()
    schema = [('FName', 'Enumerate'), ('LName', 'Enumerate'), ('Gender', 'Enumerate'), ('AreaCode', 'Enumerate'), 
            ('Phone', 'Enumerate'), ('City', 'Enumerate'), ('State', 'Enumerate'), ('Zip', 'Enumerate'), ('MaritalStatus', 'Enumerate'), 
            ('HasChild', 'Enumerate'), ('Salary', 'Value'), ('Rate', 'Value'), ('SingleExemp', 'Value'), 
            ('MarriedExemp', 'Value'), ('ChildExemp', 'Value'), ('Tax', 'Value')]
    value_type = [('Salary', 'Int'), ('Rate', 'Float'), ('SingleExemp', 'Int'), 
                ('MarriedExemp', 'Int'), ('ChildExemp', 'Int'), ('Tax', 'Float')]
    data_size = 1000
    #db.add_table("dataset/tax_clean_1k_dup1.csv", schema, ",", False, data_size)
    db.add_table("dataset/tax_clean_10k_0st.csv", schema, ",", False, data_size)
    #db.add_dc(constraints)
    print(db.schema)
    db.value_type(value_type)
    print(db.dom)
    #db.add_error(0.01, related=True)
    #print(len(db.error_change))
    #print(db.error_change)
    """
    8t; t0:(t[St] = t0[St] ^ t[MC] = t0[MC] ^ t[StAvg] != t0[StAvg])
    8t; t0:(t[OSt] = t0[OSt] ^ t[DSt] = t0[DSt] ^ t[DTime] >= t0[DTime])^t[ATime] <= t0[ATime]) ^ t[ETime] > t0[ETime])
    8t; t0:(t[Age] < t0[Age] ^ t[BirthYear] < t0[BirthYear])
    St – state, Ph – phone, G – gender, SE – single exemption, MC – measure code, OSt,
DSt – origin and destination state, Dtime, ATime, ETime – departure, arrival, and elapsed time, C – county.
    """
    constraints = [[("t1", "Salary", ">", "t2", "Salary"), ("t1", "Tax", "<", "t2", "Tax"), ("t1", "State", "=", "t2", "State")], 
                    #[("t1", "Zip", "=", "t2", "Zip"), ("t1", "State", "!=", "t2", "State")],
                    #[("t1", "Zip", "!=", "t2", "Zip"), ("t1", "State", "=", "t2", "State")],
                    [("t", "Salary", "<", "t", "Tax")], 
                    #[("t1", "State", "!=", "t2", "State"), ("t1", "Phone", "=", "t2", "Phone")],
                    #[("t1", "Salary", "!=", "t2", "Salary"), ("t1", "Phone", "=", "t2", "Phone")],
                    [("t1", "Tax", "!=", "t2", "Tax"), ("t1", "Phone", "=", "t2", "Phone")],
                    #[("t1", "Zip", "!=", "t2", "Zip"), ("t1", "Phone", "=", "t2", "Phone")],
                    #[("t1", "FName", "=", "t2", "FName"), ("t1", "LName", "=", "t2", "LName"), ("t1", "Phone", "!=", "t2", "Phone")]
                    #[("t1", "FName", "=", "t2", "FName"), ("t1", "LName", "=", "t2", "LName"), ("t1", "Salary", "=", "t2", "Salary"), ("t1", "Phone", "!=", "t2", "Phone")]
                    ]
    db.add_dc(dcs=constraints)
    db.add_error(0.1, related=True)
    #db.rule_based_error(error_rate=0.2, related=True, pointed_attrs=None, constraint={'Tax': [('>=', 'Salary', False)], 'Salary': [('<', 'Tax', False)]})
    #print(db.table)
    print(db.error_change)
    #noi = db.dc_noise(error_rate=0.2)
    #print(noi)


    print(db.dcs)

    Csp = CleanInterface_spk.DCClean(db)
    # Csp.detect().foreach(print)
    import time
    start = time.time()
    #print(database.vairance_tp(db.table))
    #db, call, cover = Csp.holistic(no_fv=False, dom_ext=False)
    db, call, cover = Csp.violation_free(no_fv=True, dom_ext=True, info_fetch=True)
    #db, call, cover = Csp.dc_variance(1.0, no_fv=True, vfree=True, metric='weight', dc_variant_candidate=noi)
    #db, call, cover = Csp.dc_variant(theta=1.0, no_fv=False, dom_ext=False, info_fetch=True)
    print("time cost:" + str(time.time() - start))
    db.persist()
    print(db.modifyHistory, db.fv, call)
    #db.persist_file("dataset/tax_clean_10k_0st", duplicate=True)
    #tp = Csp.table_parallel()
    #import statistical
    #aver = statistical.average(tp)
    #vari = statistical.variance(tp, aver)
    print(db.evaluate())
    print(Csp.detect(None).collect())
    

if __name__ == "__main__":
    main(0.01)