import database
import CleanInterface
import CleanInterface_spk
import CDDDiscovery



if __name__ == "__main__":
    db = database.database()
    db.add_table(path="dataset/gps/gps.csv",
             schema=[("Time", "Enumerate"), ("OLU", "Value"), ("OLA", "Value"),
                     ("TLU", "Value"), ("TLA", "Value")],
             regex=",", first_line_omit=True)
    db.value_type([("OLU", "Float"), ("OLA", "Float"), ("TLU", "Float"), ("TLA", "Float")])

    # print(db.table)
    # print(db.value_map)
    # print(db.schema)
    # print(db.data_precision)

    # db.add_error(0.2)
    # print(db.table)
    # print(db.error_change)

    # db.add_dc(dcs=[[("t1", "Income", ">", "t2", "Income"), ("t1", "Tax", "<", "t2", "Tax")],
    #               [("t", "Income", "<", "t", "Tax")]])
    # print(db.dcs)

    # Csp = CleanInterface_spk.DCClean(db)
    # Csp.detect().foreach(print)
    #db, cal = Csp.violation_free(no_fv=True, dom_ext=True)
    #db.persist()
    #print(db.evaluate())
    #print(cal)
    #db.persist_file("cleanedIncome-testw1206")
    import time
    start = time.time()
    disc = CDDDiscovery.DCDiscovery(db)
    print(disc.speed_constraints((0, 2), 0.9))
    print(time.time()-start)
    # print(disc.dd2((4, 5), 20, 0.6))
    # print(disc.single_row_cdd((4, 5), 3, 20, 0.6))
    # print(disc.single_row_dd((4, 5), 20, 0.6))
    
