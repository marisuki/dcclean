import database
import CleanInterface_spk
import dc_discovery

def main(error_rate = 0.01): 
    db = database.database()
    constraints = [[("t1", "Condition", "=", "t2", "Condition"), ("t1", "MeasureName", "=", "t2", "MeasureName"),
                    ("t1", "HospitalType", "!=", "t2", "HospitalType")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "ZIPCode", "!=", "t2", "ZIPCode")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"),
                    ("t1", "PhoneNumber", "!=", "t2", "PhoneNumber")],
                   [("t1", "MeasureCode", "=", "t2", "MeasureCode"), ("t1", "MeasureName", "!=", "t2", "MeasureName")],
                   [("t1", "MeasureCode", "=", "t2", "MeasureCode"), ("t1", "StateAvg", "!=", "t2", "StateAvg")],
                   [("t1", "ProviderNumber", "=", "t2", "ProviderNumber"),
                    ("t1", "HospitalName", "!=", "t2", "HospitalName")],
                   [("t1", "MeasureCode", "=", "t2", "MeasureCode"), ("t1", "Condition", "!=", "t2", "Condition")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "Address1", "!=", "t2", "Address1")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"),
                    ("t1", "HospitalOwner", "!=", "t2", "HospitalOwner")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"),
                    ("t1", "ProviderNumber", "!=", "t2", "ProviderNumber")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "PhoneNumber", "=", "t2", "PhoneNumber"),
                    ("t1", "HospitalOwner", "=", "t2", "HospitalOwner"), ("t1", "State", "!=", "t2", "State")],
                   [("t1", "City", "=", "t2", "City"), ("t1", "CountryName", "!=", "t2", "CountryName")],
                   [("t1", "ZIPCode", "=", "t2", "ZIPCode"),
                    ("t1", "EmergencyService", "!=", "t2", "EmergencyService")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "City", "!=", "t2", "City")],
                   [("t1", "MeasureName", "=", "t2", "MeasureName"), ("t1", "MeasureCode", "!=", "t2", "MeasureCode")]]
    schema = [("ProviderNumber", "Enumerate"), ("HospitalName", "Enumerate"), ("Address1", "Enumerate"),
              ("Address2", "Enumerate"), ("Address3", "Enumerate"), ("City", "Enumerate"), ("State", "Enumerate"),
              ("ZIPCode", "Enumerate"), ("CountryName", "Enumerate"), ("PhoneNumber", "Enumerate"),
              ("HospitalType", "Enumerate"), ("HospitalOwner", "Enumerate"), ("EmergencyService", "Enumerate"),
              ("Condition", "Enumerate"), ("MeasureCode", "Enumerate"), ("MeasureName", "Enumerate"),
              ("Score", "Enumerate"), ("Sample", "Enumerate"), ("StateAvg", "Enumerate")]
    data_size = 1000
    db.add_table("dataset/cleanedHosp10w1202.csv", schema, ",", True, data_size)
    db.add_dc(constraints[:13])
    print(db.schema)
    db.value_type([])
    print(db.dom)
    db.add_error(0.01, related=True)
    #db.add_error_tupWise(tup_err_rate = 0.01, num_error=1, related=True)
    print(len(db.error_change))
    print(db.error_change)
    print(db.dcs)
    noi_a, noi_d = db.dc_noise(error_rate=0.1, add_pred=False, del_pred=True)
    print(noi_a, noi_d)
    print(db.dcs)
    """
    predSpace = dc_discovery.PredicateSpace(db, opers=['='])
    for dc in db.dcs: predSpace.add_existing_dcs(dc[1])
    preds_AB_1, preds_AB_2 = predSpace.AB_1_preds(), predSpace.AB_2_preds(fd=True)
    print(preds_AB_2)

    predVariant = dc_discovery.energyEM(db)
    
    for dc in db.dcs: print(predVariant.rule_variance(dc[1], preds_AB_2, preds_AB_1, loc=True))
    """
    ci = CleanInterface_spk.DCClean(db)
    import time
    start = time.time()
    #db, call, cover = ci.holistic(no_fv=False)
    #db, call, cover = ci.violation_free(no_fv=False, info_fetch=True)
    db, call, cover = ci.dc_variance(1.0, no_fv=True, vfree=True, metric='unit', dc_variant_candidate=(noi_a, noi_d),
                                     max_del=None)
    # db, call, cover = ci.v_repair()
    #print(db.evaluate())
    #print(database.set_prf_calc(truth={x[0] for x in db.error_change}, x=cover))
    #print(database.set_prf_calc(truth={x[0][0] for x in db.error_change}, x={x[0] for x in cover}))
    print("time cost:" + str(time.time() - start))
    db.persist()
    print(db.modifyHistory, db.fv, call)
    print(time.time()-start)
    db.persist_file("buffer/cleanedHosptestw1206") 
    print(db.evaluate())
    print(ci.detect(None).collect() == [])


if __name__ == "__main__":
    main(0.01)