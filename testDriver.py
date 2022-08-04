# import CleanAdapter
import database
import time
import CleanInterface_spk
# import CleanInterface


def test_hosp(data_error_rate=0.01, dc_error_rate=0.10, data_size=1000, cv_mertics='unit', 
                vfree=True, nfv=True, cv_tol=1.0, cv=True, v_repair=False, constraint_bef=-1):
    db = database.database()
    constraints = [[("t1", "Condition", "=", "t2", "Condition"), ("t1", "MeasureName", "=", "t2", "MeasureName"), ("t1", "HospitalType", "!=", "t2", "HospitalType")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "ZIPCode", "!=", "t2", "ZIPCode")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "PhoneNumber", "!=", "t2", "PhoneNumber")],
                   [("t1", "MeasureCode", "=", "t2", "MeasureCode"),  ("t1", "MeasureName", "!=", "t2", "MeasureName")],
                   [("t1", "MeasureCode", "=", "t2", "MeasureCode"), ("t1", "StateAvg", "!=", "t2", "StateAvg")],
                   [("t1", "ProviderNumber", "=", "t2", "ProviderNumber"), ("t1", "HospitalName", "!=", "t2", "HospitalName")],
                   [("t1", "MeasureCode", "=", "t2", "MeasureCode"), ("t1", "Condition", "!=", "t2", "Condition")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "Address1", "!=", "t2", "Address1")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "HospitalOwner", "!=", "t2", "HospitalOwner")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "ProviderNumber", "!=", "t2", "ProviderNumber")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "PhoneNumber", "=", "t2", "PhoneNumber"), ("t1", "HospitalOwner", "=", "t2", "HospitalOwner"), ("t1", "State", "!=", "t2", "State")],
                   [("t1", "City", "=", "t2", "City"), ("t1", "CountryName", "!=", "t2", "CountryName")],
                   [("t1", "ZIPCode", "=", "t2", "ZIPCode"), ("t1", "EmergencyService", "!=", "t2", "EmergencyService")],
                   [("t1", "HospitalName", "=", "t2", "HospitalName"), ("t1", "City", "!=", "t2", "City")],
                   [("t1", "MeasureName", "=", "t2", "MeasureName"), ("t1", "MeasureCode", "!=", "t2", "MeasureCode")]]
    schema = [("ProviderNumber", "Enumerate"), ("HospitalName", "Enumerate"), ("Address1", "Enumerate"), ("Address2", "Enumerate"), ("Address3", "Enumerate"), ("City", "Enumerate"), ("State", "Enumerate"), ("ZIPCode", "Enumerate"), ("CountryName", "Enumerate"), ("PhoneNumber", "Enumerate"), ("HospitalType", "Enumerate"), ("HospitalOwner", "Enumerate"), ("EmergencyService", "Enumerate"), ("Condition", "Enumerate"), ("MeasureCode", "Enumerate"), ("MeasureName", "Enumerate"), ("Score", "Enumerate"), ("Sample", "Enumerate"), ("StateAvg", "Enumerate")]
    db.add_table("dataset/cleanedHosp10w1202.csv", schema, ",", False, data_size)
    if constraint_bef >= 1 and constraint_bef <= len(constraints): db.add_dc(constraints[:constraint_bef])
    else: db.add_dc(constraints)
    db.value_type([])
    db.add_error(data_error_rate, related=True)
    #db.add_error_tupWise(tup_err_rate = 0.01, num_error=data_error_rate, related=True)
    print(db.error_change)
    noi = db.dc_noise(error_rate=dc_error_rate)
    ci = CleanInterface_spk.DCClean(db)
    import time
    start = time.time()
    if cv:
        db, call, cover = ci.dc_variance(cv_tol, no_fv=nfv, vfree=vfree, metric=cv_mertics, 
                                    dc_variant_candidate=noi)
    else:
        if vfree: db, call, cover = ci.violation_free(no_fv=nfv, info_fetch=True)
        elif v_repair: db, call, cover = ci.v_repair()
        else: db, call, cover = ci.holistic(no_fv=nfv, dom_ext=False)
    delta_t = time.time() - start
    db.persist()
    eva = db.evaluate()
    eva = list(eva)
    print(eva)
    evaluation = {'precision':eva[0], 'recall':eva[1], 'f_score':eva[2], 'dis_tr':eva[3], 'mnad':eva[4],
                    'accuracy':eva[5], 'modif':eva[6], 'n.fv':eva[7], 'delta_t':delta_t, 'call': call}
    return evaluation


def test_hosp_all(data_error_rate=0.01, dc_error_rate=0.10, data_size=1000, cv_mertics='unit', 
                vfree=True, nfv=True, cv_tol=1.0, cv=True, v_repair=False, constraint_bef=-1):
    evaluation_all = dict()
    for i in range(2):
        evalx = test_hosp(data_error_rate, dc_error_rate, data_size, cv_mertics, vfree, nfv, cv_tol, cv, v_repair, constraint_bef)
        for k in evalx:
            if k not in evaluation_all: evaluation_all[k] = evalx[k]
            else:
                evaluation_all[k] += evalx[k]
    for k in evaluation_all:
        evaluation_all[k] = evaluation_all[k]/2.0
    return evaluation_all

def to_str(evaluation):
    ans = ""
    for k in evaluation:
        ans += str(evaluation[k]) + " "
    return ans

if __name__ == "__main__":
    dc_error_rate=0.10
    data_size=1000
    cv_tol=1.0
    mappings = {'vfree': {'vfree': True, 'cv': False, 'nfv': False, 'v_repair': False}, 
                'nofv': {'vfree': True, 'cv': False, 'nfv': True, 'v_repair': False},
                'holistic': {'vfree': False, 'cv': False, 'nfv': False, 'v_repair': False},
                'v_repair': {'vfree': False, 'cv': False, 'nfv': False, 'v_repair': True}
                }
    # varied error rate
    for i in range(1, 16):
        f_res, f_meta = open("hosp_numFds/fd-" + str(i) + ".txt", "w"), open("hosp_numFds/fd-meta-" + str(i) + ".txt", "w")
        data_error_rate = 1/100
        #col_errors = 1*i
        cv_tol = 1.0
        data_size = 1000
        constraint_befx = i
        for k in mappings:
            if k == 'vfree' or k == 'nofv':# or k == 'holistic':
                print(k + " var=" +  str(cv_tol))
                #eva_o = test_hosp_all(data_error_rate, dc_error_rate, data_size, cv_mertics='unit', 
                #                    vfree=mappings[k]['vfree'], nfv=mappings[k]['nfv'], cv_tol=cv_tol, cv=False, v_repair=mappings[k]['v_repair'])
                #out_o = to_str(eva_o)
                #f_res.write(out_o + "\n")
                #f_meta.write(k + "\n")
                eva_u = test_hosp_all(data_error_rate, dc_error_rate, data_size, cv_mertics='unit', 
                                    vfree=mappings[k]['vfree'], nfv=mappings[k]['nfv'], cv_tol=cv_tol, cv=True, 
                                    v_repair=mappings[k]['v_repair'], constraint_bef=constraint_befx)
                eva_w = test_hosp_all(data_error_rate, dc_error_rate, data_size, cv_mertics='weight', 
                                    vfree=mappings[k]['vfree'], nfv=mappings[k]['nfv'], cv_tol=cv_tol, cv=True, 
                                    v_repair=mappings[k]['v_repair'], constraint_bef=constraint_befx)
                #eva_cv = {ek: max(eva_u[ek], eva_w[ek]) for ek in eva_u}
                #out_u = to_str(eva_u)
                #f_res.write(out_u + "\n")
                #f_meta.write(k + "cv-unit\n")
                out_u, out_w = to_str(eva_u), to_str(eva_w)
                f_res.write(out_u + "\n" + out_w + "\n")
                f_meta.write(k + "cv-unit\n" + k + "cv-weight\n")
                #print(out_o + "\n" + out_u + "\n" + out_w + "\n")
                #print(k + "\n" + k + "cv-unit\n" + k + "cv-weight\n")
                #print(out_u + "\n")
                #print(k + "cv-unit\n")
            else:
                eva_o = test_hosp_all(data_error_rate, dc_error_rate, data_size, cv_mertics='unit', 
                                    vfree=mappings[k]['vfree'], nfv=mappings[k]['nfv'], cv_tol=cv_tol, cv=False, 
                                    v_repair=mappings[k]['v_repair'], constraint_bef=constraint_befx)
                out_o = to_str(eva_o)
                f_res.write(out_o + "\n")
                f_meta.write(k + "\n")
                print(out_o + "\n")
                print(k + "\n")
        f_res.close()
        f_meta.close()


"""
test_hosp(data_error_rate=0.01, dc_error_rate=0.10, data_size=1000, cv_mertics='unit', 
                vfree=True, nfv=True, cv_tol=1.0, cv=True, v_repair=False)
    res = open("test/hosp/nofraction-vfree-error-1000-001-01-1211.csv", "w+")
    result = {i: {"p":[], "r": [], "f": [], "t": [], "cnf": [], "cf": [], "c":[], "call_p":[], "call_n": [], "call":[]} for i in range(20)}
    mp = {0: "fv", 1: "nfv", 2: "tree"} 
    for j in range(1, 11):
        x = test(error_rate=j*1.0/100.0, db_size=1000)
        result[j]["p"].append(x[0][0])
        result[j]["r"].append(x[0][1])
        result[j]["f"].append(x[0][2])
        result[j]["cnf"].append(x[0][5])
        result[j]["cf"].append(x[0][6])
        result[j]["c"].append(x[0][5] + x[0][6])
        result[j]["t"].append(x[1])
        result[j]["call_p"].append(x[2][0])
        result[j]["call_n"].append(x[2][1])
        result[j]["call"].append(x[2][0]+x[2][1])
        print("Position {}, Precision={}, Recall={}, F={}, t={}.".format(j, x[0][0], x[0][1], x[0][2], x[1]))
    for j in result:
        op = "" + str(j) + " "
        for evu in result[j]:
            ans = mp[1] + "." + evu + ","
            for item in result[j][evu]:
                ans += str(item) + ","
                op += str(item) + " "
            ans += "\n"
            # res.write(ans)
        op += "\n"
        res.write(op)
    res.close()
"""