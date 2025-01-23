
from datetime import datetime
from dynamodb import select

from Util import Util

def calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    energy_lng = 0
    energy_hfo = 0
    energy_lfo = 0
    energy_mdo = 0
    energy_mgo = 0

    if total_lng > 0:
        lng_lcv =  float(fuel_oil_info_list["LNG_info_list"]["lcv"]["S"])
        energy_lng += total_lng * lng_lcv
    if total_hfo > 0:
        hfo_lcv =  float(fuel_oil_info_list["HFO_info_list"]["lcv"]["S"])
        energy_hfo += total_hfo * hfo_lcv
    if total_lfo > 0:
        lfo_lcv =  float(fuel_oil_info_list["LFO_info_list"]["lcv"]["S"])
        energy_lfo += total_lfo * lfo_lcv
    if total_mdo > 0:
        mdo_lcv =  float(fuel_oil_info_list["MDO_info_list"]["lcv"]["S"])
        energy_mdo += total_mdo * mdo_lcv
    if total_mgo > 0:
        mgo_lcv =  float(fuel_oil_info_list["MGO_info_list"]["lcv"]["S"])
        energy_mgo += total_mgo * mgo_lcv

    energy = (energy_lng + energy_hfo + energy_lfo + energy_mdo + energy_mgo) * float(eu_rate) / 100
    return energy

def calc_GHG_Max(year):
    year = int(year)
    if year <= 2029:
        target_rate = 2
    elif year <= 2034:
        target_rate = 6
    elif year <= 2039:
        target_rate = 14.5
    elif year <= 2044:
        target_rate = 31
    elif year <= 2049:
        target_rate = 62
    else:
        target_rate = 80

    GHG_Max = round(float(91.16 * (100 - float(target_rate)) / 100), 2)
    print(f"GHG_Max{type(GHG_Max)}: {GHG_Max}")
    return GHG_Max

def calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    sum_ghg = 0
    sum_foc = 0

    if total_lng > 0:
        lng_ghg_intensity =  float(fuel_oil_info_list["LNG_info_list"]["ghg_intensity"]["S"])
        sum_ghg += total_lng * lng_ghg_intensity
        sum_foc += total_lng
    if total_hfo > 0:
        hfo_ghg_intensity =  float(fuel_oil_info_list["HFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += total_hfo * hfo_ghg_intensity
        sum_foc += total_hfo
    if total_lfo > 0:
        lfo_ghg_intensity =  float(fuel_oil_info_list["LFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += total_lfo * lfo_ghg_intensity
        sum_foc += total_lfo
    if total_mdo > 0:
        mdo_ghg_intensity =  float(fuel_oil_info_list["MDO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += total_mdo * mdo_ghg_intensity
        sum_foc += total_mdo
    if total_mgo > 0:
        mgo_ghg_intensity =  float(fuel_oil_info_list["MGO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += total_mgo * mgo_ghg_intensity
        sum_foc += total_mgo

    GHG_Actual = 0
    if sum_foc != 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    cb_formatted = round(float(cb), 1)
    print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")
    return cb_formatted

def check_port_name(port_code):
    port_record = select.get_port_record(port_code)
    # print(f"port_record = {(port_record)}")
    port_name   = port_record[0]["port_name"]["S"]
    return port_name

def calc_sum_fuel(res_np):

    total_distance      = 0
    total_displacement  = 0
    count               = 0
    total_lng           = 0
    total_hfo           = 0
    total_lfo           = 0
    total_mdo           = 0
    total_mgo           = 0
    total_total_foc     = 0

    for i in range(len(res_np)):

        distance = Util.format_to_one_decimal(round(float(res_np[i]["log_distance"]["S"]), 1)) if 'log_distance' in res_np[i] and res_np[i]["log_distance"]["S"] != "" else 0.0
        displacement = Util.format_to_one_decimal(round(float(res_np[i]["displacement"]["S"]), 1)) if 'displacement' in res_np[i] and res_np[i]["displacement"]["S"] != "" else 0
        me_bog  = Util.format_to_one_decimal(round(float(res_np[i]["me_bog"]["S"]), 1))  if 'me_bog'  in res_np[i] and res_np[i]["me_bog"]["S"]  != "" else 0.0
        me_hfo  = Util.format_to_one_decimal(round(float(res_np[i]["me_hfo"]["S"]), 1))  if 'me_hfo'  in res_np[i] and res_np[i]["me_hfo"]["S"]  != "" else 0.0
        me_lsfo = Util.format_to_one_decimal(round(float(res_np[i]["me_lsfo"]["S"]), 1)) if 'me_lsfo' in res_np[i] and res_np[i]["me_lsfo"]["S"] != "" else 0.0
        me_do   = Util.format_to_one_decimal(round(float(res_np[i]["me_do"]["S"]), 1))   if 'me_do'   in res_np[i] and res_np[i]["me_do"]["S"]   != "" else 0.0
        me_lsgo = Util.format_to_one_decimal(round(float(res_np[i]["me_lsgo"]["S"]), 1)) if 'me_lsgo' in res_np[i] and res_np[i]["me_lsgo"]["S"] != "" else 0.0
        dg_bog  = Util.format_to_one_decimal(round(float(res_np[i]["dg_bog"]["S"]), 1))  if 'dg_bog'  in res_np[i] and res_np[i]["dg_bog"]["S"]  != "" else 0.0
        dg_hfo  = Util.format_to_one_decimal(round(float(res_np[i]["dg_hfo"]["S"]), 1))  if 'dg_hfo'  in res_np[i] and res_np[i]["dg_hfo"]["S"]  != "" else 0.0
        dg_lsfo = Util.format_to_one_decimal(round(float(res_np[i]["dg_foc"]["S"]), 1))  if 'dg_foc'  in res_np[i] and res_np[i]["dg_foc"]["S"]  != "" else 0.0
        dg_do   = Util.format_to_one_decimal(round(float(res_np[i]["dg_do"]["S"]), 1))   if 'dg_do'   in res_np[i] and res_np[i]["dg_do"]["S"]   != "" else 0.0
        dg_lsgo = Util.format_to_one_decimal(round(float(res_np[i]["dg_lsgo"]["S"]), 1)) if 'dg_lsgo' in res_np[i] and res_np[i]["dg_lsgo"]["S"] != "" else 0.0
        boiler_hfo  = Util.format_to_one_decimal(round(float(res_np[i]["boiler_hfo"]["S"]), 1))  if 'boiler_hfo'  in res_np[i] and res_np[i]["boiler_hfo"]["S"]  != "" else 0.0
        boiler_lsfo = Util.format_to_one_decimal(round(float(res_np[i]["boiler_foc"]["S"]), 1))  if 'boiler_foc'  in res_np[i] and res_np[i]["boiler_foc"]["S"]  != "" else 0.0
        boiler_do   = Util.format_to_one_decimal(round(float(res_np[i]["boiler_do"]["S"]), 1))   if 'boiler_do'   in res_np[i] and res_np[i]["boiler_do"]["S"]   != "" else 0.0
        boiler_lsgo = Util.format_to_one_decimal(round(float(res_np[i]["boiler_lsgo"]["S"]), 1)) if 'boiler_lsgo' in res_np[i] and res_np[i]["boiler_lsgo"]["S"] != "" else 0.0
        igg_go      = Util.format_to_one_decimal(round(float(res_np[i]["igg_go"]["S"]), 1))   if 'igg_go'   in res_np[i] and res_np[i]["igg_go"]["S"]   != "" else 0.0
        igg_lsgo    = Util.format_to_one_decimal(round(float(res_np[i]["igg_lsgo"]["S"]), 1)) if 'igg_lsgo' in res_np[i] and res_np[i]["igg_lsgo"]["S"] != "" else 0.0
        gcu_bog     = Util.format_to_one_decimal(round(float(res_np[i]["gcu_bog"]["S"]), 1))  if 'gcu_bog'  in res_np[i] and res_np[i]["gcu_bog"]["S"]  != "" else 0.0
        total_foc   = Util.format_to_one_decimal(round(float(res_np[i]["total_foc"]["S"]), 1))  if 'total_foc' in res_np[i] and res_np[i]["total_foc"]["S"]  != "" else 0.0

        lng = me_bog  + dg_bog  + gcu_bog
        hfo = me_hfo  + dg_hfo  + boiler_hfo
        lfo = me_lsfo + dg_lsfo + boiler_lsfo
        mdo = me_do   + dg_do   + boiler_do
        mgo = me_lsgo + dg_lsgo + boiler_lsgo + igg_go + igg_lsgo

        total_distance     += distance
        total_displacement += displacement
        count              += 1
        total_lng          += lng
        total_hfo          += hfo
        total_lfo          += lfo
        total_mdo          += mdo
        total_mgo          += mgo
        total_total_foc    += total_foc
    print(f"record_count: {(count)}")
    return total_distance, total_displacement/count, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, total_total_foc

def choise_period_noonreport(res_np, time_from, time_to):

    nr_list = []

    # 日付形式変更
    time_from_dt = datetime.strptime(time_from, "%Y/%m/%d %H:%M")
    tmp_time_from = time_from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    time_to_dt = datetime.strptime(time_to, "%Y/%m/%d %H:%M")
    tmp_time_to = time_to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")  
    
    for i in range(len(res_np)):
        record     = res_np[i]
        local_date = record['timestamp']['S']
        if tmp_time_from <= local_date and local_date < tmp_time_to:
            nr_list.append(record)

    return nr_list

def make_voyage_data(imo, Timestamp_from, Timestamp_to, res_np, fuel_oil_info_list, voyage_info):

    nr_list = []
    voyage_departure_time     = ""
    voyage_arrival_time       = ""
    voyage_total_distance     = 0
    voyage_total_displacement = 0
    voyage_count_displacement = 0
    voyage_total_lng          = 0
    voyage_total_hfo          = 0
    voyage_total_lfo          = 0
    voyage_total_mdo          = 0
    voyage_total_mgo          = 0
    voyage_total_foc          = 0
    voyage_total_eua          = 0
    voyage_total_energy       = 0
    voyage_total_cb           = 0
    voyage_count              = 0

    # leg-totalデータ取得
    res_leg = select.get_leg_total(imo, Timestamp_to[0:4])

    voyage_departure_time = voyage_info["departure_time"]["S"]
    voyage_arrival_time = voyage_info["arrival_time"]["S"]

    voyage_count += 1
    print(f"VoyageNo変化。ひとつ前のVoyage {(voyage_count)}を処理")
    print(f"voyage_count{type(voyage_count)}: {voyage_count}")

    for j in range(len(res_leg)):

        print(f"{(j + 1)}レグ目を確認")

        # leg-totalの各項目を取得
        departure_time = res_leg[j]["departure_time"]["S"]
        departure_year = departure_time[0:4]
        arrival_time   = res_leg[j]["arrival_time"]["S"]
        eu_rate        = res_leg[j]["eu_rate"]["S"]
        displacement   = Util.format_to_one_decimal(round(float(res_leg[j]["displacement"]["S"] if 'displacement' in res_leg[j] and res_leg[j]["displacement"]["S"] != "" else 0), 1))
        distance       = Util.format_to_one_decimal(round(float(res_leg[j]["distance"]["S"] if 'distance' in res_leg[j] and res_leg[j]["distance"]["S"] != "" else 0), 1))
        total_lng      = Util.format_to_one_decimal(round(float(res_leg[j]["total_lng"]["S"]), 1))
        total_hfo      = Util.format_to_one_decimal(round(float(res_leg[j]["total_hfo"]["S"]), 1))
        total_lfo      = Util.format_to_one_decimal(round(float(res_leg[j]["total_lfo"]["S"]), 1))
        total_mdo      = Util.format_to_one_decimal(round(float(res_leg[j]["total_mdo"]["S"]), 1))
        total_mgo      = Util.format_to_one_decimal(round(float(res_leg[j]["total_mgo"]["S"]), 1))
        total_foc      = Util.format_to_one_decimal(round(float(res_leg[j]["total_foc"]["S"]), 1))
        eua            = Util.format_to_one_decimal(round(float(res_leg[j]["eua"]["S"]), 1))
        # print(f"total_foc[{type(total_foc)}]: {total_foc}")

        # 各legがこのvoyageに関わっているかを確認する。
        print(f"voyage_departure_time: {(voyage_departure_time)}, voyage_arrival_time: {(voyage_arrival_time)}")
        print(f"leg_departure_time: {(departure_time)}, leg_arrival_time: {(arrival_time)}")

        if voyage_departure_time <= departure_time:

            if arrival_time <= voyage_arrival_time:
                print("このlegはvoyageの中で完結している")
                print(f"departure_time: {(departure_time)}")
                voyage_total_distance     += distance
                voyage_total_lng          += total_lng
                voyage_total_hfo          += total_hfo
                voyage_total_lfo          += total_lfo
                voyage_total_mdo          += total_mdo
                voyage_total_mgo          += total_mgo
                voyage_total_foc          += total_foc
                voyage_total_displacement += displacement
                voyage_count_displacement += 1
                voyage_total_eua          += eua

                energy = calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
                voyage_total_energy += energy

            elif voyage_arrival_time <= departure_time:
                print("このlegはvoyageの期間外")

            # legの途中までこのvoyage
            else:
                print("このlegは最初～途中までこのvoyage")
                # NoonReportを取得するための時刻が、timestamp(UTC)ではなくlocal_timeであるため
                nr_list = choise_period_noonreport(res_np, departure_time, arrival_time)
                # print(f"record_len: {(nr_list)}")

                distance, displacement, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, total_foc = calc_sum_fuel(nr_list)
                print(f"distance: {(distance)}, displacement: {(displacement)}, total_lng: {(total_lng)}, total_hfo: {(total_hfo)}, total_lfo:{(total_lfo)}, total_mdo: {(total_mdo)}, total_mgo: {(total_mgo)}, total_foc: {(total_foc)}")
                energy = calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
                voyage_total_distance     += distance
                voyage_total_lng          += total_lng
                voyage_total_hfo          += total_hfo
                voyage_total_lfo          += total_lfo
                voyage_total_mdo          += total_mdo
                voyage_total_mgo          += total_mgo
                voyage_total_foc          += total_foc
                voyage_total_displacement += displacement
                voyage_count_displacement += 1
                voyage_total_eua          += eua
                voyage_total_energy       += energy

        # legの途中からこのvoyage
        elif voyage_departure_time < arrival_time:
            print("このlegは途中～最後までこのvoyage")

            # NoonReportを取得するための時刻が、timestamp(UTC)ではなくlocal_timeであるため
            nr_list = choise_period_noonreport(res_np, voyage_departure_time, arrival_time)
            # print(f"record_len: {(nr_list)}")

            distance, displacement, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, total_foc = calc_sum_fuel(nr_list)
            print(f"distance: {(distance)}, displacement: {(displacement)}, total_lng: {(total_lng)}, total_hfo: {(total_hfo)}, total_lfo:{(total_lfo)}, total_mdo: {(total_mdo)}, total_mgo: {(total_mgo)}, total_foc: {(total_foc)}")
            energy = calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
            voyage_total_distance     += distance
            voyage_total_lng          += total_lng
            voyage_total_hfo          += total_hfo
            voyage_total_lfo          += total_lfo
            voyage_total_mdo          += total_mdo
            voyage_total_mgo          += total_mgo
            voyage_total_foc          += total_foc
            voyage_total_displacement += displacement
            voyage_count_displacement += 1
            voyage_total_eua          += eua
            voyage_total_energy       += energy

            print(f"voyage_total_distance: {(voyage_total_distance)}, voyage_total_hfo: {(voyage_total_hfo)}, total_foc: {(voyage_total_foc)}")

        else:
            print("このlegはvoyageの期間外")

    # voyageの合計値からCB, CBを算出する。
    print(f"voyage_total_distance: {(voyage_total_distance)}, voyage_total_hfo: {(voyage_total_hfo)}, total_foc: {(voyage_total_foc)}")
    print(f"voyage_total_eua{type(voyage_total_eua)}: {voyage_total_eua}")
    voyage_total_cb = calc_cb(departure_year, voyage_total_energy, voyage_total_lng, voyage_total_hfo, voyage_total_lfo, voyage_total_mdo, voyage_total_mgo, fuel_oil_info_list)

    # コストが必要なので算出する
    voyage_total_GHG = calc_GHG_Actual(voyage_total_lng, voyage_total_hfo, voyage_total_lfo, voyage_total_mdo, voyage_total_mgo, fuel_oil_info_list)
    voyage_total_cb_cost = 0
    if voyage_total_cb < 0:
        voyage_total_cb_cost = abs(voyage_total_cb) * 2400 / (voyage_total_GHG * 41000)
    voyage_displacement = voyage_total_displacement / voyage_count_displacement

    voyage_displacement   = str(round(float(voyage_displacement), 0))
    voyage_total_distance = str(round(float(voyage_total_distance), 0))
    voyage_total_lng      = str(round(float(voyage_total_lng), 1))
    voyage_total_hfo      = str(round(float(voyage_total_hfo), 1))
    voyage_total_lfo      = str(round(float(voyage_total_lfo), 1))
    voyage_total_mdo      = str(round(float(voyage_total_mdo), 1))
    voyage_total_mgo      = str(round(float(voyage_total_mgo), 1))
    voyage_total_foc      = str(round(float(voyage_total_foc), 1))
    voyage_total_eua      = str(round(float(voyage_total_eua), 1))
    voyage_total_cb       = str(round(float(voyage_total_cb), 1))
    voyage_total_cb_cost  = str(round(float(voyage_total_cb_cost), 0))

    dataset = {
        "imo"           : imo,
        "voyage_no"     : "",
        "departure_port": voyage_info["departure_port"]["S"],
        "departure_time": voyage_info["departure_time"]["S"],
        "arrival_port"  : voyage_info["arrival_port"]["S"],
        "arrival_time"  : voyage_info["arrival_time"]["S"],
        # "eu_rate"       : "", # voyage_eu_rate
        "displacement"  : voyage_displacement,
        "operator"      : voyage_info["operater"]["S"],
        "distance"      : voyage_total_distance,
        "total_lng"     : voyage_total_lng,
        "total_hfo"     : voyage_total_hfo,
        "total_lfo"     : voyage_total_lfo,
        "total_mdo"     : voyage_total_mdo,
        "total_mgo"     : voyage_total_mgo,
        "total_foc"     : voyage_total_foc,
        "GHG_Actual"    : voyage_total_GHG,
        "eua"           : voyage_total_eua,
        "cb"            : voyage_total_cb,
        "cb_cost"       : voyage_total_cb_cost
    }
    # print(f"dataset[{type(dataset)}]: {dataset}")

    return dataset

