
from datetime import datetime
from dynamodb import select

from Util import Util

def calc_EUA(year, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):

    # EUAの算出
    co2_lng = 0
    co2_hfo = 0
    co2_lfo = 0
    co2_mdo = 0
    co2_mgo = 0
    eu_ets_rate = 0
    eua = 0

    # EU-ETS対象割合を確認
    if year == "2024":
        eu_ets_rate = 40
    elif year == "2025":
        eu_ets_rate = 70
    else:
        eu_ets_rate = 100

    print(f"eu_ets_rate: {(eu_ets_rate)}")
    if total_lng > 0:
        lng_co2_factor =  float(fuel_oil_info_list["LNG_info_list"]["emission_factor"]["S"])
        co2_lng = total_lng * lng_co2_factor
    if total_hfo > 0:
        hfo_co2_factor =  float(fuel_oil_info_list["HFO_info_list"]["emission_factor"]["S"])
        co2_hfo = total_hfo * hfo_co2_factor
    if total_lfo > 0:
        lfo_co2_factor =  float(fuel_oil_info_list["LFO_info_list"]["emission_factor"]["S"])
        co2_lfo = total_lfo * lfo_co2_factor
    if total_mdo > 0:
        mdo_co2_factor =  float(fuel_oil_info_list["MDO_info_list"]["emission_factor"]["S"])
        co2_mdo = total_mdo * mdo_co2_factor
    if total_mgo > 0:
        mgo_co2_factor =  float(fuel_oil_info_list["MGO_info_list"]["emission_factor"]["S"])
        co2_mgo = total_mgo * mgo_co2_factor

    # CO2の総排出量(MT)
    total_co2 = co2_lng + co2_hfo + co2_lfo + co2_mdo + co2_mgo
    print(f"total_co2{type(total_co2)}: {total_co2}")
    eua       = total_co2 * float(eu_ets_rate) / 100
    return str(eua)

def calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
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

    energy = (energy_lng + energy_hfo + energy_lfo + energy_mdo + energy_mgo)
    return energy

def calc_GHG_Max(year):
    year = int(year)
    if year <= 2024:
        target_rate = 0
    elif year <= 2029:
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
    return cb

def check_eu_rate(port_code, eta_port_code):
    eu_rate = 0
    departure_port_record  = select.get_port_record(port_code)
    departure_port_eu_flag = departure_port_record[0]["eu_flg"]["S"] if len(departure_port_record) != 0 else 0
    arrival_port_record    = select.get_port_record(eta_port_code)
    arrival_port_eu_flag   = arrival_port_record[0]["eu_flg"]["S"] if len(arrival_port_record) != 0 else 0
    if departure_port_eu_flag == "1" and arrival_port_eu_flag == "1":
        eu_rate = 100
    elif departure_port_eu_flag == "1" or arrival_port_eu_flag == "1":
        eu_rate = 50
    return eu_rate

def check_port_name(port_code):
    port_record = select.get_port_record(port_code)
    # print(f"port_record = {(port_record)}")
    port_name   = port_record[0]["port_name"]["S"]
    return port_name

def make_leg_data(imo, Timestamp_from, Timestamp_to, res_np, fuel_oil_info_list, leg_info):

    # 該当期間のNoonReportがゼロの場合、空で返す
    dataset = []

    leg_count             = 0
    eu_rate               = leg_info["eu_rate"]["S"]
    noonreport_count      = 0
    sum_displacement      = 0
    avg_displacement      = 0
    leg_type              = leg_info["leg_type"]["S"]
    total_distance        = 0
    total_lng             = 0
    total_hfo             = 0
    total_lfo             = 0
    total_mdo             = 0
    total_mgo             = 0
    total_total_foc       = 0
    eta_local_date        = ""
    eua                   = 0
    cb                    = 0

    # datetimeをformatを指定して文字列に変換
    dt_now = datetime.now()
    print(f"dt_now{type(dt_now)}: {dt_now}")
    
    dt_now_str = dt_now.strftime('%Y-%m-%dT%H:%M:%SZ')
    # print(f"dt_now_str{type(dt_now_str)}: {dt_now_str}")
    year_now = dt_now_str[0:4]
    print(f"year_now{type(year_now)}: {year_now}")
    
    #timestampの西暦部分を確認
    year_timestamp_from = Timestamp_from[0:4]
    # print(f"year_timestamp{type(year_timestamp)}: {year_timestamp}")

    # NoonReportのレコード数分回す
    for i in range(len(res_np)):

        print(f"NoonReportカウント: {(i)}")

        # NoonReportの各項目を取得
        distance = Util.format_to_one_decimal(round(float(res_np[i]["log_distance"]["S"]), 1)) if 'log_distance' in res_np[i] and res_np[i]["log_distance"]["S"] != "" else 0.0
        displacement = Util.format_to_one_decimal(round(float(res_np[i]["displacement"]["S"]), 1)) if 'displacement' in res_np[i] and res_np[i]["displacement"]["S"] != "" else 0
        me_bog  = Util.format_to_one_decimal(round(float(res_np[i]["me_bog"]["S"]), 1))  if 'me_bog'  in res_np[i] and res_np[i]["me_bog"]["S"]  != "" else 0.0
        me_hfo  = Util.format_to_one_decimal(round(float(res_np[i]["me_hfo"]["S"]), 1))  if 'me_hfo'  in res_np[i] and res_np[i]["me_hfo"]["S"]  != "" else 0.0
        me_lsfo = Util.format_to_one_decimal(round(float(res_np[i]["me_lsfo"]["S"]), 1)) if 'me_lsfo' in res_np[i] and res_np[i]["me_lsfo"]["S"] != "" else 0.0
        me_do   = Util.format_to_one_decimal(round(float(res_np[i]["me_do"]["S"]), 1))   if 'me_do'   in res_np[i] and res_np[i]["me_do"]["S"]   != "" else 0.0
        me_lsgo = Util.format_to_one_decimal(round(float(res_np[i]["me_lsgo"]["S"]), 1)) if 'me_lsgo' in res_np[i] and res_np[i]["me_lsgo"]["S"] != "" else 0.0
        dg_bog  = Util.format_to_one_decimal(round(float(res_np[i]["dg_bog"]["S"]), 1))  if 'dg_bog'  in res_np[i] and res_np[i]["dg_bog"]["S"]  != "" else 0.0
        dg_hfo  = Util.format_to_one_decimal(round(float(res_np[i]["dg_hfo"]["S"]), 1))  if 'dg_hfo'  in res_np[i] and res_np[i]["dg_hfo"]["S"]  != "" else 0.0
        dg_lsfo = Util.format_to_one_decimal(round(float(res_np[i]["ge_foc"]["S"]), 1))  if 'ge_foc'  in res_np[i] and res_np[i]["ge_foc"]["S"]  != "" else 0.0
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
        eta_local_date = res_np[i]["eta_local_date"]["S"]   if 'eta_local_date'  in res_np[i] and res_np[i]["eta_local_date"]["S"]      != "" else ""

        # print(f"total_foc[{type(total_foc)}]: {total_foc}")

        # 計算に必要なデータを加算
        lng = me_bog  + dg_bog  + gcu_bog
        hfo = me_hfo  + dg_hfo  + boiler_hfo
        lfo = me_lsfo + dg_lsfo + boiler_lsfo
        mdo = me_do   + dg_do   + boiler_do
        mgo = me_lsgo + dg_lsgo + boiler_lsgo + igg_go + igg_lsgo       
        sum_displacement += displacement
        noonreport_count += 1
        total_distance   += distance
        total_lng        += lng
        total_hfo        += hfo
        total_lfo        += lfo
        total_mdo        += mdo
        total_mgo        += mgo
        total_total_foc  += total_foc

        if i == (len(res_np) - 1):
            # 最終レコードの場合の処理

            # 各種燃料消費量にEU Rateを考慮する
            total_lng = total_lng * float(eu_rate) / 100
            total_hfo = total_hfo * float(eu_rate) / 100
            total_lfo = total_lfo * float(eu_rate) / 100
            total_mdo = total_mdo * float(eu_rate) / 100
            total_mgo = total_mgo * float(eu_rate) / 100
            total_total_foc = total_total_foc * float(eu_rate) / 100

            print("Leg終了、登録処理開始")
            print(f"year_timestamp:{(year_timestamp_from)}, eu_rate:{(eu_rate)}, total_lng:{(total_lng)}, total_hfo:{(total_hfo)}, total_lfo:{(total_lfo)}, total_mdo:{(total_mdo)}, total_mgo:{(total_mgo)}")
            #EUA, CBを算出
            eua        = calc_EUA(year_timestamp_from, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
            print(f"EUA[{type(eua)}]: {eua}")
            energy     = calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
            print(f"energy[{type(energy)}]: {energy}")
            cb         = calc_cb(year_timestamp_from, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
            print(f"CB[{type(cb)}]: {cb}")
            GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)

            # CBが0未満の場合、罰金額を算出
            cb_cost = 0
            if float(cb) < 0:
                cb_cost = abs(float(cb)) * 2400 / (GHG_Actual * 41000)

            # DB登録用データセットを作成、更新する。
            leg_count       += 1

            avg_displacement = str(float(sum_displacement / noonreport_count))
            total_distance   = str(float(total_distance))
            total_lng        = str(float(total_lng))
            total_hfo        = str(float(total_hfo))
            total_lfo        = str(float(total_lfo))
            total_mdo        = str(float(total_mdo))
            total_mgo        = str(float(total_mgo))
            total_total_foc  = str(float(total_total_foc))
            GHG_Actual       = str(float(GHG_Actual))
            cb               = str(float(cb))

            dataset = {
                "imo": imo,
                "leg_no": leg_info["leg_no"]["S"],
                "departure_port": leg_info["departure_port"]["S"],
                "departure_time": Timestamp_from,
                "arrival_port": leg_info["arrival_port"]["S"],
                "arrival_time": Timestamp_to,
                "eu_rate": str(eu_rate),
                "displacement": avg_displacement,
                "leg_type": leg_type,
                "distance": total_distance,
                "total_lng": total_lng,
                "total_hfo": total_hfo,
                "total_lfo": total_lfo,
                "total_mdo": total_mdo,
                "total_mgo": total_mgo,
                "total_foc": total_total_foc,
                "GHG_Actual": GHG_Actual,
                "eta_local_date": eta_local_date,
                # "latest_course": latest_course,
                # "latest_wind_direction": latest_wind_direction,
                # "latest_beaufort": latest_beaufort,
                # "latest_log_speed": latest_log_speed,
                # "latest_me_rpm": latest_me_rpm,
                # "latest_me_load": latest_me_load,
                # "latest_foc": latest_foc, 
                "eua": eua,
                "cb": cb,
                "cb_cost": cb_cost
            }

    return dataset