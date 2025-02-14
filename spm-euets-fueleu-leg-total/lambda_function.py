
import os
import json
from datetime import datetime
from dynamodb import select, upsert
import boto3

from Util import Util

def make_fuel_oil_type_info_list():

    # Ecoで使用する燃料の情報リスト
    fuel_oil_info_list = {
        "HFO_info_list": [],
        "LFO_info_list": [],
        "MDO_info_list": [],
        "MGO_info_list": [],
        "LNG_OMS_info_list": []
    }

    # 燃料情報リストを取得し、データセットを作成する
    fuel_oil_name_list = ["HFO", "LFO", "MDO", "MGO", "LNG(Otto Medium Speed)"]
    fuel_oil_type_info_list = []

    for fuel_oil_name in fuel_oil_name_list:
        fuel_oil_type_info_list.append(select.get_fuel_oil_type(fuel_oil_name)[0])
    for fuel_oil_type_info in fuel_oil_type_info_list:
        name = fuel_oil_type_info["fuel_oil_type"]["S"]

        # それぞれの燃料リストに格納する
        if name == "HFO":
            fuel_oil_info_list["HFO_info_list"] = fuel_oil_type_info
        elif name == "LFO":
            fuel_oil_info_list["LFO_info_list"] = fuel_oil_type_info
        elif name == "MDO":
            fuel_oil_info_list["MDO_info_list"] = fuel_oil_type_info
        elif name == "MGO":
            fuel_oil_info_list["MGO_info_list"] = fuel_oil_type_info
        elif name == "LNG(Otto Medium Speed)":        
            fuel_oil_info_list["LNG_OMS_info_list"] = fuel_oil_type_info

    return fuel_oil_info_list

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
        lng_co2_factor =  float(fuel_oil_info_list["LNG_OMS_info_list"]["emission_factor"]["S"])
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
    return eua

def calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    energy_lng = 0
    energy_hfo = 0
    energy_lfo = 0
    energy_mdo = 0
    energy_mgo = 0

    if total_lng > 0:
        lng_lcv =  float(fuel_oil_info_list["LNG_OMS_info_list"]["lcv"]["S"])
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
        lng_ghg_intensity =  float(fuel_oil_info_list["LNG_OMS_info_list"]["ghg_intensity"]["S"])
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

    GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = 0

    # ゼロ割防止のため、燃料消費量がゼロでない場合のみ計算する
    total_foc = total_lng + total_hfo + total_lfo + total_mdo + total_mgo
    if total_foc > 0:
        GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
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

def main(imo, timestamp):

    keep_state            = ""
    keep_port_code        = ""
    leg_count             = 0
    leg_no                = ""
    departure_time        = ""
    eu_rate               = 0
    noonreport_count      = 0
    sum_displacement      = 0
    avg_displacement      = 0
    leg_type              = ""
    total_distance        = 0
    total_lng             = 0
    total_hfo             = 0
    total_lfo             = 0
    total_mdo             = 0
    total_mgo             = 0
    total_total_foc       = 0
    eta_local_date        = ""
    latest_course         = 0
    latest_wind_direction = 0
    latest_beaufort       = 0
    latest_log_speed      = 0
    latest_me_rpm         = 0
    latest_me_load        = 0
    latest_foc            = 0
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
    year_timestamp = timestamp[0:4]
    print(f"year_timestamp{type(year_timestamp)}: {year_timestamp}")

    #NoonReport取得期間のスタートを設定
    dt_timestamp_year_from = datetime(year = int(year_timestamp), month = 1, day = 1)
    dt_timestamp_year_from_str = dt_timestamp_year_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"dt_timestamp_year_from_str{type(dt_timestamp_year_from_str)}: {dt_timestamp_year_from_str}")

    if year_timestamp == year_now:
        #処理当日分まで
        dt_timestamp_year_to_str = dt_now_str
    else:
        dt_timestamp_year_to = datetime(year = int(year_timestamp), month = 12, day = 31, hour = 23, minute = 59, second = 59, microsecond = 999999)
        dt_timestamp_year_to_str = dt_timestamp_year_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        
    print(f"dt_timestamp_year_to_str{type(dt_timestamp_year_to_str)}: {dt_timestamp_year_to_str}")

    # NoonReportデータ取得 from: dt_last_year_from_str, to: dt_now_str
    res_np = select.get_noonreport(imo, dt_timestamp_year_from_str, dt_timestamp_year_to_str)
    # print(f"res_np[{type(res_np)}]: {res_np}")
    print(f"Got NoonReport.")

    # Fuel-Oil-Typeリストを取得する
    fuel_oil_type_info_list = make_fuel_oil_type_info_list()

    # print(f"fuel_oil_type_info_list[{type(fuel_oil_type_info_list)}]: {fuel_oil_type_info_list}")

    # NoonReportのレコード数分回す
    print(f"len(res_np):{(len(res_np))}")
    for i in range(len(res_np)):

        print(f"NoonReportカウント: {(i)}")

        # NoonReportの各項目を取得
        local_date = res_np[i]["local_date"]["S"]
        state = res_np[i]["state"]["S"] if 'state' in res_np[i] and res_np[i]["state"]["S"] != "" else keep_state
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
        port_code      = res_np[i]["port_code"]["S"]        if 'port_code'       in res_np[i] and res_np[i]["port_code"]["S"]      != "" else ""
        eta_port_code  = res_np[i]["eta_port_code"]["S"]    if 'eta_port_code'   in res_np[i] and res_np[i]["eta_port_code"]["S"]  != "" else ""
        course         = Util.format_to_one_decimal(round(float(res_np[i]["course"]["S"]), 1))  if 'course' in res_np[i] and res_np[i]["course"]["S"]  != "" else ""
        wind_direction = Util.format_to_one_decimal(round(float(res_np[i]["wind_direction"]["S"]), 1))  if 'wind_direction' in res_np[i] and res_np[i]["wind_direction"]["S"]  != "" else ""
        beaufort       = Util.format_to_one_decimal(round(float(res_np[i]["beaufort"]["S"]), 1))  if 'beaufort' in res_np[i] and res_np[i]["beaufort"]["S"]  != "" else ""
        log_speed      = Util.format_to_one_decimal(round(float(res_np[i]["log_speed"]["S"]), 1))  if 'log_speed' in res_np[i] and res_np[i]["log_speed"]["S"]  != "" else ""
        me_rpm         = Util.format_to_one_decimal(round(float(res_np[i]["me_rpm"]["S"]), 1))  if 'me_rpm' in res_np[i] and res_np[i]["me_rpm"]["S"]  != "" else ""
        me_load        = Util.format_to_one_decimal(round(float(res_np[i]["me_load"]["S"]), 1))  if 'me_load' in res_np[i] and res_np[i]["me_load"]["S"]  != "" else ""

        # local_timeを変換
        print(f"local_date(befort):{(local_date)}")
        # datetimeオブジェクトに変換
        dt = datetime.strptime(local_date, "%Y/%m/%d %H:%M")
        # フォーマットを適用して再度文字列化
        formatted_date = dt.strftime("%Y/%m/%d %H:%M")
        print(f"local_date(after):{(formatted_date)}")

        # 最初のNoonReport
        if i == 0:
            print("初回処理")
            keep_state = state
            last_year = str(int(year_timestamp) - 1)
            # print(f"last_year_leg_no_start[{type(last_year_leg_no_start)}]: {last_year_leg_no_start}")
            last_year_leg_list = select.get_leg_total(imo, last_year)
            # print(f"last_year_leg_list[{type(last_year_leg_list)}]: {last_year_leg_list}")

            # 昨年のleg-totalレコードが存在する場合
            if last_year_leg_list:
                last_year_leg_list_sorted = sorted(last_year_leg_list, key=lambda x:x["leg_no"]["S"], reverse=True)
                # print(f"last_year_leg_list_sorted[{type(last_year_leg_list_sorted)}]: {last_year_leg_list_sorted}")

                eu_rate = last_year_leg_list_sorted[0]["eu_rate"]["S"] if 'eu_rate' in last_year_leg_list_sorted[0] and last_year_leg_list_sorted[0]["eu_rate"]["S"]  != "" else 0
            # 昨年のleg-totalレコードが存在しない場合
            else:
                eu_rate = check_eu_rate(port_code, eta_port_code)
            print(f"eu_rate[{type(eu_rate)}]: {eu_rate}")
            
            # 航海中の場合、出発港を確認する
            print(f"state[{type(state)}]: {state}")
            if state == "AT SEA" and last_year_leg_list:
                departure_port = last_year_leg_list_sorted[0]["arrival_port"]["S"]
                print(f"last_year_leg_list_sorted[0]: {(last_year_leg_list_sorted[0])}")
            else:
                print(f"port_code:{(port_code)}")
                departure_port = check_port_name(port_code)
            print(f"departure_port[{type(departure_port)}]: {departure_port}")

            # データをセット(各leg初回だけ)
            departure_time   = year_timestamp + "/01/01 00:00"
            print(f"eta_port_code[{type(eta_port_code)}]: {eta_port_code}")
            arrival_port     = check_port_name(eta_port_code)
            leg_type         = "Port" if state == "IN PORT" else "Sailing"
            sum_displacement = 0
            noonreport_count = 0
            print(f"leg_type[{type(leg_type)}]: {leg_type}")

        if state != keep_state:
            print("state変化")

            if (keep_state == 'AT SEA' and state == 'IN HARBOR') or (keep_state == 'IN HARBOR' and state == 'AT SEA'):
                print("航海Leg継続")
                # データをセット(毎回)
                arrival_time          = eta_local_date
                latest_course         = str(course)
                latest_wind_direction = str(wind_direction)
                latest_beaufort       = str(beaufort)
                latest_log_speed      = str(log_speed)
                latest_me_rpm         = str(me_rpm)
                latest_me_load        = str(me_load)
                latest_foc            = str(total_foc)

                #データを加算(毎回)
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

            else:
                print("Leg終了、登録処理開始")
                print(f"year_timestamp:{(year_timestamp)}, eu_rate:{(eu_rate)}, total_lng:{(total_lng)}, total_hfo:{(total_hfo)}, total_lfo:{(total_lfo)}, total_mdo:{(total_mdo)}, total_mgo:{(total_mgo)}")
                
                # 各種燃料消費量にEU Rateを考慮する
                total_lng = total_lng * float(eu_rate) / 100
                total_hfo = total_hfo * float(eu_rate) / 100
                total_lfo = total_lfo * float(eu_rate) / 100
                total_mdo = total_mdo * float(eu_rate) / 100
                total_mgo = total_mgo * float(eu_rate) / 100
                total_total_foc = total_total_foc * float(eu_rate) / 100
                
                #EUA, CBを算出
                leg_eua        = calc_EUA(year_timestamp, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
                print(f"leg_eua[{type(leg_eua)}]: {leg_eua}")
                energy     = calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
                print(f"energy[{type(energy)}]: {energy}")
                leg_cb         = calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
                print(f"CB[{type(cb)}]: {cb}")

                # DB登録用データセットを作成、更新する。
                leg_count       += 1
                leg_no           = year_timestamp + "{:0>3}".format(leg_count)

                # 航海に出発した場合この時点で海上なので、port_codeを参照できない
                if state != "IN PORT":
                    port_code = keep_port_code
                print(f"port_code: {(port_code)}")
                arrival_port     = check_port_name(port_code)
                print(f"arrival_port_name: {(arrival_port)}")

                avg_displacement = str(float(sum_displacement / noonreport_count))
                total_distance   = str(float(total_distance))
                total_lng        = str(float(total_lng))
                total_hfo        = str(float(total_hfo))
                total_lfo        = str(float(total_lfo))
                total_mdo        = str(float(total_mdo))
                total_mgo        = str(float(total_mgo))
                total_total_foc  = str(float(total_total_foc))
                leg_eua          = str(float(leg_eua))
                leg_cb           = str(float(leg_cb))
                eu_rate          = str(eu_rate)

                dataset = {
                    "imo": imo,
                    "leg_no": leg_no,
                    "departure_port": departure_port,
                    "departure_time": departure_time,
                    "arrival_port": arrival_port,
                    "arrival_time": formatted_date,
                    "eu_rate": eu_rate,
                    "displacement": avg_displacement,
                    "leg_type": leg_type,
                    "distance": total_distance,
                    "total_lng": total_lng,
                    "total_hfo": total_hfo,
                    "total_lfo": total_lfo,
                    "total_mdo": total_mdo,
                    "total_mgo": total_mgo,
                    "total_foc": total_total_foc,
                    "eta_local_date": eta_local_date,
                    "latest_course": latest_course,
                    "latest_wind_direction": latest_wind_direction,
                    "latest_beaufort": latest_beaufort,
                    "latest_log_speed": latest_log_speed,
                    "latest_me_rpm": latest_me_rpm,
                    "latest_me_load": latest_me_load,
                    "latest_foc": latest_foc, 
                    "eua": leg_eua,
                    "cb": leg_cb,
                    "timestamp": dt_now_str
                }
                # print(f"dataset[{type(dataset)}]: {dataset}")

                upsert.upsert_leg_total(dataset)
                print("Upserted dataset.")

                # データをセット(各leg初回だけ)
                print("新規legのデータを作成")
                departure_port = check_port_name(port_code)
                departure_time   = formatted_date
                leg_type         = "Port" if state == "IN PORT" else "Sailing"

                # 停泊中の場合、出発地点と到着地は同じ
                if leg_type == "Port":
                    eta_port_code = port_code

                print(f"port_code:{(port_code)}, eta_port_code:{(eta_port_code)}")
                arrival_port     = check_port_name(eta_port_code) if leg_type == "Sailing" else departure_port
                eu_rate          = check_eu_rate(port_code, eta_port_code)
                sum_displacement = 0
                noonreport_count = 0

                # データをセット(毎回)
                arrival_time          = formatted_date
                sum_displacement     += displacement
                noonreport_count     += 1
                latest_course         = str(course)
                latest_wind_direction = str(wind_direction)
                latest_beaufort       = str(beaufort)
                latest_log_speed      = str(log_speed)
                latest_me_rpm         = str(me_rpm)
                latest_me_load        = str(me_load)
                latest_foc            = str(total_foc)

                lng = me_bog  + dg_bog  + gcu_bog
                hfo = me_hfo  + dg_hfo  + boiler_hfo
                lfo = me_lsfo + dg_lsfo + boiler_lsfo
                mdo = me_do   + dg_do   + boiler_do
                mgo = me_lsgo + dg_lsgo + boiler_lsgo + igg_go + igg_lsgo

                total_distance  = distance
                total_lng       = lng
                total_hfo       = hfo
                total_lfo       = lfo
                total_mdo       = mdo
                total_mgo       = mgo
                total_total_foc = total_foc

        else:
            # データをセット(毎回)
            arrival_time          = eta_local_date
            sum_displacement     += displacement
            noonreport_count     += 1
            latest_course         = str(course)
            latest_wind_direction = str(wind_direction)
            latest_beaufort       = str(beaufort)
            latest_log_speed      = str(log_speed)
            latest_me_rpm         = str(me_rpm)
            latest_me_load        = str(me_load)
            latest_foc            = str(total_foc)

            #データを加算(毎回)
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
        
        # keep_stateの更新
        keep_state = state
        # print(f"keep_state = {(keep_state)}")
        keep_port_code = port_code if port_code != "" else keep_port_code

        # 最終レコードの場合、計算して更新
        if i == len(res_np)-1 :

            print("NoonReport最終レコード。Leg終了、登録処理開始")
            print(f"year_timestamp:{(year_timestamp)}, eu_rate:{(eu_rate)}, total_lng:{(total_lng)}, total_hfo:{(total_hfo)}, total_lfo:{(total_lfo)}, total_mdo:{(total_mdo)}, total_mgo:{(total_mgo)}")

            # 各種燃料消費量にEU Rateを考慮する
            total_lng = total_lng * float(eu_rate) / 100
            total_hfo = total_hfo * float(eu_rate) / 100
            total_lfo = total_lfo * float(eu_rate) / 100
            total_mdo = total_mdo * float(eu_rate) / 100
            total_mgo = total_mgo * float(eu_rate) / 100
            total_total_foc = total_total_foc * float(eu_rate) / 100

            #EUA, CBを算出
            leg_eua        = calc_EUA(year_timestamp, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
            print(f"EUA[{type(eua)}]: {eua}")
            energy     = calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
            print(f"energy[{type(energy)}]: {energy}")
            leg_cb         = calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
            print(f"Leg_CB[{type(leg_cb)}]: {leg_cb}")

            # DB登録用データセットを作成、更新する。
            leg_count       += 1
            leg_no           = year_timestamp + "{:0>3}".format(leg_count)

            # 航海に出発した場合この時点で海上なので、port_codeを参照できない
            if state != "IN PORT":
                port_code = keep_port_code
            arrival_port     = check_port_name(port_code)
            print(f"arrival_port_name: {(arrival_port)}")

            arrival_time     = formatted_date
            avg_displacement = str(float(sum_displacement / noonreport_count))
            total_distance   = str(float(total_distance))
            total_lng        = str(float(total_lng))
            total_hfo        = str(float(total_hfo))
            total_lfo        = str(float(total_lfo))
            total_mdo        = str(float(total_mdo))
            total_mgo        = str(float(total_mgo))
            total_total_foc  = str(float(total_total_foc))
            leg_eua          = str(float(leg_eua))
            leg_cb           = str(float(leg_cb))
            eu_rate          = str(eu_rate)

            dataset = {
                "imo": imo,
                "leg_no": leg_no,
                "departure_port": departure_port,
                "departure_time": departure_time,
                "arrival_port": arrival_port,
                "arrival_time": arrival_time,
                "eu_rate": eu_rate,
                "displacement": avg_displacement,
                "leg_type": leg_type,
                "distance": total_distance,
                "total_lng": total_lng,
                "total_hfo": total_hfo,
                "total_lfo": total_lfo,
                "total_mdo": total_mdo,
                "total_mgo": total_mgo,
                "total_foc": total_total_foc,
                "eta_local_date": eta_local_date,
                "latest_course": latest_course,
                "latest_wind_direction": latest_wind_direction,
                "latest_beaufort": latest_beaufort,
                "latest_log_speed": latest_log_speed,
                "latest_me_rpm": latest_me_rpm,
                "latest_me_load": latest_me_load,
                "latest_foc": latest_foc, 
                "eua": leg_eua,
                "cb": leg_cb,
                "timestamp": dt_now_str
            }
            # print(f"dataset[{type(dataset)}]: {dataset}")

            upsert.upsert_leg_total(dataset)
        
def lambda_handler(event,context):
    message = event
    print(f"message:{message}")
    try:
        imo = message["imo"]
        timestamp = message["timestamp"]
        print(f"imo: {imo}, timestamp: {timestamp}")
        main(imo, timestamp)

        # lambda_function_name1 = "spm-euets-fueleu-year-total"
        lambda_function_name2 = "spm-euets-fueleu-voyage-total"
        payload = {
            "imo" : imo,
            "timestamp" : timestamp
        }

        try:
            client = boto3.client('lambda')
            client.invoke(
                FunctionName = lambda_function_name2,
                InvocationType = 'Event',
                LogType = 'Tail',
                Payload = json.dumps(payload)
            )
            print(f"message{type(payload)}: {payload} is sent.")
        except Exception as e:
            print(f"Couldn't invoke function : {lambda_function_name2}")
            print(json.dumps(str(e)))

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }