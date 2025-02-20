
# from Tools.reuse import S3ConfigTools, DynamoDBConfigTools
# import requests
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

    GHG_Actual = 0
    if sum_foc > 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = 0

    # ゼロ割防止のため、燃料消費量がゼロでない場合のみ計算する
    if total_lng + total_hfo + total_lfo + total_mdo + total_mgo > 0:

        GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
        cb = (GHG_Max - GHG_Actual) * energy
        print(f"cb{type(cb)}: {cb}")

    return cb

def check_port_name(port_code):
    port_record = select.get_port_record(port_code)
    # print(f"port_record = {(port_record)}")
    port_name   = port_record[0]["port_name"]["S"]
    return port_name

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

def calc_sum_fuel(res_np, year, fuel_oil_info_list):
    print(f"res_nr: {(res_np)}")

    total_distance      = 0
    total_displacement  = 0
    count               = 0
    total_lng           = 0
    total_hfo           = 0
    total_lfo           = 0
    total_mdo           = 0
    total_mgo           = 0
    total_total_foc     = 0

    if len(res_np) > 0:

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

        total_eua = calc_EUA(year, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)

        data_list = {
            "total_distance": total_distance,
            "displacement"  : total_displacement/count,
            "total_lng"     : total_lng,
            "total_hfo"     : total_hfo,
            "total_lfo"     : total_lfo,
            "total_mdo"     : total_mdo,
            "total_mgo"     : total_mgo,
            "total_foc"     : total_total_foc,
            "total_eua"     : total_eua
        }

    else:
        data_list = {
            "total_distance": 0,
            "displacement"  : 0,
            "total_lng"     : 0,
            "total_hfo"     : 0,
            "total_lfo"     : 0,
            "total_mdo"     : 0,
            "total_mgo"     : 0,
            "total_foc"     : 0,
            "total_eua"     : 0
        }

    return data_list

def choise_period_noonreport(res_np, time_from, time_to):

    nr_list = []
    
    for i in range(len(res_np)):
        record     = res_np[i]
        local_date = record['local_date']['S']

        # local_timeを変換
        # datetimeオブジェクトに変換
        dt = datetime.strptime(local_date, "%Y/%m/%d %H:%M")
        # フォーマットを適用して再度文字列化
        formatted_date = dt.strftime("%Y/%m/%d %H:%M")
        local_date =  formatted_date

        if time_from <= local_date and local_date < time_to:
            nr_list.append(record)

    return nr_list

def main(imo, timestamp):

    nr_list = []
    keep_voyage_no            = 0
    keep_local_date           = ""
    voyage_departure_time     = ""
    voyage_departure_port     = ""
    voyage_arrival_time       = ""
    voyage_arrival_port       = ""
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
    voyage_displacement       = ""

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

    # Fuel-Oil-Typeリストを取得する
    fuel_oil_type_info_list = make_fuel_oil_type_info_list()
    # print(f"fuel_oil_type_info_list[{type(fuel_oil_type_info_list)}]: {fuel_oil_type_info_list}")

    # NoonReportデータ取得 from: dt_last_year_from_str, to: dt_now_str
    res_np = select.get_noonreport(imo, dt_timestamp_year_from_str, dt_timestamp_year_to_str)
    # print(f"res_np[{type(res_np)}]: {res_np}")

    # leg-totalデータ取得
    res_leg_list = select.get_leg_total(imo, year_timestamp)

    # EU Rateがゼロのレコードを除いたリストを作成する
    res_leg = []
    for leg in res_leg_list:
        leg_eu_rate = leg["eu_rate"]["S"]
        if leg_eu_rate != "0":
            res_leg.append(leg)
    print(f"len(res_leg): {len(res_leg)}")
    # print(f"res_np[{type(res_leg)}]: {res_leg}")

    # NoonReportでループ
    for i in range(len(res_np)):
        print(f"NoonReport  {(i+1)}レコード目")

        operator   = res_np[i]["operator"]["S"] if 'operator' in res_np[i] and res_np[i]["operator"]["S"] != "" else "NYK"
        voyage_no  = res_np[i]["voyage_no"]["S"]
        local_date = res_np[i]["local_date"]["S"]
        port_cord  = res_np[i]["port_code"]["S"] if 'port_code' in res_np[i] and res_np[i]["port_code"]["S"] != "" else ""
        print(f"port_cord:{(port_cord)}")

        # local_timeを変換
        # datetimeオブジェクトに変換
        dt = datetime.strptime(local_date, "%Y/%m/%d %H:%M")
        # フォーマットを適用して再度文字列化
        formatted_date = dt.strftime("%Y/%m/%d %H:%M")
        local_date =  formatted_date
        
        if port_cord != "":
            port_name  = check_port_name(port_cord)

        # 最初のNoonReportの時だけ
        if i == 0:
            # NoonReportの1レコード目のvoyage_noを保持
            keep_voyage_no = voyage_no
            voyage_departure_time = year_timestamp + "/01/01 00:00"
            # voyage_departure_time = res_leg[0]["departure_time"]["S"]
            voyage_departure_port = res_leg[0]["departure_port"]["S"]

        # Voyage Noが変わった時
        if voyage_no != keep_voyage_no:
            voyage_count += 1
            print(f"NoonReport{(i + 1)}レコード目でVoyageNo変化。Voyage {(voyage_count)}を処理")
            print(f"voyage_count{type(voyage_count)}: {voyage_count}")

            # 到着時刻を設定
            print(f"local_date:{(local_date)}")
            voyage_arrival_time = local_date
            voyage_arrival_port = port_name
            print(f"voyage_departure_time: {(voyage_departure_time)}, voyage_arrival_time: {(voyage_arrival_time)}")


            for j in range(len(res_leg)):

                # print(f"{(j + 1)}レグ目を確認")

                # leg-totalの各項目を取得
                departure_port = res_leg[j]["departure_port"]["S"]
                departure_time = res_leg[j]["departure_time"]["S"]
                arrival_time   = res_leg[j]["arrival_time"]["S"]
                displacement   = float(res_leg[j]["displacement"]["S"])
                distance       = float(res_leg[j]["distance"]["S"])
                total_lng      = float(res_leg[j]["total_lng"]["S"])
                total_hfo      = float(res_leg[j]["total_hfo"]["S"])
                total_lfo      = float(res_leg[j]["total_lfo"]["S"])
                total_mdo      = float(res_leg[j]["total_mdo"]["S"])
                total_mgo      = float(res_leg[j]["total_mgo"]["S"])
                total_foc      = float(res_leg[j]["total_foc"]["S"])
                eua            = float(res_leg[j]["eua"]["S"])
                leg_timestamp  = res_leg[j]["timestamp"]["S"]
                # print(f"total_foc[{type(total_foc)}]: {total_foc}")

                # 各legがこのvoyageに関わっているかを確認する。
                # print(f"leg_departure_time: {(departure_time)}, leg_arrival_time: {(arrival_time)}")

                if voyage_departure_time <= departure_time:

                    if arrival_time <= voyage_arrival_time:
                        print(f"leg{(j + 1)}はvoyageの中で完結している")
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

                        energy = calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
                        voyage_total_energy += energy

                    elif voyage_arrival_time < departure_time:
                        # print("このlegはvoyageの期間外")
                        None

                    # legの途中までこのvoyage
                    else:
                        print(f"leg{(j + 1)}は最初～途中までこのvoyage")
                        print(f"departure_time: {(departure_time)}")
                        # NoonReportを取得するための時刻が、timestamp(UTC)ではなくlocal_timeであるため
                        nr_list = choise_period_noonreport(res_np, departure_time, voyage_arrival_time)
                        # print(f"record_len: {(nr_list)}")

                        fuel_total_list = calc_sum_fuel(nr_list, year_timestamp, fuel_oil_type_info_list)
                        print(f"fuel_total_list: {(fuel_total_list)}")
                        total_energy = calc_energy(fuel_total_list["total_lng"], fuel_total_list["total_hfo"], fuel_total_list["total_lfo"], fuel_total_list["total_mdo"], fuel_total_list["total_mgo"], fuel_oil_type_info_list)
                        voyage_total_distance     += fuel_total_list["total_distance"]
                        voyage_total_lng          += fuel_total_list["total_lng"]
                        voyage_total_hfo          += fuel_total_list["total_hfo"]
                        voyage_total_lfo          += fuel_total_list["total_lfo"]
                        voyage_total_mdo          += fuel_total_list["total_mdo"]
                        voyage_total_mgo          += fuel_total_list["total_mgo"]
                        voyage_total_foc          += fuel_total_list["total_foc"]
                        voyage_total_displacement += fuel_total_list["displacement"]
                        if fuel_total_list["displacement"] != 0:
                            voyage_count_displacement += 1
                        voyage_total_eua          += fuel_total_list["total_eua"]
                        voyage_total_energy       += total_energy
                            
                # legの途中からこのvoyage
                elif voyage_departure_time <= arrival_time:
                    print(f"leg{(j + 1)}は途中～最後までこのvoyage")

                    # NoonReportを取得するための時刻が、timestamp(UTC)ではなくlocal_timeであるため
                    nr_list = choise_period_noonreport(res_np, voyage_departure_time, arrival_time)
                    # print(f"record_len: {(nr_list)}")

                    fuel_total_list = calc_sum_fuel(nr_list, year_timestamp, fuel_oil_type_info_list)
                    print(f"fuel_total_list: {(fuel_total_list)}")
                    total_energy = calc_energy(fuel_total_list["total_lng"], fuel_total_list["total_hfo"], fuel_total_list["total_lfo"], fuel_total_list["total_mdo"], fuel_total_list["total_mgo"], fuel_oil_type_info_list)
                    voyage_total_distance     += fuel_total_list["total_distance"]
                    voyage_total_lng          += fuel_total_list["total_lng"]
                    voyage_total_hfo          += fuel_total_list["total_hfo"]
                    voyage_total_lfo          += fuel_total_list["total_lfo"]
                    voyage_total_mdo          += fuel_total_list["total_mdo"]
                    voyage_total_mgo          += fuel_total_list["total_mgo"]
                    voyage_total_foc          += fuel_total_list["total_foc"]
                    voyage_total_displacement += fuel_total_list["displacement"]
                    if fuel_total_list["displacement"] != 0:
                        voyage_count_displacement += 1
                    voyage_total_eua          += fuel_total_list["total_eua"]
                    voyage_total_energy       += total_energy

                    print(f"voyage_total_distance: {(voyage_total_distance)}, voyage_total_hfo: {(voyage_total_hfo)}, total_foc: {(voyage_total_foc)}")

                else:
                    # print("このlegはvoyageの期間外")
                    None

            # voyageの合計値からCB, CBを算出する。
            print(f"voyage_total_distance: {(voyage_total_distance)}, voyage_total_hfo: {(voyage_total_hfo)}, total_foc: {(voyage_total_foc)}")
            print(f"voyage_total_eua{type(voyage_total_eua)}: {voyage_total_eua}")
            
            # 全てのlegレコードが範囲外の場合、voyage_count_displacement = 0 でゼロ割りが発生する
            if voyage_count_displacement > 0:
                print(f"このvoyageの中のlegは{(voyage_count_displacement)}レグ")

                voyage_total_cb = calc_cb(year_timestamp, voyage_total_energy, voyage_total_lng, voyage_total_hfo, voyage_total_lfo, voyage_total_mdo, voyage_total_mgo, fuel_oil_type_info_list)
                voyage_displacement = voyage_total_displacement / voyage_count_displacement

                # 更新用データセットを設定
                insert_voyage_no = year_timestamp + "{:0>3}".format(voyage_count)

                voyage_displacement   = str(float(voyage_displacement))
                voyage_total_distance = str(float(voyage_total_distance))
                voyage_total_lng      = str(float(voyage_total_lng))
                voyage_total_hfo      = str(float(voyage_total_hfo))
                voyage_total_lfo      = str(float(voyage_total_lfo))
                voyage_total_mdo      = str(float(voyage_total_mdo))
                voyage_total_mgo      = str(float(voyage_total_mgo))
                voyage_total_foc      = str(float(voyage_total_foc))
                voyage_total_energy   = str(float(voyage_total_energy))
                voyage_total_eua      = str(float(voyage_total_eua))
                voyage_total_cb       = str(float(voyage_total_cb))

                dataset = {
                    "imo"           : imo,
                    "voyage_no"     : insert_voyage_no,
                    "departure_port": voyage_departure_port,
                    "departure_time": voyage_departure_time,
                    "arrival_port"  : voyage_arrival_port,
                    "arrival_time"  : voyage_arrival_time,
                    "dispracement"  : voyage_displacement,
                    "operator"      : operator,
                    "distance"      : voyage_total_distance,
                    "total_lng"     : voyage_total_lng,
                    "total_hfo"     : voyage_total_hfo,
                    "total_lfo"     : voyage_total_lfo,
                    "total_mdo"     : voyage_total_mdo,
                    "total_mgo"     : voyage_total_mgo,
                    "total_foc"     : voyage_total_foc,
                    "total_energy"  : voyage_total_energy,
                    "eua"           : voyage_total_eua,
                    "cb"            : voyage_total_cb,
                    "timestamp"     : dt_now_str
                }
                # print(f"dataset[{type(dataset)}]: {dataset}")
                # voyage-totalテーブルに更新
                upsert.upsert_voyage_total(dataset)
            
            else:
                print(f"このvoyageには、EUに関係するレグは無い。")

            # このNoonReportから始まるvoyageのデータセット
            voyage_departure_port = departure_port
            voyage_departure_time = local_date

            voyage_total_distance     = 0
            voyage_total_lng          = 0
            voyage_total_hfo          = 0
            voyage_total_lfo          = 0
            voyage_total_mdo          = 0
            voyage_total_mgo          = 0
            voyage_total_foc          = 0
            voyage_total_displacement = 0
            voyage_count_displacement = 0
            voyage_total_eua          = 0
            voyage_total_energy       = 0

            keep_voyage_no = voyage_no
            print(f"keep_voyage_no :{(keep_voyage_no)}")

        # voyage_noが変わっていない時
        else:
            # 最後のNoonReportを処理する時
            if i == (len(res_np) - 1):
                voyage_count += 1
                print(f"最後のNoonReport。Voyage{(voyage_count)}を処理する。")

                # 到着時刻を設定
                voyage_arrival_time = local_date
                voyage_arrival_port = port_name
                print(f"voyage_departure_time: {(voyage_departure_time)}, voyage_arrival_time: {(voyage_arrival_time)}")


                for j in range(len(res_leg)):

                    # print(f"{(j + 1)}レグ目を確認")

                    # leg-totalの各項目を取得
                    departure_port = res_leg[j]["departure_port"]["S"]
                    departure_time = res_leg[j]["departure_time"]["S"]
                    arrival_time   = res_leg[j]["arrival_time"]["S"]
                    displacement   = float(res_leg[j]["displacement"]["S"])
                    distance       = float(res_leg[j]["distance"]["S"])
                    total_lng      = float(res_leg[j]["total_lng"]["S"])
                    total_hfo      = float(res_leg[j]["total_hfo"]["S"])
                    total_lfo      = float(res_leg[j]["total_lfo"]["S"])
                    total_mdo      = float(res_leg[j]["total_mdo"]["S"])
                    total_mgo      = float(res_leg[j]["total_mgo"]["S"])
                    total_foc      = float(res_leg[j]["total_foc"]["S"])
                    eua            = float(res_leg[j]["eua"]["S"])
                    leg_timestamp  = res_leg[j]["timestamp"]["S"]
                    # print(f"total_foc[{type(total_foc)}]: {total_foc}")

                    # 各legがこのvoyageに関わっているかを確認する。
                    print(f"leg_departure_time: {(departure_time)}, leg_arrival_time: {(arrival_time)}")

                    if voyage_departure_time <= departure_time:

                        if arrival_time <= voyage_arrival_time:
                            print(f"leg{(j + 1)}はvoyageの中で完結している")
                            print(f"voyage_total_distance: {(voyage_total_distance)}")  #足す前から600
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

                            print(f"distance: {(voyage_total_distance)}, total_hfo: {(voyage_total_hfo)}, total_foc: {(voyage_total_foc)}")
                            energy = calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
                            voyage_total_energy += energy

                        elif voyage_arrival_time < departure_time:
                            # print("このlegはvoyageの期間外")
                            None

                        # legの途中までこのvoyage
                        else:
                            print(f"leg{(j + 1)}は最初～途中までこのvoyage")
                            print(f"departure_time: {(departure_time)}")
                            # NoonReportを取得するための時刻が、timestamp(UTC)ではなくlocal_timeであるため
                            nr_list = choise_period_noonreport(res_np, departure_time, voyage_arrival_time)
                            # print(f"record_len: {(nr_list)}")

                            fuel_total_list = calc_sum_fuel(nr_list, year_timestamp, fuel_oil_type_info_list)
                            print(f"fuel_total_list: {(fuel_total_list)}")
                            total_energy = calc_energy(fuel_total_list["total_lng"], fuel_total_list["total_hfo"], fuel_total_list["total_lfo"], fuel_total_list["total_mdo"], fuel_total_list["total_mgo"], fuel_oil_type_info_list)
                            voyage_total_distance     += fuel_total_list["total_distance"]
                            voyage_total_lng          += fuel_total_list["total_lng"]
                            voyage_total_hfo          += fuel_total_list["total_hfo"]
                            voyage_total_lfo          += fuel_total_list["total_lfo"]
                            voyage_total_mdo          += fuel_total_list["total_mdo"]
                            voyage_total_mgo          += fuel_total_list["total_mgo"]
                            voyage_total_foc          += fuel_total_list["total_foc"]
                            voyage_total_displacement += fuel_total_list["displacement"]
                            if fuel_total_list["displacement"] != 0:
                                voyage_count_displacement += 1
                            voyage_total_eua          += fuel_total_list["total_eua"]
                            voyage_total_energy       += total_energy

                    # legの途中からこのvoyage
                    elif voyage_departure_time <= arrival_time:
                        print(f"leg{(j + 1)}は途中～最後までこのvoyage")

                        # NoonReportを取得するための時刻が、timestamp(UTC)ではなくlocal_timeであるため
                        nr_list = choise_period_noonreport(res_np, voyage_departure_time, arrival_time)
                        # print(f"record_len: {(nr_list)}")

                        fuel_total_list = calc_sum_fuel(nr_list, year_timestamp, fuel_oil_type_info_list)
                        print(f"fuel_total_list: {(fuel_total_list)}")
                        total_energy = calc_energy(fuel_total_list["total_lng"], fuel_total_list["total_hfo"], fuel_total_list["total_lfo"], fuel_total_list["total_mdo"], fuel_total_list["total_mgo"], fuel_oil_type_info_list)
                        voyage_total_distance     += fuel_total_list["total_distance"]
                        voyage_total_lng          += fuel_total_list["total_lng"]
                        voyage_total_hfo          += fuel_total_list["total_hfo"]
                        voyage_total_lfo          += fuel_total_list["total_lfo"]
                        voyage_total_mdo          += fuel_total_list["total_mdo"]
                        voyage_total_mgo          += fuel_total_list["total_mgo"]
                        voyage_total_foc          += fuel_total_list["total_foc"]
                        voyage_total_displacement += fuel_total_list["displacement"]
                        if fuel_total_list["displacement"] != 0:
                            voyage_count_displacement += 1
                        voyage_total_eua          += fuel_total_list["total_eua"]
                        voyage_total_energy       += total_energy

                    else:
                        # print("このlegはvoyageの期間外")
                        None

                # voyageの合計値からCB, CBを算出する。
                print(f"voyage_total_eua{type(voyage_total_eua)}: {voyage_total_eua}")
                print(f"voyage_total_distance: {(voyage_total_distance)}, voyage_total_hfo: {(voyage_total_hfo)}, total_foc: {(voyage_total_foc)}")

                # 全てのlegレコードが範囲外の場合、voyage_count_displacement = 0 でゼロ割りが発生する
                if voyage_count_displacement > 0:
                    voyage_total_cb = calc_cb(year_timestamp, voyage_total_energy, voyage_total_lng, voyage_total_hfo, voyage_total_lfo, voyage_total_mdo, voyage_total_mgo, fuel_oil_type_info_list)
                    voyage_displacement = voyage_total_displacement / voyage_count_displacement

                    # 更新用データセットを設定
                    insert_voyage_no = year_timestamp + "{:0>3}".format(voyage_count)

                    voyage_displacement   = str(float(voyage_displacement))
                    voyage_total_distance = str(float(voyage_total_distance))
                    voyage_total_lng      = str(float(voyage_total_lng))
                    voyage_total_hfo      = str(float(voyage_total_hfo))
                    voyage_total_lfo      = str(float(voyage_total_lfo))
                    voyage_total_mdo      = str(float(voyage_total_mdo))
                    voyage_total_mgo      = str(float(voyage_total_mgo))
                    voyage_total_foc      = str(float(voyage_total_foc))
                    voyage_total_energy   = str(float(voyage_total_energy))
                    voyage_total_eua      = str(float(voyage_total_eua))
                    voyage_total_cb       = str(float(voyage_total_cb))

                    dataset = {
                        "imo"           : imo,
                        "voyage_no"     : insert_voyage_no,
                        "departure_port": voyage_departure_port,
                        "departure_time": voyage_departure_time,
                        "arrival_port"  : voyage_arrival_port,
                        "arrival_time"  : voyage_arrival_time,
                        "dispracement"  : voyage_displacement,
                        "operator"      : operator,
                        "distance"      : voyage_total_distance,
                        "total_lng"     : voyage_total_lng,
                        "total_hfo"     : voyage_total_hfo,
                        "total_lfo"     : voyage_total_lfo,
                        "total_mdo"     : voyage_total_mdo,
                        "total_mgo"     : voyage_total_mgo,
                        "total_foc"     : voyage_total_foc,
                        "total_energy"  : voyage_total_energy,
                        "eua"           : voyage_total_eua,
                        "cb"            : voyage_total_cb,
                        "timestamp"     : dt_now_str
                    }
                    # print(f"dataset[{type(dataset)}]: {dataset}")
                    upsert.upsert_voyage_total(dataset)
                else:
                    print(f"このvoyageには、EUに関係するレグは無い。")
        
def lambda_handler(event,context):
    message = event
    print(f"message:{message}")

    try:
        imo = message["imo"]
        timestamp = message["timestamp"]
        print(f"imo: {imo}, timestamp: {timestamp}")
        main(imo, timestamp)

        lambda_function_name1 = "spm-euets-fueleu-year-total"

        payload = {
            "imo" : imo,
            "timestamp" : timestamp
        }

        try:
            client = boto3.client('lambda')
            client.invoke(
                FunctionName = lambda_function_name1,
                InvocationType = 'Event',
                LogType = 'Tail',
                Payload = json.dumps(payload)
            )
            print(f"message{type(payload)}: {payload} is sent.")
        except Exception as e:
            print(f"Couldn't invoke function : {lambda_function_name1}")
            print(json.dumps(str(e)))

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }