
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
        elif name == "MGO":
            fuel_oil_info_list["MDO_info_list"] = fuel_oil_type_info
        elif name == "MGO":
            fuel_oil_info_list["MGO_info_list"] = fuel_oil_type_info
        elif name == "LNG(Otto Medium Speed)":        
            fuel_oil_info_list["LNG_OMS_info_list"] = fuel_oil_type_info

    return fuel_oil_info_list

def calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
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

def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = 0

    # ゼロ割防止のため、燃料消費量がゼロでない場合のみ計算する
    if total_lng + total_hfo + total_lfo + total_mdo + total_mgo > 0:
        GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
        cb = (GHG_Max - GHG_Actual) * energy
        print(f"cb{type(cb)}: {cb}")
        # cb_formatted = round(float(cb), 1)
        # print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")

    return cb

def main(imo, timestamp):

    operator_list = []
    year_total_distance = 0
    year_total_lng      = 0
    year_total_hfo      = 0
    year_total_lfo      = 0
    year_total_mdo      = 0
    year_total_mgo      = 0
    year_total_foc      = 0
    year_total_eua      = 0
    year_total_energy   = 0
    year_total_cb       = 0

    year_and_ope    = ""
    total_lng       = 0
    total_hfo       = 0
    total_lfo       = 0
    total_mdo       = 0
    total_mgo       = 0
    fine_flag       = "0"
    banking         = 0

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

    # leg-totalデータ取得
    res_leg = select.get_leg_total(imo, year_timestamp)
    # print(f"res_leg[{type(res_leg)}]: {res_leg}")

    for i in range(len(res_leg)):

        print(f"{(i+1)}leg目を処理")

        # leg-totalの各項目を取得
        departure_time = res_leg[i]["departure_time"]["S"]
        eu_rate        = res_leg[i]["eu_rate"]["S"]
        displacement   = res_leg[i]["eu_rate"]["S"]
        distance       = Util.format_to_one_decimal(round(float(res_leg[i]["distance"]["S"]), 1))
        total_lng      = Util.format_to_one_decimal(round(float(res_leg[i]["total_lng"]["S"]), 1))
        total_hfo      = Util.format_to_one_decimal(round(float(res_leg[i]["total_hfo"]["S"]), 1))
        total_lfo      = Util.format_to_one_decimal(round(float(res_leg[i]["total_lfo"]["S"]), 1))
        total_mdo      = Util.format_to_one_decimal(round(float(res_leg[i]["total_mdo"]["S"]), 1))
        total_mgo      = Util.format_to_one_decimal(round(float(res_leg[i]["total_mgo"]["S"]), 1))
        total_foc      = Util.format_to_one_decimal(round(float(res_leg[i]["total_foc"]["S"]), 1))
        eua            = Util.format_to_one_decimal(round(float(res_leg[i]["eua"]["S"]), 1))
        leg_timestamp  = res_leg[i]["timestamp"]["S"]
        print(f"departure_time[{type(departure_time)}]: {departure_time}")

        dt_timestamp_date_from = datetime(year = int(departure_time[0:4]), month = int(departure_time[5:7]), day = int(departure_time[8:10]) + 1, hour = 00, minute = 00, second = 00, microsecond = 000000)
        dt_timestamp_date_from = dt_timestamp_date_from.strftime('%Y-%m-%dT%H:%M:%SZ')
        dt_timestamp_date_to   = datetime(year = int(departure_time[0:4]), month = int(departure_time[5:7]), day = int(departure_time[8:10]) + 2, hour = 23, minute = 59, second = 59, microsecond = 999999)
        dt_timestamp_date_to   = dt_timestamp_date_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"date_from: {(dt_timestamp_date_from)}, date_to: {(dt_timestamp_date_to)}")

        nr = select.get_noonreport(imo, dt_timestamp_date_from, dt_timestamp_date_to)
        operator = nr[0]["operator"]["S"] if 'operator' in nr[0] and nr[0]["operator"]["S"] != "" else "NYK"

        # opeリストを作成する
        if operator in operator_list:

            # 【残】どのオペレーターのリストに足し合わせるかを振り分ける

            energy = calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
            year_total_energy += energy
            year_total_distance     += distance
            year_total_lng          += total_lng
            year_total_hfo          += total_hfo
            year_total_lfo          += total_lfo
            year_total_mdo          += total_mdo
            year_total_mgo          += total_mgo
            year_total_foc          += total_foc
            year_total_eua          += eua

        # opeリストに追加、新規データセットを作成する
        else:
            operator_list.append(operator)
            print(f"operator_list[{type(operator_list)}]: {operator_list}")

            energy = calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
            year_total_energy += energy
            year_total_distance     += distance
            year_total_lng          += total_lng
            year_total_hfo          += total_hfo
            year_total_lfo          += total_lfo
            year_total_mdo          += total_mdo
            year_total_mgo          += total_mgo
            year_total_foc          += total_foc
            year_total_eua          += eua

    # print(f"total_data[{type(total_data)}]: {total_data}")

    # CBの算出
    year_total_cb     = calc_cb(year_timestamp, year_total_energy, year_total_lng, year_total_hfo, year_total_lfo, year_total_mdo, year_total_mgo, fuel_oil_type_info_list)
    print(f"year_total_cb[{type(year_total_cb)}]: {year_total_cb}")

    year_and_ope = year_timestamp + operator

    # 去年分のyearレコードを取得する
    last_year_and_ope = str(int(year_timestamp) - 1) + operator
    last_year_record = select.get_year_total(imo, last_year_and_ope)
    print(f"last_year_record: {(last_year_record)}")

    if len(last_year_record) > 0:
        last_year_borrowing_cb = last_year_record[0]["borrowing"]["S"]
        last_year_banking_cb   = last_year_record[0]["banking"]["S"]

        if last_year_borrowing_cb > 0:
            if year_total_cb - last_year_borrowing_cb * 1.1 > 0:
                banking = year_total_cb - last_year_borrowing_cb * 1.1
            else:
                banking = 0
        
        elif last_year_banking_cb > 0:
            if last_year_banking_cb + year_total_cb > 0:
                banking = last_year_banking_cb + year_total_cb
            else:
                banking = 0
        elif year_total_cb > 0:
            banking = year_total_cb
        else:
            banking = 0

    # プーリンググループを取得する
    vessl_master = select.get_vesselmaster(imo)
    company_id =vessl_master[0]["Owner"]["S"]
    print(f"vessel_company_id: {(company_id)}")
    company_and_year = company_id + year_timestamp
    pooling_record = select.get_pooling_table(company_and_year)

    total_cb_plus = 0
    total_cb      = 0
    
    if len(pooling_record)> 0:
        for i in range(len(pooling_record)):
            imo_list = pooling_record[0]["imo_list"]["S"]
            # 各プーリンググループの中にimoがある場合
            if imo in imo_list:
                for j in range(len(imo_list)):
                    loop_imo = imo_list[j]
                    loop_year_list = select.get_get_year_total(loop_imo, year_and_ope)
                    loop_cb = Util.format_to_one_decimal(round(float(loop_year_list[0]["cb"]["S"]), 1)) if 'cb' in loop_year_list[0] and loop_year_list[0]["cb"]["S"] != "" else 0.0

                    if loop_cb > 0:
                        total_cb_plus += loop_cb
                    
                    total_cb += loop_cb

                # プーリンググループ内の合計CBがマイナスの場合、船単体も罰金対象とする（実際にはならない）
                if total_cb < 0:
                    fine_flag = "1"
                # プーリンググループ内の合計CBがプラスの場合、プラス分を按分する
                elif total_cb > 0:
                    banking = total_cb * year_total_cb / total_cb_plus
            
            elif year_total_cb + last_year_banking_cb - last_year_borrowing_cb * 1.1 > 0:
                fine_flag = "1"
    else:
        if year_total_cb < 0:
            fine_flag = "1"

    # 更新用データセットを設定
    year_total_distance = str(round(float(year_total_distance), 0))
    year_total_lng      = str(round(float(year_total_lng), 1))
    year_total_hfo      = str(round(float(year_total_hfo), 1))
    year_total_lfo      = str(round(float(year_total_lfo), 1))
    year_total_mdo      = str(round(float(year_total_mdo), 1))
    year_total_mgo      = str(round(float(year_total_mgo), 1))
    year_total_foc      = str(round(float(year_total_foc), 1))
    year_total_eua      = str(round(float(year_total_eua), 1))
    year_total_cb       = str(round(float(year_total_cb * 10**(-6)), 2))   # g→tonに変換。×10^(-6)
    banking             = str(round(float(banking), 1))

    dataset = {
        "imo": imo,
        "year_and_ope": year_and_ope,
        "distance":  year_total_distance,
        "total_lng": year_total_lng,
        "total_hfo": year_total_hfo,
        "total_lfo": year_total_lfo,
        "total_mdo": year_total_mdo,
        "total_mgo": year_total_mgo,
        "total_foc": year_total_foc,
        "eua": year_total_eua,
        "cb": year_total_cb,
        "banking": banking,
        # "borrowing": borrowing,
        "fine_flag": fine_flag,
        "timestamp": dt_now_str
    }
    print(f"dataset[{type(dataset)}]: {dataset}")
    upsert.upsert_year_total(dataset)
        
def lambda_handler(event,context):
    message = event
    print(f"message:{message}")
    try:
        imo = message["imo"]
        timestamp = message["timestamp"]
        print(f"imo: {imo}, timestamp: {timestamp}")
        main(imo, timestamp)

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }