
import ast
from datetime import datetime
import json
import math
import re

import auth
from dynamodb import select

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
    
    if sum_foc != 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    cb_formatted = round(float(cb), 1)
    print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")
    return cb_formatted

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    # イベントからクエリ取得-------------------------------------------------------
    pathParameters = event['pathParameters']
    pathParameters = pathParameters['proxy']
    pathParameters = pathParameters.split("/")
    queryStringParameters = event['queryStringParameters']
    
    # クエリからインプット情報取得-------------------------------------------------------
    user_id = queryStringParameters['user']
    input_year = queryStringParameters['year']
    data = queryStringParameters['data']
    data_pooling_list = data.split(',')

    # Fuel-Oil-Typeリストを取得する
    fuel_oil_type_info_list = make_fuel_oil_type_info_list()

    # 合計値用変数を設定する。
    total_lng = 0
    total_hfo = 0
    total_lfo = 0
    total_mdo = 0
    total_mgo = 0
    total_energy = 0

    # pooling_groupのimo_listでループ
    return_datas = []
    for pooling_imo_str in data_pooling_list:

        imoList = pooling_imo_str.strip("()").split('-')

        total_lng    = 0
        total_hfo    = 0
        total_lfo    = 0
        total_mdo    = 0
        total_mgo    = 0
        total_energy = 0
        last_year_total_cb = 0

        for loop_imo in imoList:

            print(f"loop_imo:{loop_imo}")

            # imoが空の場合を考慮
            if loop_imo =="":
                continue

            # imo, yearをキーに、
            year_total_list = select.get_year_total_by_year(loop_imo, input_year)
            last_year_total_list = select.get_year_total_by_year(loop_imo, str(int(input_year) - 1))

            for year_total in year_total_list:

                # 各項目を取得する
                lng = float(year_total["total_lng"]["S"])
                hfo = float(year_total["total_hfo"]["S"])
                lfo = float(year_total["total_lfo"]["S"])
                mdo = float(year_total["total_mdo"]["S"])
                mgo = float(year_total["total_mgo"]["S"])
                cb  = float(year_total["cb"]["S"])

                # エネルギー消費量（EU Rate考慮済）を算出する
                energy     = calc_energy(lng, hfo, lfo, mdo, mgo, fuel_oil_type_info_list)

                # 各種合計値に足し合わせる
                total_lng    += lng
                total_hfo    += hfo
                total_lfo    += lfo
                total_mdo    += mdo
                total_mgo    += mgo
                total_energy += energy

            # last_yearを確認
            for last_year_total in last_year_total_list:

                # 各項目を取得する
                last_year_banking   = float(last_year_total["banking"]["S"]) if "banking" in last_year_total and last_year_total["banking"]["S"] != "" else 0
                last_year_borrowing = float(last_year_total["borrowing"]["S"]) if "borrowing" in last_year_total and last_year_total["borrowing"]["S"] != "" else 0
            
                last_year_total_cb += (last_year_banking - last_year_borrowing * 1.1)

        # プーリンググループ合計のCBを算出する
        year_total_cb = calc_cb(input_year, total_energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
        total_cb      = year_total_cb + last_year_total_cb
        str_total_cb  = str(round(total_cb / 1000000, 1))

        # CBがマイナスの場合、コストを算出する
        if total_cb < 0:
            total_GHG_Actual  = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
            if total_GHG_Actual != 0:
                total_cb_cost     = abs(total_cb) / total_GHG_Actual * 2400 / 41000
                str_total_cb_cost = str(round(total_cb_cost))
            else:
                str_total_cb_cost    = "0"
        else:
            str_total_cb_cost    = "0"

        # データにセット
        group_data = {
            "total_cb"     :str_total_cb,
            "total_cb_cost": str_total_cb_cost 
        }

        return_datas.append(group_data)

    datas = {
        "group_total_list":return_datas
    }
    datas = json.dumps(datas)
    print(f"datas{type(datas)}: {datas}")

    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
