
import ast
from datetime import datetime
import json
import math
import re

import auth
from dynamodb import insert, select, delete
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
    
    if sum_foc != 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_type_info_list)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    return cb

def calc_banking(res_pooling_group, year, company_id):

    # Fuel-Oil-Typeリストを取得する
    fuel_oil_type_info_list = make_fuel_oil_type_info_list()

    for pooling_group in res_pooling_group:

        # 単体でのCBがプラスのものの合計CB
        total_cb_plus = 0
        # 全レコードのリスト
        all_sk_list = []

        imo_list = ast.literal_eval(pooling_group["imo_list"]["S"])
        print(f"imo_list{type(imo_list)}:{(imo_list)}")
        group_name = pooling_group["group_name"]["S"]

        pooling_group_info = company_id + ", " + group_name
        print(f"pooling_group_info:{(pooling_group_info)}")

        # グループの合計燃料消費量
        group_total_lng = 0
        group_total_hfo = 0
        group_total_lfo = 0
        group_total_mdo = 0
        group_total_mgo = 0

        for imo in imo_list:
            vessel_year_list = select.get_year_total_by_year(imo, year)

            for year_record in vessel_year_list:
                loop_year_and_ope = year_record["year_and_ope"]["S"]
                loop_lng          = float(year_record["total_lng"]["S"])   if 'total_lng' in year_record and year_record["total_lng"]["S"] != "" else 0.0
                loop_hfo          = float(year_record["total_hfo"]["S"])   if 'total_hfo' in year_record and year_record["total_hfo"]["S"] != "" else 0.0
                loop_lfo          = float(year_record["total_lfo"]["S"])   if 'total_lfo' in year_record and year_record["total_lfo"]["S"] != "" else 0.0
                loop_mdo          = float(year_record["total_mdo"]["S"])   if 'total_mdo' in year_record and year_record["total_mdo"]["S"] != "" else 0.0
                loop_mgo          = float(year_record["total_mgo"]["S"])   if 'total_mgo' in year_record and year_record["total_mgo"]["S"] != "" else 0.0
                loop_cb           = float(year_record["cb"]["S"])   if 'cb' in year_record and year_record["cb"]["S"] != "" else 0.0
                loop_borrowing    = float(year_record["borrowing"]["S"]) if 'borrowing' in year_record and year_record["borrowing"]["S"] != "" else 0.0

                # グループの合計燃料消費量に加算する
                group_total_lng += loop_lng
                group_total_hfo += loop_hfo
                group_total_lfo += loop_lfo
                group_total_mdo += loop_mdo
                group_total_mgo += loop_mgo

                # 去年分のyearレコードを取得する
                last_year_and_ope   = str(int(loop_year_and_ope[0:4]) - 1) + loop_year_and_ope[4:7]
                print(f"last_year_and_ope:{(last_year_and_ope)}")
                last_year_record    = select.get_year_total(imo, last_year_and_ope)
                last_year_banking = float(last_year_record[0]["banking"]) if 'banking' in last_year_record and last_year_record["banking"]["S"] != "" else 0.0
                last_year_borrowing = float(last_year_record[0]["borrowing"]) if 'borrowing' in last_year_record and last_year_record["borrowing"]["S"] != "" else 0.0

                # 今年のCBの合計値を算出する
                this_year_total_cb  = loop_cb + loop_borrowing - (last_year_borrowing * 1.1) + last_year_banking

                # 全レコードの情報を入れるリスト
                all_data = {
                    "year_record":        year_record,
                    "this_year_total_cb": this_year_total_cb
                }
                all_sk_list.append(all_data)

                # プラスの場合
                if this_year_total_cb > 0:
                    total_cb_plus += this_year_total_cb

        # プーリンググループ内の合計CBを算出する
        print(f"group_total_lng:{(group_total_lng)} group_total_hfo:{(group_total_hfo)} group_total_lfo:{(group_total_lfo)} group_total_mdo:{(group_total_mdo)} group_total_mgo:{(group_total_mgo)}")
        group_total_energy = calc_energy(group_total_lng, group_total_hfo, group_total_lfo, group_total_mdo, group_total_mgo, fuel_oil_type_info_list)
        print(f"group_total_energy:{(group_total_energy)}")
        total_cb = calc_cb(year, group_total_energy, group_total_lng, group_total_hfo, group_total_lfo, group_total_mdo, group_total_mgo, fuel_oil_type_info_list)

        # プーリンググループ内の合計CBがプラスの場合、プラス分を按分する
        print(f"all_sk_list:{(all_sk_list)}")
        for all_sk in all_sk_list:

            banking_cb = 0
            # プーリンググループのCBがプラスの場合（fine_flag="0"）
            if total_cb > 0:
                # 単体のCBがプラスの場合
                if all_sk["this_year_total_cb"] > 0:
                    banking_cb = all_sk["this_year_total_cb"] / total_cb_plus * total_cb
                    insert.upsert_year_total(all_sk["year_record"], banking_cb, "0", pooling_group_info)
                # 単体のCBはゼロ以下→banking=0
                else:
                    insert.upsert_year_total(all_sk["year_record"], banking_cb, "0", pooling_group_info)

            # プーリンググループのCBがマイナスの場合（fine_flag="1"）
            else:
                insert.upsert_year_total(all_sk["year_record"], banking_cb, "1", pooling_group_info)

    print("banking upserting is finished.")
