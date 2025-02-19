
# from Tools.reuse import S3ConfigTools, DynamoDBConfigTools
# import requests
import os
import json
from datetime import datetime
from dynamodb import select, upsert
import boto3
import ast

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

def main(imo, timestamp):

    operator_list       = []
    operator_total_list = []

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

    # leg-totalデータ取得 →Voyage-totalを取得
    res_voyage = select.get_voyage_total(imo, year_timestamp)
    # print(f"res_leg[{type(res_leg)}]: {res_leg}")

    for i in range(len(res_voyage)):

        print(f"{(i+1)}voyage目を処理")

        # voyage-totalの各項目を取得
        operator       = res_voyage[i]["operator"]["S"]
        distance       = float(res_voyage[i]["distance"]["S"])
        total_lng      = float(res_voyage[i]["total_lng"]["S"])
        total_hfo      = float(res_voyage[i]["total_hfo"]["S"])
        total_lfo      = float(res_voyage[i]["total_lfo"]["S"])
        total_mdo      = float(res_voyage[i]["total_mdo"]["S"])
        total_mgo      = float(res_voyage[i]["total_mgo"]["S"])
        total_foc      = float(res_voyage[i]["total_foc"]["S"])
        total_energy   = float(res_voyage[i]["total_energy"]["S"])
        eua            = float(res_voyage[i]["eua"]["S"])

        # dt_timestamp_date_from = datetime(year = int(departure_time[0:4]), month = int(departure_time[5:7]), day = int(departure_time[8:10]) + 1, hour = 00, minute = 00, second = 00, microsecond = 000000)
        # dt_timestamp_date_from = dt_timestamp_date_from.strftime('%Y-%m-%dT%H:%M:%SZ')
        # dt_timestamp_date_to   = datetime(year = int(departure_time[0:4]), month = int(departure_time[5:7]), day = int(departure_time[8:10]) + 2, hour = 23, minute = 59, second = 59, microsecond = 999999)
        # dt_timestamp_date_to   = dt_timestamp_date_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        # print(f"date_from: {(dt_timestamp_date_from)}, date_to: {(dt_timestamp_date_to)}")

        # opeリストを作成する
        if operator in operator_list:

            # 燃料消費量を足し合わせるグループを探す
            list_index = 0
            for operator_total in operator_total_list:
                    
                if operator_total["operator"] == operator:
                    print(f"ループ({(list_index+1)})  operator_total_list[{(list_index)}]:{(operator_total_list[list_index])}")

                    # 合計値（加算前）を取得する。
                    bk_operator_total_distance = operator_total["year_total_distance"]
                    bk_operator_total_lng      = operator_total["year_total_lng"]
                    bk_operator_total_hfo      = operator_total["year_total_hfo"]
                    bk_operator_total_lfo      = operator_total["year_total_lfo"]
                    bk_operator_total_mdo      = operator_total["year_total_mdo"]
                    bk_operator_total_mgo      = operator_total["year_total_mgo"]
                    bk_operator_total_foc      = operator_total["year_total_foc"]
                    bk_operator_total_energy   = operator_total["year_total_energy"]
                    bk_operator_total_eua      = operator_total["year_total_eua"]

                    # プーリンググループの合計値を加算した値に書き換える。
                    operator_total["year_total_distance"] = bk_operator_total_distance + distance
                    operator_total["year_total_lng"]      = bk_operator_total_lng + total_lng
                    operator_total["year_total_hfo"]      = bk_operator_total_hfo + total_hfo
                    operator_total["year_total_lfo"]      = bk_operator_total_lfo + total_lfo
                    operator_total["year_total_mdo"]      = bk_operator_total_mdo + total_mdo
                    operator_total["year_total_mgo"]      = bk_operator_total_mgo + total_mgo
                    operator_total["year_total_foc"]      = bk_operator_total_foc + total_foc
                    operator_total["year_total_energy"]   = bk_operator_total_energy + total_energy
                    operator_total["year_total_eua"]      = bk_operator_total_eua + eua

                    # リストを更新する。
                    operator_total_list[list_index] = operator_total
                    print(f"加算後のoperator_total_list[{type(operator_total_list)}]: {operator_total_list}")


                list_index += 1

        # opeリストに追加、新規データセットを作成する
        else:
            operator_list.append(operator)
            print(f"operator_list[{type(operator_list)}]: {operator_list}")

            data_list = {
                "operator"           : operator,
                "year_total_distance": distance,
                "year_total_lng"     : total_lng,
                "year_total_hfo"     : total_hfo,
                "year_total_lfo"     : total_lfo,
                "year_total_mdo"     : total_mdo,
                "year_total_mgo"     : total_mgo,
                "year_total_foc"     : total_foc,
                "year_total_energy"  : total_energy,
                "year_total_eua"     : eua
            }
            operator_total_list.append(data_list)
            print(f"追加後のoperator_total_list[{type(operator_total_list)}]: {operator_total_list}")

    # 以下、合計値データセットでオペレーター分ループ
    for operator_total in operator_total_list:

        # オペレーター毎の合計値を取得する
        operator                = operator_total["operator"]
        operator_total_distance = operator_total["year_total_distance"]
        operator_total_lng      = operator_total["year_total_lng"]
        operator_total_hfo      = operator_total["year_total_hfo"]
        operator_total_lfo      = operator_total["year_total_lfo"]
        operator_total_mdo      = operator_total["year_total_mdo"]
        operator_total_mgo      = operator_total["year_total_mgo"]
        operator_total_foc      = operator_total["year_total_foc"]
        operator_total_energy   = operator_total["year_total_energy"]
        operator_total_eua      = operator_total["year_total_eua"]

        # CBの算出
        operator_total_cb     = calc_cb(year_timestamp, operator_total_energy, operator_total_lng, operator_total_hfo, operator_total_lfo, operator_total_mdo, operator_total_mgo, fuel_oil_type_info_list)
        print(f"year_total_cb[{type(operator_total_cb)}]: {operator_total_cb}")

        year_and_ope = year_timestamp + operator

        # 去年分のyearレコードを取得する
        last_year_and_ope = str(int(year_timestamp) - 1) + operator
        last_year_record = select.get_year_total(imo, last_year_and_ope)
        print(f"last_year_record: {(last_year_record)}")

        last_year_borrowing_cb = 0
        last_year_banking_cb   = 0
        if len(last_year_record) > 0:
            last_year_borrowing_cb = float(last_year_record[0]["borrowing"]["S"]) if "borrowing" in last_year_record[0] and last_year_record[0]["borrowing"]["S"] != "" else 0.0
            last_year_banking_cb   = float(last_year_record[0]["banking"]["S"]) if "banking" in last_year_record[0] and last_year_record[0]["banking"]["S"] != "" else 0.0

            if last_year_borrowing_cb > 0:
                if operator_total_cb - last_year_borrowing_cb * 1.1 > 0:
                    banking = operator_total_cb - last_year_borrowing_cb * 1.1
                else:
                    banking = 0
                    fine_flag = "1"
            
            elif last_year_banking_cb > 0:
                if last_year_banking_cb + operator_total_cb > 0:
                    banking = last_year_banking_cb + operator_total_cb
                else:
                    banking = 0
                    fine_flag = "1"

            elif operator_total_cb > 0:
                banking = operator_total_cb
            else:
                banking = 0
                fine_flag = "1"

        # 更新前のyear-totalレコードを取得し、既存のborrowingを保持する
        bk_str_borrowing = ""
        bk_year_total = select.get_year_total(imo, year_and_ope)
        print(f"bk_year_total:{(bk_year_total)}")

        bk_pooling_info = ""
        if bk_year_total:
            bk_str_borrowing = bk_year_total[0]["borrowing"]["S"] if "borrowing" in bk_year_total[0] and bk_year_total[0]["borrowing"]["S"] != "" else "0.0"
            print(f"bk_str_borrowing:{(bk_str_borrowing)}")

            # プーリンググループを取得する
            bk_pooling_info = bk_year_total[0]["pooling_group"]["S"] if "pooling_group" in bk_year_total[0] and bk_year_total[0]["pooling_group"]["S"] != "" else ""
            print(f"bk_pooling_info{type(bk_pooling_info)}:{(bk_pooling_info)}")
            if bk_pooling_info != "":
                bk_pooling_info_list = bk_pooling_info.split(", ")
                print(f"bk_pooling_info_list{type(bk_pooling_info_list)}:{(bk_pooling_info_list)}")
                company_id = bk_pooling_info_list[0]
                group_name = bk_pooling_info_list[1]

                print(f"company_id: {(company_id)}")
                company_and_year = year_timestamp + company_id
                pooling_record = select.get_pooling_table(company_and_year, group_name)[0]
                print(f"pooling_record:{(pooling_record)}")

                total_cb_plus = 0
                total_cb      = 0
                
                if len(pooling_record)> 0:
                    imo_list = ast.literal_eval(pooling_record["imo_list"]["S"])
                    imo_list = list(set(imo_list))

                    # プーリンググループの中にimoがある場合
                    if imo in imo_list:
                        for j in range(len(imo_list)):
                            loop_imo = imo_list[j]
                            print(f"loop_imo:{(loop_imo)}, ")
                            loop_year_list = select.get_year_total_by_year(loop_imo, year_now)
                            for loop_year_record in loop_year_list:
                                loop_cb = float(loop_year_record["cb"]["S"]) if 'cb' in loop_year_record and loop_year_record["cb"]["S"] != "" else 0.0
                                print(f"loop_cb:{(loop_cb)}")

                                if loop_cb > 0:
                                    total_cb_plus += loop_cb
                                
                                total_cb += loop_cb

                        # プーリンググループ内の合計CBがマイナスの場合、船単体も罰金対象とする（実際にはならない）
                        if total_cb < 0:
                            fine_flag = "1"
                        # プーリンググループ内の合計CBがプラスの場合、プラス分を按分する
                        elif total_cb > 0:
                            banking = total_cb * operator_total_cb / total_cb_plus
                    
                    # プーリンググループの中にimoがない（プーリンググループから外されている）場合
                    else:
                        bk_pooling_info = ""
                        if operator_total_cb + last_year_banking_cb - last_year_borrowing_cb * 1.1 > 0:
                            fine_flag = "1"

                # プーリンググループが取得できない（そのグループがなくなっている）場合
                else:
                    bk_pooling_info = ""

        operator_total_distance = str(float(operator_total_distance))
        operator_total_lng      = str(float(operator_total_lng))
        operator_total_hfo      = str(float(operator_total_hfo))
        operator_total_lfo      = str(float(operator_total_lfo))
        operator_total_mdo      = str(float(operator_total_mdo))
        operator_total_mgo      = str(float(operator_total_mgo))
        operator_total_foc      = str(float(operator_total_foc))
        operator_total_eua      = str(float(operator_total_eua))
        operator_total_cb       = str(float(operator_total_cb))
        banking                 = str(float(banking))

        dataset = {
            "imo"         : imo,
            "year_and_ope": year_and_ope,
            "distance"    : operator_total_distance,
            "total_lng"   : operator_total_lng,
            "total_hfo"   : operator_total_hfo,
            "total_lfo"   : operator_total_lfo,
            "total_mdo"   : operator_total_mdo,
            "total_mgo"   : operator_total_mgo,
            "total_foc"   : operator_total_foc,
            "eua"         : operator_total_eua,
            "cb"          : operator_total_cb,
            "banking"     : banking,
            "borrowing"   : bk_str_borrowing,
            "fine_flag"   : fine_flag,
            "pooling_group": bk_pooling_info,
            "timestamp"   : dt_now_str
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
        print("Upserting eco-euets-fueleu-year-total is succeeded.")

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }