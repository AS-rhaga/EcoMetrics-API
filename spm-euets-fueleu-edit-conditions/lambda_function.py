
import ast
from datetime import datetime
import json
import math
import re

import auth
from dynamodb import insert, select, delete
from Util import Util

# Ecoで使用する燃料の情報リスト
LNG_info_list = {}
HFO_info_list = {}
LFO_info_list = {}
MDO_info_list = {}
MGO_info_list = {}

def calc_EUA(year, eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo):

    # EUAの算出
    co2_lng = 0
    co2_hfo = 0
    co2_lfo = 0
    co2_mdo = 0
    co2_mgo = 0
    eu_ets_rate = 0
    eua = 0

    # EU Rateの確認
    if eu_rate == 0:
        # EU外航海は対象外なのでゼロ
        total_co2 = 0
    else:
        # EU-ETS対象割合を確認
        if year == "2024":
            eu_ets_rate = 40
        elif year == "2025":
            eu_ets_rate = 70
        else:
            eu_ets_rate = 100

        print(f"eu_ets_rate: {(eu_ets_rate)}")
        if total_lng > 0:
            lng_co2_factor =  float(LNG_info_list["emission_factor"]["S"])
            co2_lng = total_lng * lng_co2_factor
        if total_hfo > 0:
            hfo_co2_factor =  float(HFO_info_list["emission_factor"]["S"])
            co2_hfo = total_hfo * hfo_co2_factor
        if total_lfo > 0:
            lfo_co2_factor =  float(LFO_info_list["emission_factor"]["S"])
            co2_lfo = total_lfo * lfo_co2_factor
        if total_mdo > 0:
            mdo_co2_factor =  float(MDO_info_list["emission_factor"]["S"])
            co2_mdo = total_mdo * mdo_co2_factor
        if total_mgo > 0:
            mgo_co2_factor =  float(MGO_info_list["emission_factor"]["S"])
            co2_mgo = total_mgo * mgo_co2_factor

        # CO2の総排出量(MT)
        total_co2 = co2_lng + co2_hfo + co2_lfo + co2_mdo + co2_mgo
        print(f"total_co2{type(total_co2)}: {total_co2}")
        eua       = total_co2 * float(eu_ets_rate) / 100 * float(eu_rate) / 100
        eua_formatted = Util.format_to_one_decimal(round(float(eua), 1))
        print(f"eua_formatted{type(eua_formatted)}: {eua_formatted}")
    return str(eua_formatted)

def calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo):
    energy_lng = 0
    energy_hfo = 0
    energy_lfo = 0
    energy_mdo = 0
    energy_mgo = 0

    if total_lng > 0:
        lng_lcv =  float(LNG_info_list["lcv"]["S"])
        energy_lng += total_lng * lng_lcv
    if total_hfo > 0:
        hfo_lcv =  float(HFO_info_list["lcv"]["S"])
        energy_hfo += total_hfo * hfo_lcv
    if total_lfo > 0:
        lfo_lcv =  float(LFO_info_list["lcv"]["S"])
        energy_lfo += total_lfo * lfo_lcv
    if total_mdo > 0:
        mdo_lcv =  float(MDO_info_list["lcv"]["S"])
        energy_mdo += total_mdo * mdo_lcv
    if total_mgo > 0:
        mgo_lcv =  float(MGO_info_list["lcv"]["S"])
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

def calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo):
    sum_ghg = 0
    sum_foc = 0

    if total_lng > 0:
        lng_ghg_intensity =  float(LNG_info_list["ghg_intensity"]["S"])
        sum_ghg += total_lng * lng_ghg_intensity
        sum_foc += total_lng
    if total_hfo > 0:
        hfo_ghg_intensity =  float(HFO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_hfo * hfo_ghg_intensity
        sum_foc += total_hfo
    if total_lfo > 0:
        lfo_ghg_intensity =  float(LFO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_lfo * lfo_ghg_intensity
        sum_foc += total_lfo
    if total_mdo > 0:
        mdo_ghg_intensity =  float(MDO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_mdo * mdo_ghg_intensity
        sum_foc += total_mdo
    if total_mgo > 0:
        mgo_ghg_intensity =  float(MGO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_mgo * mgo_ghg_intensity
        sum_foc += total_mgo

    GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    cb_formatted = str(round(float(cb), 1))
    print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")
    return cb_formatted

# datetime型のstartとendの時間差を返却。30分以上の場合は繰り上がり。
def calc_time_diff(start_time, end_time):

    return_val = 0
                        
    time_difference = end_time - start_time
    hours_difference = time_difference.total_seconds() / 3600

    # 30分以上で繰り上げ
    if hours_difference % 1 >= 0.5:
        return_val = math.ceil(hours_difference)  # 繰り上げ
    else:
        return_val = math.floor(hours_difference)  # 切り捨て
    
    return return_val

# FuelListをリストに変換
def convertFuelOileStringToList(text):
    
    pattern = r'\([^()]*\([^()]*\)[^()]*\)|\([^()]*\)'
    matches = re.findall(pattern, text)

    # 前後の括弧を除去
    cleaned_matches = [match[1:-1] for match in matches]

    return cleaned_matches

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    global LNG_info_list
    global HFO_info_list
    global LFO_info_list
    global MDO_info_list
    global MGO_info_list

    foc = 0
    foc_str = ""
    leg_count = 0

    total_lng = 0
    total_hfo = 0
    total_lfo = 0
    total_mdo = 0
    total_mgo = 0
    
    body = event['body']
    token = event['headers']['Authorization']
    
    edit_conditions_list = json.loads(body)
    imo = edit_conditions_list[0]["imo"]

    # 認可：IMO参照権限チェック
    authCheck = auth.imo_check(token, imo)
    if authCheck == 401 or authCheck == 500:
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin" : "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },  
            'body': authCheck
        }

    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # 既存Simulationテーブル削除
    delete.delete_simulation(imo)

    # FOC Formulas取得
    res_foc_formulas = select.get_foc_formulas(imo)

    # 燃料情報を取得
    # fuel_oil_type_info_list = select.get_fuel_oil_type()
    fuel_oil_type_info_list = [
        {'fuel_oil_type_name': {'S': 'Marine Gas Oil'}, 'ghg_intensity': {'S': '90.8'}, 'fuel_oil_type': {'S': 'MGO'}, 'lcv': {'S': '0.0427'}, 'emission_factor': {'S': '3.206'}}, 
        {'fuel_oil_type': {'S': 'LPG(Propane)'}, 'fuel_oil_type_name': {'S': 'Liquefied petroleum gas (Propane)'}, 'emission_factor': {'S': '3.000'}}, 
        {'fuel_oil_type': {'S': 'Ethanol'}, 'fuel_oil_type_name': {'S': 'Ethanol'}, 'emission_factor': {'S': '1.913'}}, 
        {'fuel_oil_type': {'S': 'Methanol'}, 'fuel_oil_type_name': {'S': 'Methanol'}, 'emission_factor': {'S': '1.375'}}, 
        {'fuel_oil_type_name': {'S': 'Marine Diesel Oil'}, 'ghg_intensity': {'S': '90.8'}, 'fuel_oil_type': {'S': 'MDO'}, 'lcv': {'S': '0.0427'}, 'emission_factor': {'S': '3.206'}}, 
        {'fuel_oil_type_name': {'S': 'Heavy fuel oil'}, 'ghg_intensity': {'S': '91.7'}, 'fuel_oil_type': {'S': 'HFO'}, 'lcv': {'S': '0.0405'}, 'emission_factor': {'S': '3.114'}}, 
        {'fuel_oil_type': {'S': 'LPG(Butane)'}, 'fuel_oil_type_name': {'S': 'Liquefied petroleum gas (Butane)'}, 'emission_factor': {'S': '3.030'}}, 
        {'fuel_oil_type_name': {'S': 'Liquefied natural gas'}, 'ghg_intensity': {'S': '89.2'}, 'fuel_oil_type': {'S': 'LNG'}, 'lcv': {'S': '0.0491'}, 'emission_factor': {'S': '2.750'}}, 
        {'fuel_oil_type_name': {'S': 'Light fuel oil'}, 'ghg_intensity': {'S': '91.4'}, 'fuel_oil_type': {'S': 'LFO'}, 'lcv': {'S': '0.041'}, 'emission_factor': {'S': '3.151'}}
    ]
    for i in range(len(fuel_oil_type_info_list)):
        name = fuel_oil_type_info_list[i]["fuel_oil_type"]["S"]
        if  name == "LNG":
            LNG_info_list = fuel_oil_type_info_list[i]
        elif name == "HFO":
            HFO_info_list = fuel_oil_type_info_list[i]
        elif name == "LFO":
            LFO_info_list = fuel_oil_type_info_list[i]
        elif name == "MDO":
            MDO_info_list = fuel_oil_type_info_list[i]
        elif name == "MGO":
            MGO_info_list = fuel_oil_type_info_list[i]

    # edit_conditions_list = {
    #     {
    #         "imo"                   : "9876543",
    #         "year_and_serial_number": "2024E1",
    #         "operator"              : "NYK",
    #         "departure_port"        : "Departute",
    #         "departure_time"        : "2024/12/12 13:00",
    #         "arrival_port"          : "Arrival",
    #         "arrival_time"          : "2024/ 12/16 19:00",
    #         "distance"              : "5000", 
    #         "displacement"          : "Ballast",
    #         "fuel"                  : {
    #             {
    #                 "fuel_type": "HFO",
    #                 "fuel_rate": "60"
    #             },
    #             {
    #                 "fuel_type": "LFO",
    #                 "fuel_rate": "40" 
    #             }
    #         },
    #         "eu_rate"               : "50"
    #     }
    # }
    # res_foc_formulas = {
    #     {
    #         "imo"                : "9876543",
    #         "me_ballast"         : ["0.013", "2.85", "0"],
    #         "me_laden"           : ["0.018", "2.85", "0"],
    #         "auxiliary_equipment": "8.2"
    #     }
    # }

    # 新規Simulationテーブル登録
    for item in edit_conditions_list:

        leg_count += 1

        # EU Rate 取得
        eu_rate       = item["eu_rate"]

        # 以下でitem内に無い項目を算出する。

        # "total_time" を算出する。
        # DepartureTime取得
        departure_time_string = item["departure_time"]
        departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")

        # ArrivalTime取得
        arrival_time_string = item["arrival_time"]
        arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")

        # Leg航海時間算出
        total_time = calc_time_diff(departure_time, arrival_time)


        # "log_speed" を算出する。
        log_speed     = int(item["distance"]) / total_time
        log_speed_str = str(round(log_speed, 1))

        # "foc", "eua" を算出する。（FOC Formulasが取得出来なかった場合は計算しない）
        if res_foc_formulas:
            # FOC算出時にBallast/Ladenどちらの式を使うかを判定
            if item["dispracement"] == "Ballast":
                # Ballast用の計算パラメータを取得し、FOCを算出
                calc_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])

            else:
                # 上記以外の場合（実質Laden）                       
                # Laden用の計算パラメータを取得し、FOCを算出
                calc_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])

            alpah = calc_param_list[0]
            a = calc_param_list[1]
            c = calc_param_list[2]

            # 1日あたりのFOC算出（**は指数）
            foc_per_day = alpah * log_speed ** a + c
            # 1時間あたりのFOC算出
            foc_per_hour = foc_per_day / 24
            # 総FOCを算出
            foc = foc_per_hour * total_time
            foc_str = str(round(foc, 1))
            print(f"foc: {(foc)}")

            # 以下で"eua", "cb" を算出する。
            # simulation-voyage-planの燃料名と割合部分を取得する
            # fuel_list = ast.literal_eval()   # [(燃料, 割合%), (), ...]
            fuel = convertFuelOileStringToList(item["fuel"])   # [(燃料, 割合%), (), ...]

            for i in range(len(fuel)):
                # focと割合から各種燃料消費量を算出する。
                fuel_info = fuel[i].split(',')
                fuel_name = fuel_info[0]
                if  fuel_name == "LNG":
                    total_lng = foc * (int(fuel_info[1]) / 100)
                elif fuel_name == "HFO":
                    total_hfo = foc * (int(fuel_info[1]) / 100)
                elif fuel_name == "LFO":
                    total_lfo = foc * (int(fuel_info[1]) / 100)
                elif fuel_name == "MDO":
                    total_mdo = foc * (int(fuel_info[1]) / 100)
                elif fuel_name == "MGO":
                    total_mgo = foc * (int(fuel_info[1]) / 100)
            print(f"total_lng: {(total_lng)}, total_hfo: {(total_hfo)}, total_lfo: {(total_lfo)}, total_mdo: {(total_mdo)}, total_mgo: {(total_mgo)}")

            eua_str = calc_EUA(year_now, eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo)
            energy  = calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo)
            print(f"year_now:{year_now}, energy:{energy}, total_lng:{total_lng}, total_hfo:{total_hfo}, total_lfo:{total_lfo}, total_mdo:{total_mdo}, total_mgo:{total_mgo}")
            cb_str  = calc_cb(year_now, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo)

        else:
            foc_str = "-"
            eua_str = "-"
            cb_str  = "-"

        upsert_data = {
            "imo"                     : imo,
            "year_and_serial_number"  : item["year_and_serial_number"],
            "operator"                : item["operator"],
            "departure_port"          : item["departure_port"],
            "departure_time"          : item["departure_time"],
            "arrival_port"            : item["arrival_port"],
            "arrival_time"            : item["arrival_time"],
            "distance"                : item["distance"],
            "eu_rate"                 : item["eu_rate"],
            "fuel"                    : item["fuel"],
            "dispracement"            : item["dispracement"],
            "log_speed"               : log_speed_str,
            "foc"                     : foc_str,
            "eua"                     : eua_str,
            "cb"                      : cb_str
        }
        insert.upsert_simulation_voyage_plan(year_now, upsert_data)

    # Simulationテーブル取得
    res_simulation = select.get_simulation_voyage_plan(imo)

    # 返却値設定
    data_list = []
    for res_item in res_simulation:

        # Leg No 取得
        tmp_text = res_item["year_and_serial_number"]["S"]
        start_index = tmp_text.find('E')
        leg_no = tmp_text[start_index:]  # 'E' 以降を抽出

        # total time算出
        # DepartureTime取得
        departure_time_string = res_item["departure_time"]["S"]
        departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")

        # ArrivalTime取得
        arrival_time_string = res_item["arrival_time"]["S"]
        arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")

        # Leg航海時間算出
        total_time = calc_time_diff(departure_time, arrival_time)

        #Fuel取得
        output_fuel_list = []
        fuel_list = convertFuelOileStringToList(res_item["fuel"]["S"]) 

        for fuel in fuel_list:
            fuel_info_list = fuel.split(',')

            output_fuel = {
                "fuel_type" : fuel_info_list[0],
                "fuel_rate" : fuel_info_list[1],
            }

            output_fuel_list.append(output_fuel)

        data = {
            "leg_no"  : leg_no,
            "operator" : res_item["operator"]["S"],
            "departure_port" : res_item["departure_port"]["S"],
            "departure_time" : res_item["departure_time"]["S"],
            "arrival_port"   : res_item["arrival_port"]["S"],
            "arrival_time"   : res_item["arrival_time"]["S"],
            "total_time"     : str(total_time),
            "distance"       : res_item["distance"]["S"],
            "eu_rate"        : res_item["eu_rate"]["S"],
            "fuel"           : output_fuel_list,
            "displacement"   : res_item["dispracement"]["S"],
            "log_speed"      : res_item["log_speed"]["S"],
            "foc"            : res_item["foc"]["S"],
            "eua"            : res_item["eua"]["S"],
            "cb"             : res_item["cb"]["S"]
        }

        data_list.append(data)

    # ソート実行-------------------------------------------------------
    new_data_list = sorted(data_list, key=lambda x: x['leg_no'])

    datas = {
        "datas": new_data_list
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
    