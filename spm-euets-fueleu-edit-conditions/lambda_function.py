
import ast
from datetime import datetime
import json
import math
import re

import auth
from dynamodb import insert, select, delete
from Util import Util

# CO2排出量の算出メソッド
def calc_co2(year, lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_info_list):

    # EUAの算出
    co2_total   = 0
    eu_ets_rate = 0

    # EU-ETS対象割合を確認
    if year == "2024":
        eu_ets_rate = 40
    elif year == "2025":
        eu_ets_rate = 70
    else:
        eu_ets_rate = 100

    print(f"eu_ets_rate: {(eu_ets_rate)}")
    if lng_ods > 0:
        lng_ods_co2_factor =  float(fuel_oil_type_info_list["LNG_ODS_info_list"]["emission_factor"]["S"])
        co2_total += lng_ods * lng_ods_co2_factor
    if lng_oms > 0:
        lng_oms_co2_factor =  float(fuel_oil_type_info_list["LNG_OMS_info_list"]["emission_factor"]["S"])
        co2_total += lng_oms * lng_oms_co2_factor
    if lng_oss > 0:
        lng_oss_co2_factor =  float(fuel_oil_type_info_list["LNG_OSS_info_list"]["emission_factor"]["S"])
        co2_total += lng_oms * lng_oss_co2_factor
    if hfo > 0:
        hfo_co2_factor =  float(fuel_oil_type_info_list["HFO_info_list"]["emission_factor"]["S"])
        co2_total += hfo * hfo_co2_factor
    if lfo > 0:
        lfo_co2_factor =  float(fuel_oil_type_info_list["LFO_info_list"]["emission_factor"]["S"])
        co2_total += lfo * lfo_co2_factor
    if mdo > 0:
        mdo_co2_factor =  float(fuel_oil_type_info_list["MDO_info_list"]["emission_factor"]["S"])
        co2_total += mdo * mdo_co2_factor
    if mgo > 0:
        mgo_co2_factor =  float(fuel_oil_type_info_list["MGO_info_list"]["emission_factor"]["S"])
        co2_total += mgo * mgo_co2_factor
    if lpg_p > 0:
        lpg_p_co2_factor = float(fuel_oil_type_info_list["LPG_Propane_info_list"]["emission_factor"]["S"])
        co2_total += lpg_p * lpg_p_co2_factor
    if lpg_b > 0:
        lpg_b_co2_factor = float(fuel_oil_type_info_list["LPG_Butane_info_list"]["emission_factor"]["S"])
        co2_total += lpg_b * lpg_b_co2_factor
    if nh3_ng > 0:
        nh3_ng_co2_factor = float(fuel_oil_type_info_list["NH3_Ng_info_list"]["emission_factor"]["S"])
        co2_total += nh3_ng * nh3_ng_co2_factor
    if nh3_ef > 0:
        nh3_ef_co2_factor = float(fuel_oil_type_info_list["NH3_eFuel_info_list"]["emission_factor"]["S"])
        co2_total += nh3_ef * nh3_ef_co2_factor
    if methanol_ng > 0:
        methanol_ng_co2_factor = float(fuel_oil_type_info_list["Methanol_Ng_info_list"]["emission_factor"]["S"])
        co2_total = methanol_ng * methanol_ng_co2_factor
    if h2_ng > 0:
        h2_ng_co2_factor = float(fuel_oil_type_info_list["H2_Ng_info_list"]["emission_factor"]["S"])
        co2_total += h2_ng * h2_ng_co2_factor
        
    return co2_total

# EUAの算出メソッド
def calc_eua(year, total_co2):

    # EUAの算出
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

    eua       = total_co2 * float(eu_ets_rate) / 100
    print(f"eua{type(eua)}: {eua}")
    return eua

# エネルギーの総消費量を算出するメソッド
def calc_energy(lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_list):
    total_energy = 0

    if lng_ods > 0:
        lng_ods_lcv =  float(fuel_oil_type_list["LNG_ODS_info_list"]["lcv"]["S"])
        total_energy += lng_ods * lng_ods_lcv
    if lng_oms > 0:
        lng_oms_lcv =  float(fuel_oil_type_list["LNG_OMS_info_list"]["lcv"]["S"])
        total_energy += lng_oms * lng_oms_lcv
    if lng_oss > 0:
        lng_oss_lcv =  float(fuel_oil_type_list["LNG_OMS_info_list"]["lcv"]["S"])
        total_energy += lng_oss * lng_oss_lcv
    if hfo > 0:
        hfo_lcv =  float(fuel_oil_type_list["HFO_info_list"]["lcv"]["S"])
        total_energy += hfo * hfo_lcv
    if lfo > 0:
        lfo_lcv =  float(fuel_oil_type_list["LFO_info_list"]["lcv"]["S"])
        total_energy += lfo * lfo_lcv
    if mdo > 0:
        mdo_lcv =  float(fuel_oil_type_list["MDO_info_list"]["lcv"]["S"])
        total_energy += mdo * mdo_lcv
    if mgo > 0:
        mgo_lcv =  float(fuel_oil_type_list["MGO_info_list"]["lcv"]["S"])
        total_energy += mgo * mgo_lcv
    if lpg_p > 0:
        lpg_p_lcv = float(fuel_oil_type_list["LPG_Propane_info_list"]["lcv"]["S"])
        total_energy += lpg_p * lpg_p_lcv
    if lpg_b > 0:
        lpg_b_lcv = float(fuel_oil_type_list["LPG_Butane_info_list"]["lcv"]["S"])
        total_energy += lpg_b * lpg_b_lcv
    if nh3_ng > 0:
        nh3_ng_lcv = float(fuel_oil_type_list["NH3_Natural_Gas_info_list"]["lcv"]["S"])
        total_energy += nh3_ng * nh3_ng_lcv
    if nh3_ef > 0:
        nh3_ef_lcv = float(fuel_oil_type_list["NH3_eFuel_info_list"]["lcv"]["S"])
        total_energy += nh3_ef * nh3_ef_lcv
    if methanol_ng > 0:
        methanol_ng_lcv = float(fuel_oil_type_list["Methanol_Natural_Gas_info_list"]["lcv"]["S"])
        total_energy += methanol_ng * methanol_ng_lcv
    if h2_ng > 0:
        h2_ng_lcv = float(fuel_oil_type_list["Methanol_Natural_Gas_info_list"]["lcv"]["S"])
        total_energy += h2_ng * h2_ng_lcv

    return_energy = total_energy

    return return_energy

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

#実際のGHG強度を算出するメソッド
def calc_GHG_Actual(lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_list):
    sum_ghg = 0
    sum_foc = 0

    if lng_ods > 0:
        lng_ghg_ods_intensity =  float(fuel_oil_type_list["LNG_ODS_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lng_ods * lng_ghg_ods_intensity
        sum_foc += lng_ods
    if lng_oms > 0:
        lng_ghg_oms_intensity =  float(fuel_oil_type_list["LNG_OMS_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lng_oms * lng_ghg_oms_intensity
        sum_foc += lng_oms
    if lng_oss > 0:
        lng_ghg_oss_intensity =  float(fuel_oil_type_list["LNG_OSS_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lng_oss * lng_ghg_oss_intensity
        sum_foc += lng_oss
    if hfo > 0:
        hfo_ghg_intensity =  float(fuel_oil_type_list["HFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += hfo * hfo_ghg_intensity
        sum_foc += hfo
    if lfo > 0:
        lfo_ghg_intensity =  float(fuel_oil_type_list["LFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lfo * lfo_ghg_intensity
        sum_foc += lfo
    if mdo > 0:
        mdo_ghg_intensity =  float(fuel_oil_type_list["MDO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += mdo * mdo_ghg_intensity
        sum_foc += mdo
    if mgo > 0:
        mgo_ghg_intensity =  float(fuel_oil_type_list["MGO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += mgo * mgo_ghg_intensity
        sum_foc += mgo
    if lpg_p > 0:
        lpg_p_ghg_intensity =  float(fuel_oil_type_list["LPG_Propane_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lpg_p * lpg_p_ghg_intensity
        sum_foc += lpg_p
    if lpg_b > 0:
        lpg_b_ghg_intensity =  float(fuel_oil_type_list["LPG_Butane_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lpg_b * lpg_b_ghg_intensity
        sum_foc += lpg_b
    if nh3_ng > 0:
        nh3_ng_ghg_intensity =  float(fuel_oil_type_list["NH3_Natural_Gas_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ng * nh3_ng_ghg_intensity
        sum_foc += nh3_ng
    if nh3_ef > 0:
        nh3_ef_ghg_intensity =  float(fuel_oil_type_list["NH3_eFuel_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ef * nh3_ef_ghg_intensity
        sum_foc += nh3_ef
    if methanol_ng > 0:
        methanol_ng_ghg_intensity =  float(fuel_oil_type_list["Methanol_Natural_Gas_info_list"]["ghg_intensity"]["S"])
        sum_ghg += methanol_ng * methanol_ng_ghg_intensity
        sum_foc += methanol_ng
    if h2_ng > 0:
        h2_ng_ghg_intensity =  float(fuel_oil_type_list["H2_Natural_Gas_info_list"]["ghg_intensity"]["S"])
        sum_ghg += h2_ng * h2_ng_ghg_intensity
        sum_foc += h2_ng

    GHG_Actual = 0
    if sum_foc != 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

# コンプライアンスバランスを算出するメソッド
def calc_cb(year_timestamp, energy, GHG_Actual):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    return cb

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

def make_fuel_oil_type_info_list():

    # Ecoで使用する燃料の情報リスト
    fuel_oil_info_list = {
        "HFO_info_list": [],
        "LFO_info_list": [],
        "MDO_info_list": [],
        "MGO_info_list": [],
        "LNG_OMS_info_list": [],
        "LNG_OSS_info_list": [],
        "LNG_ODS_info_list": [],
        "LPG_Butane_info_list": [],
        "LPG_Puropane_info_list": [],
        "H2_Ng_info_list"       : [],
        "NH3_Ng_info_list"      : [],
        "Methanol_Ng_info_list" : [],
        "NH3_eFuel_info_list"   : []
    }

    # 燃料情報リストを取得し、データセットを作成する
    fuel_oil_name_list = ["HFO", "LFO", "MDO", "MGO", "LNG(Otto Medium Speed)", "LNG(Otto Slow Speed)", "LNG(Otto Diesel Speed)", "LPG(Butane)", "LPG(Propane)", "H2(Natural gas)", "NH3(Natural gas)", "Methanol(Natural gas)", "NH3(e-fuel)"]
    fuel_oil_type_info_list = []

    for fuel_oil_name in fuel_oil_name_list:
        fuel_oil_type_info_list.append(select.get_fuel_oil_type_by_oiletype(fuel_oil_name)[0])

    for fuel_oil_type_info in fuel_oil_type_info_list:
        name = fuel_oil_type_info["fuel_oil_type"]["S"]

        # それぞれの
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
        elif name == "LNG(Otto Slow Speed)":
            fuel_oil_info_list["LNG_OSS_info_list"] = fuel_oil_type_info
        elif name == "LNG(Otto Diesel Speed)":
            fuel_oil_info_list["LNG_ODS_info_list"] = fuel_oil_type_info
        elif name == "LPG(Butane)":
            fuel_oil_info_list["LPG_Butane_info_list"] = fuel_oil_type_info
        elif name == "LPG(Propane)":
            fuel_oil_info_list["LPG_Puropane_info_list"] = fuel_oil_type_info
        elif name == "H2(Natural gas)":
            fuel_oil_info_list["H2_Ng_info_list"] = fuel_oil_type_info
        elif name == "NH3(Natural gas)":
            fuel_oil_info_list["NH3_Ng_info_list"] = fuel_oil_type_info
        elif name == "Methanol(Natural gas)":
            fuel_oil_info_list["Methanol_Ng_info_list"] = fuel_oil_type_info
        elif name == "NH3(e-fuel)":
            fuel_oil_info_list["NH3_eFuel_info_list"] = fuel_oil_type_info

    return fuel_oil_info_list, fuel_oil_name_list

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    foc = 0
    foc_str = ""
    leg_count = 0
    
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
    fuel_oil_type_info_list, FuelOilList = make_fuel_oil_type_info_list()

    # 新規Simulationテーブル登録
    for item in edit_conditions_list:

        leg_count += 1

        # EU Rate 取得
        eu_rate       = int(item["eu_rate"])

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

            # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
            auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])
            print(f"auxiliary_equipment: {(auxiliary_equipment)}")

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
            foc_per_day = alpah * log_speed ** a + c + auxiliary_equipment
            # 1時間あたりのFOC算出
            foc_per_hour = foc_per_day / 24
            # 総FOCを算出
            foc = foc_per_hour * total_time * eu_rate / 100
            foc_str = str(round(foc, 1))
            print(f"foc: {(foc)}")

            # 以下で"eua", "cb" を算出する。
            # simulation-voyage-planの燃料名と割合部分を取得する
            # fuel_list = ast.literal_eval()   # [(燃料, 割合%), (), ...]
            fuel = convertFuelOileStringToList(item["fuel"])   # [(燃料, 割合%), (), ...]

            for i in range(len(fuel)):
                # focと割合から各種燃料消費量を算出する。
                fuel_info = fuel[i].split(',')
                fuel_type = fuel_info[0]
                fuel_rate = int(fuel_info[1])

                simulation_leg_lng_oms = 0
                simulation_leg_lng_oss = 0
                simulation_leg_lng_ods = 0
                simulation_leg_hfo = 0
                simulation_leg_lfo = 0
                simulation_leg_mdo = 0
                simulation_leg_mgo = 0
                simulation_leg_lpg_p = 0
                simulation_leg_lpg_b = 0
                simulation_leg_h2_ng = 0
                simulation_leg_nh3_ng = 0
                simulation_leg_methanol_ng = 0
                simulation_leg_nh3_ef = 0

                if  fuel_type == "LNG(Otto Medium Speed)":
                    simulation_leg_lng_oms = foc * int(fuel_rate) / 100
                elif  fuel_type == "LNG(Otto Slow Speed)":
                    simulation_leg_lng_oss = foc * int(fuel_rate) / 100
                elif  fuel_type == "LNG(Otto Diesel Speed)":
                    simulation_leg_lng_ods = foc * int(fuel_rate) / 100
                elif fuel_type == "HFO":
                    simulation_leg_hfo = foc * int(fuel_rate) / 100
                elif fuel_type == "LFO":
                    simulation_leg_lfo = foc * int(fuel_rate) / 100
                elif fuel_type == "MDO":
                    simulation_leg_mdo = foc * int(fuel_rate) / 100
                elif fuel_type == "MGO":
                    simulation_leg_mgo = foc * int(fuel_rate) / 100
                elif fuel_type == "LPG(Propane)":
                    simulation_leg_lpg_p = foc * int(fuel_rate) / 100
                elif fuel_type == "LPG(Butane)":
                    simulation_leg_lpg_b = foc * int(fuel_rate) / 100
                elif fuel_type == "H2(Natural gas)":
                    simulation_leg_h2_ng = foc * int(fuel_rate) / 100
                elif fuel_type == "NH3(Natural gas)":
                    simulation_leg_nh3_ng = foc * int(fuel_rate) / 100
                elif fuel_type == "Methanol(Natural gas)":
                    simulation_leg_methanol_ng = foc * int(fuel_rate) / 100
                elif fuel_type == "NH3(e-fuel)":
                    simulation_leg_nh3_ef = foc * int(fuel_rate) / 100
           
            # シミュレーション部分で実際に排出したco2を算出する
            simulation_leg_co2 = calc_co2(year_now, simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
            # シミュレーション部分のEUAを算出する
            simulation_leg_eua = calc_eua(year_now, simulation_leg_co2)
            eua_str = str(float(simulation_leg_eua))
            
            # CB算出
            simulation_leg_GHG = calc_GHG_Actual(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
            simulation_energy  = calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
            cb_str  = str(calc_cb(year_now, simulation_energy, simulation_leg_GHG))

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
            "eua"            : str(round(float(res_item["eua"]["S"]), 1)),
            "cb"             : str(round(float(res_item["cb"]["S"]) / 1000000, 1))
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
    