
import math
import json
from datetime import datetime
import ast
import re

import auth
from dynamodb import select
from Util import Util

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        "LPG_Propane_info_list": [],
        "H2_Ng_info_list"       : [],
        "NH3_Ng_info_list"      : [],
        "Methanol_Ng_info_list" : [],
        "NH3_eFuel_info_list"   : []
    }

    # 燃料情報リストを取得し、データセットを作成する
    fuel_oil_name_list = ["HFO", "LFO", "MDO", "MGO", "LNG(Otto Medium Speed)", "LNG(Otto Slow Speed)", "LNG(Otto Diesel Speed)", "LPG(Butane)", "LPG(Propane)", "H2(Natural gas)", "NH3(Natural gas)", "Methanol(Natural gas)", "NH3(e-fuel)"]
    fuel_oil_type_info_list = []

    for fuel_oil_name in fuel_oil_name_list:
        fuel_oil_type_info_list.append(select.get_fuel_oil_type(fuel_oil_name)[0])

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
            fuel_oil_info_list["LPG_Propane_info_list"] = fuel_oil_type_info
        elif name == "H2(Natural gas)":
            fuel_oil_info_list["H2_Ng_info_list"] = fuel_oil_type_info
        elif name == "NH3(Natural gas)":
            fuel_oil_info_list["NH3_Ng_info_list"] = fuel_oil_type_info
        elif name == "Methanol(Natural gas)":
            fuel_oil_info_list["Methanol_Ng_info_list"] = fuel_oil_type_info
        elif name == "NH3(e-fuel)":
            fuel_oil_info_list["NH3_eFuel_info_list"] = fuel_oil_type_info

    return fuel_oil_info_list, fuel_oil_name_list

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
        co2_total += lng_oss * lng_oss_co2_factor
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
        co2_total += methanol_ng * methanol_ng_co2_factor
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
        lng_oss_lcv =  float(fuel_oil_type_list["LNG_OSS_info_list"]["lcv"]["S"])
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
        nh3_ng_lcv = float(fuel_oil_type_list["NH3_Ng_info_list"]["lcv"]["S"])
        total_energy += nh3_ng * nh3_ng_lcv
    if nh3_ef > 0:
        nh3_ef_lcv = float(fuel_oil_type_list["NH3_eFuel_info_list"]["lcv"]["S"])
        total_energy += nh3_ef * nh3_ef_lcv
    if methanol_ng > 0:
        methanol_ng_lcv = float(fuel_oil_type_list["Methanol_Ng_info_list"]["lcv"]["S"])
        total_energy += methanol_ng * methanol_ng_lcv
    if h2_ng > 0:
        h2_ng_lcv = float(fuel_oil_type_list["H2_Ng_info_list"]["lcv"]["S"])
        total_energy += h2_ng * h2_ng_lcv

    return_energy = total_energy

    return return_energy



# 該当年のGHG強度上限値を算出するメソッド
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
        nh3_ng_ghg_intensity =  float(fuel_oil_type_list["NH3_Ng_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ng * nh3_ng_ghg_intensity
        sum_foc += nh3_ng
    if nh3_ef > 0:
        nh3_ef_ghg_intensity =  float(fuel_oil_type_list["NH3_eFuel_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ef * nh3_ef_ghg_intensity
        sum_foc += nh3_ef
    if methanol_ng > 0:
        methanol_ng_ghg_intensity =  float(fuel_oil_type_list["Methanol_Ng_info_list"]["ghg_intensity"]["S"])
        sum_ghg += methanol_ng * methanol_ng_ghg_intensity
        sum_foc += methanol_ng
    if h2_ng > 0:
        h2_ng_ghg_intensity =  float(fuel_oil_type_list["H2_Ng_info_list"]["ghg_intensity"]["S"])
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

# FuelListをリストに変換
def convertFuelOileStringToList(text):
    
    pattern = r'\([^()]*\([^()]*\)[^()]*\)|\([^()]*\)'
    matches = re.findall(pattern, text)

    # 前後の括弧を除去
    cleaned_matches = [match[1:-1] for match in matches]

    return cleaned_matches

# 最大桁の値以外を０に変換する（例：43⇒40、549⇒500、0.345→0.3）
def maxDigitOnly(n):
    # 数値を文字列に変換
    print(f"maxDigitOnly input:{(n)}")
    str_n = str(n)
    # ゼロ以外の最初の桁を取得
    count  = 0
    degits = ""
    for i in range(len(str_n)):
        count += 1
        degits = degits + str_n[i]
        if str_n[i] != "0" and str_n[i] != ".":
            break
    # 整数部分がある場合、整数部分の残りを0で埋める
    if n >= 1:
        result = degits + '0' * (len(str(round(n))) - count)
    else:
        result = degits
    print(f"maxDigitOnly result:{(result)}")
    # 数値型に変換して返す
    return float(result)

def lambda_handler(event, context):

    print(f"event{type(event)}: {event}")
    
    pathParameters = event['pathParameters']['proxy'].split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']

    # imo取得
    imo = queryStringParameters['imo']
    print(f"imo{type(imo)}: {imo}")

    # plan取得
    user_id = queryStringParameters['user']
    plan = queryStringParameters['plan']

    # 認可：IMO参照権限チェック
    authCheck = auth.imo_check(token, imo)
    if authCheck == 401 or authCheck == 500:
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },  
            'body': authCheck
        }

    # 返却用データセット
    VESSELMASTER = []
    FOCFormulas  = []
    FuelOilList  = []
    VesselList   = []
    SimulationInformationVoyageList = []
    SimulationInformationSpeedList  = []
    EUAList_YeartoDate = []
    CBList_YeartoDate  = []
    EUAList_Simulation = []
    CBList_Simulation  = []
    SimulationResultUnit = []

    # 各種合計値用変数のセット
    total_lng_oms     = 0
    total_lng_oss     = 0
    total_lng_ods     = 0
    total_hfo         = 0
    total_lfo         = 0
    total_mdo         = 0
    total_mgo         = 0
    total_lpg_p       = 0
    total_lpg_b       = 0
    total_h2_ng       = 0
    total_nh3_ng      = 0
    total_methanol_ng = 0
    total_nh3_ef      = 0
    total_foc         = 0
    total_eu_actual_foc = 0
    total_co2         = 0
    total_distance    = 0
    total_eua         = 0
    total_energy      = 0

    # Y軸設定用の変数定義
    max_eua = 0
    min_cb = 0
    max_cb = 0
    eua_tickInterval = 0
    cb_tickInterval = 0

    year_to_leg_cb    = 0

    company_id = ""
    eua_price  = 0
    SimulationInformation_VoyageList = []

    # VesselListの作成
    res_user   = select.get_user(user_id)
    company_id = res_user[0]["company_id"]["S"]
    res_group  = select.get_group(company_id, "admin")
    
    imo_list = ast.literal_eval(res_group[0]["imo_list"]["S"])
    print(f"imo_list{type(imo_list)}: {imo_list}")

    for item in imo_list:
        # Vessel Nameを取得する
        res_vesselmaster = select.get_vessel_master(item)

        vessel_name = res_vesselmaster[0]["VesselName"]["S"]
        vessel_data = {
            "imoNo"     : item,
            "VesselName": vessel_name
        }
        VesselList.append(vessel_data)

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    str_now = dt_now.strftime('%Y/%m/%d %H:%M')
    # str_now = dt_now.strftime('%Y-%m-%dT%H:%M')

    # VesselMaster取得
    res_vesselmaster = select.get_vessel_master(imo)
    VESSELMASTER     = Util.util_vesselmaster(res_vesselmaster)

    # FOCFormulas取得
    foc_formulas = {
        "ME_Ballast": [],
        "ME_Laden":  [],
        "AuxiliryEquipment": "",
    }

    res_foc_formulas = select.get_foc_formulas(imo)
    if res_foc_formulas:
        # Respons値用に整形
        foc_formulas["ME_Ballast"] = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
        foc_formulas["ME_Laden"] = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
        foc_formulas["AuxiliryEquipment"] = ast.literal_eval(res_foc_formulas[0]["auxiliary_equipment"]["S"])

    # Fuel-Oil-Typeリストを取得する
    fuel_oil_type_info_list, FuelOilList = make_fuel_oil_type_info_list()

    # plan項目で指定されたシミュレーションテーブルを取得
    if plan == "Voyage":
        # eco-eu-simulation-cond-voyage-plan取得
        res_simulation = select.get_simulation_voyage(imo, now_year)
    else:
        # eco-eu-simulation-cond-speed-plan取得
        res_simulation = select.get_simulation_speed(imo, now_year)
    
    # simulationレコードが取得できた場合、1レコード目のflagを確認し、シミュレーション計算の要否を確認
    calc_total_flg = "0"
    flag = "0"
    if res_simulation:
        flag = res_simulation[0]["flag"]["S"] if "flag" in res_simulation[0] else "0"

        if flag == "1":
            calc_total_flg = "1"

    this_year_leg_count = 0

    # EUA単価を取得
    eua_price  = float(res_group[0]["eua_price"]["S"]) if "eua_price" in res_group[0] else 0

    # 実績のleg-totalテーブルから、該当船舶の今年の航海実績を取得する。
    this_year_leg_list = select.get_leg_total(imo, now_year)

    # EU Rateがゼロのレコードを除いたリストを作成する
    res_leg = []
    for leg_info in this_year_leg_list:
        leg_eu_rate = leg_info["eu_rate"]["S"]
        if leg_eu_rate != "0":
            res_leg.append(leg_info)
    print(f"len(res_leg): {len(res_leg)}")
    this_year_leg_list = res_leg

    this_year_leg_count = len(this_year_leg_list)
    
    # ---------- this_year_leg_listループ開始 ----------
    if calc_total_flg == "1":
        for i in range(len(this_year_leg_list)):

            # 1leg当たりの数値を取得する
            eu_rate      = int(this_year_leg_list[i]["eu_rate"]["S"])
            leg_lng      = float(this_year_leg_list[i]["total_lng"]["S"])
            leg_hfo      = float(this_year_leg_list[i]["total_hfo"]["S"])
            leg_lfo      = float(this_year_leg_list[i]["total_lfo"]["S"])
            leg_mdo      = float(this_year_leg_list[i]["total_mdo"]["S"])
            leg_mgo      = float(this_year_leg_list[i]["total_mgo"]["S"])
            leg_foc      = float(this_year_leg_list[i]["total_foc"]["S"])
            leg_distance = float(this_year_leg_list[i]["distance"]["S"])
            leg_eua      = float(this_year_leg_list[i]["eua"]["S"])
            leg_cb       = float(this_year_leg_list[i]["cb"]["S"])

            # EUAList_YeartoDateにEUAをセット
            EUAList_YeartoDate.append([i + 1, leg_eua])

            # このlegで排出したco2量を算出
            leg_co2 = calc_co2(now_year, 0, leg_lng, 0, leg_hfo, leg_lfo, leg_mdo, leg_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
            # このlegのGHG強度、CBコストを算出する
            leg_GHG = calc_GHG_Actual(0, leg_lng, 0, leg_hfo, leg_lfo, leg_mdo, leg_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
            leg_cb_cost = 0
            if leg_cb < 0:
                leg_cb_cost = abs(float(leg_cb)) * 2400 / (leg_GHG * 41000)

            # 合計用変数に加算する
            total_lng_oms  += leg_lng
            total_hfo      += leg_hfo
            total_lfo      += leg_lfo
            total_mdo      += leg_mdo
            total_mgo      += leg_mgo
            total_foc      += (leg_lng + leg_hfo + leg_lfo + leg_mdo + leg_mgo)
            total_eu_actual_foc += leg_foc / (eu_rate / 100)
            total_co2      += leg_co2
            total_distance += leg_distance
            total_eua      += leg_eua
            print(f"total_lng_oms:{total_lng_oms}, total_hfo:{total_hfo}, total_lfo:{total_lfo}, total_mdo:{total_mdo}, total_mgo:{total_mgo}")

            # CBを算出する
            to_thisLeg_energy = calc_energy(0, total_lng_oms, 0, total_hfo, total_lfo, total_mdo, total_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
            total_energy      = to_thisLeg_energy
            to_thisLeg_GHG    = calc_GHG_Actual(0, total_lng_oms, 0, total_hfo, total_lfo, total_mdo, total_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
            to_thisLeg_cb     = calc_cb(now_year, to_thisLeg_energy, to_thisLeg_GHG)
            print(f"to_thisLeg_energy:{to_thisLeg_energy}, to_thisLeg_GHG:{to_thisLeg_GHG}, to_thisLeg_cb:{to_thisLeg_cb}")

            # CBList_YeartoDateにEUAをセット
            CBList_YeartoDate.append([i + 1, float(to_thisLeg_cb) / 1000000])

            # Y軸設定用の変数に値を設定
            max_eua = leg_eua if max_eua < leg_eua else max_eua
            max_cb = float(to_thisLeg_cb) if max_cb < float(to_thisLeg_cb) else max_cb
            min_cb = float(to_thisLeg_cb) if min_cb > float(to_thisLeg_cb) else min_cb

            # 最後のleg分だけ、CBのシミュレーション用データセットに追加（折れ線ブラフ描画のため）
            if i == len(this_year_leg_list) - 1:
                CBList_Simulation.append([i + 1, float(to_thisLeg_cb)])

            # 各レグをクリックした時のSimulation Result用Listに追加
            simulation_result_data = {
                "distance": str(round(leg_distance)),
                "foc"     : str(round(leg_foc / (eu_rate / 100), 1)),
                "ghg"     : str(round(leg_co2)),
                "eua"     : str(round(leg_eua, 1)),
                "eua_cost": str(round(leg_eua * eua_price)),
                "cb"      : str(round(leg_cb / 1000000, 1)),
                "cb_cost" : str(round(leg_cb_cost))
            }
            SimulationResultUnit.append(simulation_result_data)

        # ---------- this_year_leg_listループ終了 ---------

    # 実測データにEU対象のものがなかった場合（CBList_Simulationが空）のフラグを設定する
    ytd_eu_exist_flag = "1"
    if len(CBList_Simulation) == 0:
        ytd_eu_exist_flag = "0"

    # 表示画面がVoyage Planの場合
    if plan == "Voyage":
        # ---------- res_simulationループ開始 ---------
        for i in range(len(res_simulation)):

            # 変数の設定
            leg_rate                = 0
            leg_total_time          = 0
            simulation_leg_lng      = 0
            simulation_leg_hfo      = 0
            simulation_leg_lfo      = 0
            simulation_leg_mdo      = 0
            simulation_leg_mgo      = 0
            simulation_leg_lpg_p    = 0
            simulation_leg_ethanol  = 0
            simulation_leg_lpg_b    = 0
            simulation_leg_methanol = 0
            simulation_foc_per_day  = 0
            return_departure_time   = ""
            return_arrival_time     = ""
            return_leg_total_time   = 0

            # Leg No 取得
            tmp_text = res_simulation[i]["year_and_serial_number"]["S"]
            leg_no = int(tmp_text.split("E")[1])  # 'E' 以降を抽出
            
            # legの開始・終了時刻を取得する
            str_departure_time = res_simulation[i]["departure_time"]["S"]     # "2024-12-10 12:30"形式
            str_arrival_time   = res_simulation[i]["arrival_time"]["S"]       # "2024-12-19 17:30"形式

            # legの開始・終了時刻からlegの時間を算出する
            dt_departure_time = Util.to_datetime(str_departure_time)
            # test_departure_time = dt_departure_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            dt_arrival_time = Util.to_datetime(str_arrival_time)
            # test_arrival_time = dt_arrival_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            # print(f"departure_time: {(test_departure_time)}, arrival_time: {(test_arrival_time)}")     
            leg_total_time = Util.calc_time_diff(dt_departure_time, dt_arrival_time)

            # 各legの期間から、反映割合を算出する
            # リスト項目の時刻はlocal時刻。UTCと比較してもJTCと比較しても多少ズレる
            print(f"str_now:{str_now}")
            print(f"str_departure_time:{str_departure_time}")
            print(f"dt_departure_time:{dt_departure_time}")
            leg_part_time = leg_total_time
            if str_now <= str_departure_time:
                print(f"departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは完全に先時刻")
                return_departure_time = str_departure_time
                return_arrival_time   = str_arrival_time
                return_leg_total_time = leg_total_time

            elif str_now <= str_arrival_time:
                print(f"departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは現在進行中")
                # 表示する範囲の時間を算出し、leg全体に対する割合を求める。
                dt_time_from  = Util.to_datetime(str_now)
                dt_time_to    = Util.to_datetime(str_arrival_time)
                leg_part_time = Util.calc_time_diff(dt_time_from, dt_time_to)

                return_departure_time = str_now
                return_arrival_time   = str_arrival_time
                return_leg_total_time = leg_part_time

            else:
                print(f"departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは完結済")
                # 以降の処理は行わず、次のlegを確認
                continue

            leg_rate = 0
            if leg_total_time != 0:
                leg_rate = float(leg_part_time / leg_total_time)
            print(f"leg_rate:{(leg_rate)} leg_part_time:{(leg_part_time)} leg_total_time:{(leg_total_time)}")


            # 各項目を取得し、必要項目にはleg_rateを反映する
            displacement           = res_simulation[i]["dispracement"]["S"]
            leg_distance           = float(res_simulation[i]["distance"]["S"]) * leg_rate
            print(f"res_simulation[{(i)}][distance][S]:{(res_simulation[i]["distance"]["S"])} leg_rate:{(leg_rate)} leg_distance:{(leg_distance)}")
            simulation_leg_eu_rate = int(res_simulation[i]["eu_rate"]["S"])

            print(f"leg_distance:{leg_distance}")

            # log_speedを算出
            leg_log_speed = 0
            if return_leg_total_time != 0:
                leg_log_speed = leg_distance / return_leg_total_time
            print(f"return_leg_total_time:{return_leg_total_time}")
            print(f"leg_log_speed:{leg_log_speed}")


            # FOC Formulasがある場合
            if res_foc_formulas:

                # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
                auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])
                print(f"auxiliary_equipment: {(auxiliary_equipment)}")

                # Ballast、Ladenどちらか判断して、FOCを算出
                if displacement == "Ballast":
                    # Ballast用の計算パラメータを取得し、1日当たりのFOCを算出
                    calc_balast_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
                    ballast_alpha = calc_balast_param_list[0]
                    ballast_a = calc_balast_param_list[1]
                    ballast_c = calc_balast_param_list[2]
                    simulation_foc_per_day = ballast_alpha * leg_log_speed ** ballast_a + ballast_c + auxiliary_equipment
                else:
                    # Laden用の計算パラメータを取得し、1日当たりのFOCを算出
                    calc_laden_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
                    laden_alpha = calc_laden_param_list[0]
                    laden_a = calc_laden_param_list[1]
                    laden_c = calc_laden_param_list[2]
                    simulation_foc_per_day = laden_alpha * leg_log_speed ** laden_a + laden_c + auxiliary_equipment

                # 1時間あたりのFOC算出
                simulation_foc_per_hour = simulation_foc_per_day / 24
                # Leg内総FOCを算出
                leg_total_actual_foc = simulation_foc_per_hour * return_leg_total_time
                simulation_leg_foc   = leg_total_actual_foc * simulation_leg_eu_rate / 100

                # 燃料別消費量を算出する
                output_fuel_list = []
                fuel_list = convertFuelOileStringToList(res_simulation[i]["fuel"]["S"]) 

                # EU Rateを考慮しない燃料消費量
                simulation_leg_actual_lng_oms = 0
                simulation_leg_actual_lng_oss = 0
                simulation_leg_actual_lng_ods = 0
                simulation_leg_actual_hfo = 0
                simulation_leg_actual_lfo = 0
                simulation_leg_actual_mdo = 0
                simulation_leg_actual_mgo = 0
                simulation_leg_actual_lpg_p = 0
                simulation_leg_actual_lpg_b = 0
                simulation_leg_actual_h2_ng = 0
                simulation_leg_actual_nh3_ng = 0
                simulation_leg_actual_methanol_ng = 0
                simulation_leg_actual_nh3_ef = 0

                # EU Rate考慮済の燃料消費量
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

                for fuel in fuel_list:
                    fuel_info_list = fuel.split(',')
                    fuel_type = fuel_info_list[0]
                    fuel_rate = int(fuel_info_list[1])

                    if  fuel_type == "LNG(Otto Medium Speed)":
                        simulation_leg_actual_lng_oms = leg_total_actual_foc * int(fuel_rate) / 100
                    elif  fuel_type == "LNG(Otto Slow Speed)":
                        simulation_leg_actual_lng_oss = leg_total_actual_foc * int(fuel_rate) / 100
                    elif  fuel_type == "LNG(Otto Diesel Speed)":
                        simulation_leg_actual_lng_ods = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "HFO":
                        simulation_leg_actual_hfo = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "LFO":
                        simulation_leg_actual_lfo = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "MDO":
                        simulation_leg_actual_mdo = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "MGO":
                        simulation_leg_actual_mgo = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "LPG(Propane)":
                        simulation_leg_actual_lpg_p = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "LPG(Butane)":
                        simulation_leg_actual_lpg_b = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "H2(Natural gas)":
                        simulation_leg_actual_h2_ng = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "NH3(Natural gas)":
                        simulation_leg_actual_nh3_ng = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "Methanol(Natural gas)":
                        simulation_leg_actual_methanol_ng = leg_total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "NH3(e-fuel)":
                        simulation_leg_actual_nh3_ef = leg_total_actual_foc * int(fuel_rate) / 100

                    # 表示用fuel_listに追加
                    output_fuel = {
                        "fuel_type" : fuel_type,
                        "fuel_rate" : fuel_rate,
                    }
                    output_fuel_list.append(output_fuel)

                if simulation_leg_eu_rate != 0:
                    simulation_leg_lng_oms = simulation_leg_actual_lng_oms * simulation_leg_eu_rate / 100
                    simulation_leg_lng_oss = simulation_leg_actual_lng_oss * simulation_leg_eu_rate / 100
                    simulation_leg_lng_ods = simulation_leg_actual_lng_ods * simulation_leg_eu_rate / 100
                    simulation_leg_hfo     = simulation_leg_actual_hfo * simulation_leg_eu_rate / 100
                    simulation_leg_lfo     = simulation_leg_actual_lfo * simulation_leg_eu_rate / 100
                    simulation_leg_mdo     = simulation_leg_actual_mdo * simulation_leg_eu_rate / 100
                    simulation_leg_mgo     = simulation_leg_actual_mgo * simulation_leg_eu_rate / 100
                    simulation_leg_lpg_p   = simulation_leg_actual_lpg_p * simulation_leg_eu_rate / 100
                    simulation_leg_lpg_b   = simulation_leg_actual_lpg_b * simulation_leg_eu_rate / 100
                    simulation_leg_h2_ng   = simulation_leg_actual_h2_ng * simulation_leg_eu_rate / 100
                    simulation_leg_nh3_ng  = simulation_leg_actual_nh3_ng * simulation_leg_eu_rate / 100
                    simulation_leg_methanol_ng = simulation_leg_actual_methanol_ng * simulation_leg_eu_rate / 100
                    simulation_leg_nh3_ef  = simulation_leg_actual_nh3_ef * simulation_leg_eu_rate / 100

                    total_lng_oms     += simulation_leg_lng_oms
                    total_lng_oss     += simulation_leg_lng_oss
                    total_lng_ods     += simulation_leg_lng_ods
                    total_hfo         += simulation_leg_hfo
                    total_lfo         += simulation_leg_lfo
                    total_mdo         += simulation_leg_mdo
                    total_mgo         += simulation_leg_mgo
                    total_lpg_p       += simulation_leg_lpg_p
                    total_lpg_b       += simulation_leg_lpg_b
                    total_h2_ng       += simulation_leg_h2_ng
                    total_nh3_ng      += simulation_leg_nh3_ng
                    total_methanol_ng += simulation_leg_actual_methanol_ng
                    total_nh3_ef      += simulation_leg_nh3_ef  

                simulation_leg_co2 = 0
                simulation_leg_eua = 0
                simulation_leg_cb  = 0
                simulation_leg_cb_cost = 0

                # シミュレーション部分で実際に排出したco2を算出する
                simulation_leg_actual_co2 = calc_co2(now_year, simulation_leg_actual_lng_ods, simulation_leg_actual_lng_oms, simulation_leg_actual_lng_oss, simulation_leg_actual_hfo, simulation_leg_actual_lfo, simulation_leg_actual_mdo, simulation_leg_actual_mgo, simulation_leg_actual_lpg_p, simulation_leg_actual_lpg_b, simulation_leg_actual_nh3_ng, simulation_leg_actual_nh3_ef, simulation_leg_actual_methanol_ng, simulation_leg_actual_h2_ng, fuel_oil_type_info_list)

                if simulation_leg_eu_rate != 0:
                    # シミュレーション部分で排出したEU Rate考慮済のco2を算出する
                    simulation_leg_co2 = simulation_leg_actual_co2 * simulation_leg_eu_rate / 100

                    # シミュレーション部分のEUAを算出する
                    simulation_leg_eua = calc_eua(now_year, simulation_leg_co2)
                    # EUA Costの算出
                    # total_eua_cost = round(simulation_leg_eua, 1) * eua_price

                    # シミュレーション部分のCBを算出する
                    simulation_leg_GHG = calc_GHG_Actual(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
                    simulation_energy  = calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
                    total_energy      += simulation_energy
                    simulation_leg_cb  = calc_cb(now_year, simulation_energy, simulation_leg_GHG)
                    # このlegのCBコストを算出する
                    if simulation_leg_cb < 0:
                        simulation_leg_cb_cost = abs(float(simulation_leg_cb)) * 2400 / (simulation_leg_GHG * 41000)

                # シミュレーション計算が必要な場合
                if calc_total_flg == "1":

                    # EUAList_YeartoDateにEUAをセット
                    EUAList_Simulation.append([this_year_leg_count + i + 1, simulation_leg_eua])

                    # 実測データを足したこのlegまでのCBを算出する
                    year_to_leg_GHG = calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_info_list)
                    year_to_leg_energy  = total_energy
                    year_to_leg_cb  = calc_cb(now_year, year_to_leg_energy, year_to_leg_GHG)
                    # CBList_YeartoDateに通年CBをセット
                    CBList_Simulation.append([this_year_leg_count + i + 1, float(year_to_leg_cb) / 1000000])

                    # 合計用変数に加算する
                    total_distance += leg_distance
                    total_foc      += (simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_lpg_b + simulation_leg_nh3_ng + simulation_leg_nh3_ef + simulation_leg_methanol_ng + simulation_leg_h2_ng)
                    total_eu_actual_foc += leg_total_actual_foc
                    total_co2      += simulation_leg_actual_co2
                    total_eua      += simulation_leg_eua
                    # total_cb        = float(year_to_leg_cb) # 最終的な値を保持したいため、足さない。

                    # Y軸設定用の変数に値を設定
                    max_eua = simulation_leg_eua if max_eua < simulation_leg_eua else max_eua
                    max_cb = float(year_to_leg_cb) if max_cb < float(year_to_leg_cb) else max_cb
                    min_cb = float(year_to_leg_cb) if min_cb > float(year_to_leg_cb) else min_cb

                    # 各レグをクリックした時のSimulation Result用Listに追加
                    simulation_result_data = {
                        "distance": str(round(leg_distance)),
                        "foc"     : str(round(leg_total_actual_foc, 1)),
                        "ghg"     : str(round(simulation_leg_actual_co2)),
                        "eua"     : str(round(simulation_leg_eua, 1)),
                        "eua_cost": str(round(simulation_leg_eua * eua_price)),
                        "cb"      : str(round(simulation_leg_cb / 1000000, 1)),
                        "cb_cost" : str(round(simulation_leg_cb_cost))
                    }
                    SimulationResultUnit.append(simulation_result_data)

                # Voyage Planのシミュレーション用データ
                str_eua = str(round(simulation_leg_eua, 1))
                str_cb  = str(round(float(simulation_leg_cb) / 1000000, 1))

                simulation_data = {
                    "leg_no"         : leg_no,
                    "departure_port" : res_simulation[i]["departure_port"]["S"], 
                    "departure_time" : return_departure_time,
                    "arrival_port"   : res_simulation[i]["arrival_port"]["S"], 
                    "arrival_time"   : return_arrival_time,
                    "total_time"     : str(return_leg_total_time),
                    "eu_rate"        : str(simulation_leg_eu_rate),
                    "displacement"   : res_simulation[i]["dispracement"]["S"],
                    "operator"       : res_simulation[i]["operator"]["S"],
                    "distance"       : str(round(leg_distance)),
                    "log_speed"      : str(round(leg_log_speed, 1)),
                    "fuel"           : output_fuel_list,
                    "foc"            : str(round(leg_total_actual_foc, 1)),
                    "eua"            : str_eua,
                    "cb"             : str_cb
                }
                SimulationInformation_VoyageList.append(simulation_data)

            # FOC Formulasが無い場合
            else:
                # 燃料リストを整形する
                output_fuel_list = []
                fuel_list = convertFuelOileStringToList(res_simulation[i]["fuel"]["S"]) 

                for fuel in fuel_list:
                    fuel_info_list = fuel.split(',')
                    fuel_type = fuel_info_list[0]
                    fuel_rate = int(fuel_info_list[1])

                    # 表示用fuel_listに追加
                    output_fuel = {
                        "fuel_type" : fuel_type,
                        "fuel_rate" : fuel_rate,
                    }
                    output_fuel_list.append(output_fuel)

                simulation_data = {
                    "leg_no"         : leg_no,
                    "departure_port" : res_simulation[i]["departure_port"]["S"], 
                    "departure_time" : return_departure_time,
                    "arrival_port"   : res_simulation[i]["arrival_port"]["S"], 
                    "arrival_time"   : return_arrival_time,
                    "total_time"     : str(return_leg_total_time),
                    "eu_rate"        : str(simulation_leg_eu_rate),
                    "displacement"   : res_simulation[i]["displacement"]["S"],
                    "operator"       : res_simulation[i]["operator"]["S"],
                    "distance"       : str(round(leg_distance)),
                    "log_speed"      : str(round(leg_log_speed, 1)),
                    "fuel"           : output_fuel_list,
                    "foc"            : "",
                    "eua"            : "",
                    "cb"             : ""
                }
                SimulationInformation_VoyageList.append(simulation_data)

        # ---------- res_simulationループ終了 ---------

        # departure timeでソート
        SimulationInformation_VoyageList = sorted(SimulationInformation_VoyageList, key=lambda x:x["leg_no"])

        # 通番を設定する
        num = 0
        for data in SimulationInformation_VoyageList:
            
            print(f"data[distance]:{data["distance"]}")


            num += 1
            list_data = {
                "leg_no"         : "E" + str(num),
                "departure_port" : data["departure_port"], 
                "departure_time" : data["departure_time"],
                "arrival_port"   : data["arrival_port"],
                "arrival_time"   : data["arrival_time"],
                "total_time"     : data["total_time"],
                "eu_rate"        : data["eu_rate"],
                "displacement"   : data["displacement"],
                "operator"       : data["operator"],
                "distance"       : data["distance"],
                "log_speed"      : data["log_speed"],
                "fuel"           : data["fuel"],
                "foc"            : data["foc"],
                "eua"            : data["eua"],
                "cb"             : data["cb"],
            }    
            SimulationInformationVoyageList.append(list_data)
    
    # 表示画面がSpeed Planの場合
    else:
        # シミュレーション用のSpeed Planがある かつ シミュレーション実施要の場合
        if res_simulation:

            leg_total_time          = 0
            simulation_leg_lng      = 0
            simulation_leg_hfo      = 0
            simulation_leg_lfo      = 0
            simulation_leg_mdo      = 0
            simulation_leg_mgo      = 0
            simulation_leg_lpg_p    = 0
            simulation_leg_ethanol  = 0
            simulation_leg_lpg_b    = 0
            simulation_leg_methanol = 0
        
            # SpeedPlanのSimulation処理実施   
            # Time to End of Year算出（年末 - 現在）
            year_end = datetime(dt_now.year, 12, 31, 23, 59, 59)
            time_to_end_of_year = Util.calc_time_diff(dt_now, year_end)

            # シミュレーション部分のEU Rateを取得
            simulation_leg_eu_rate = float(res_simulation[0]["eu_rate"]["S"])

            # 航海時間を算出
            sailing_rate = float(res_simulation[0]["salling_rate"]["S"])
            sailing_time = time_to_end_of_year * (sailing_rate / 100)
            port_time    = time_to_end_of_year - sailing_time
            print(f"sailing_time:{(sailing_time)} port_time:{(port_time)}")

            # Ballast、Ladenそれぞれの航海距離を算出
            displacement_rate    = float(res_simulation[0]["dispracement_rate"]["S"])
            ballast_sailing_time = sailing_time * (displacement_rate / 100)
            laden_sailing_time   = sailing_time - ballast_sailing_time

            # 時間×速さで距離を算出
            ballast_logspeed = float(res_simulation[0]["log_speed_ballast"]["S"])
            laden_logspeed   = float(res_simulation[0]["log_speed_laden"]["S"])
            ballast_ditance  = ballast_sailing_time * ballast_logspeed
            laden_ditance    = laden_sailing_time * laden_logspeed

            # BallastDisancen、LadenDistanceを加算
            total_ballast_laden_distance = ballast_ditance + laden_ditance

            if res_foc_formulas:

                # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
                auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])
                print(f"auxiliary_equipment: {(auxiliary_equipment)}")

                # Ballast用の計算パラメータを取得し、1日当たりのFOCを算出
                calc_balast_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
                ballast_alpha = calc_balast_param_list[0]
                ballast_a = calc_balast_param_list[1]
                ballast_c = calc_balast_param_list[2]
                ballast_foc_per_day = ballast_alpha * ballast_logspeed ** ballast_a + ballast_c + auxiliary_equipment
                # Laden用の計算パラメータを取得し、1日当たりのFOCを算出
                calc_laden_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
                laden_alpha = calc_laden_param_list[0]
                laden_a = calc_laden_param_list[1]
                laden_c = calc_laden_param_list[2]
                laden_foc_per_day = laden_alpha * laden_logspeed ** laden_a + laden_c + auxiliary_equipment

                # 1時間あたりのFOC算出
                ballast_foc_per_hour = ballast_foc_per_day / 24
                laden_foc_per_hour = laden_foc_per_day / 24
                # FOC算出
                ballast_foc = ballast_foc_per_hour * ballast_sailing_time
                laden_foc = laden_foc_per_hour * ballast_sailing_time
                # 航海Leg内総FOCを算出
                leg_total_actual_foc = ballast_foc + laden_foc
                leg_total_FOC_speed  = leg_total_actual_foc * simulation_leg_eu_rate / 100
                # 停泊中の総FOCを算出
                port_total_actual_foc = auxiliary_equipment / 24 * port_time
                port_total_FOC_speed  = port_total_actual_foc * simulation_leg_eu_rate / 100
                # 総FOCを算出
                total_actual_foc = leg_total_actual_foc + port_total_actual_foc
                total_FOC_speed  = leg_total_FOC_speed + port_total_FOC_speed
                
                # 燃料別消費量を算出する
                output_fuel_list = []
                fuel_list = convertFuelOileStringToList(res_simulation[0]["fuel"]["S"])

                # EU Rateを考慮しない燃料消費量
                simulation_leg_actual_lng_oms = 0
                simulation_leg_actual_lng_oss = 0
                simulation_leg_actual_lng_ods = 0
                simulation_leg_actual_hfo = 0
                simulation_leg_actual_lfo = 0
                simulation_leg_actual_mdo = 0
                simulation_leg_actual_mgo = 0
                simulation_leg_actual_lpg_p = 0
                simulation_leg_actual_lpg_b = 0
                simulation_leg_actual_h2_ng = 0
                simulation_leg_actual_nh3_ng = 0
                simulation_leg_actual_methanol_ng = 0
                simulation_leg_actual_nh3_ef = 0

                # EU Rate考慮済の燃料消費量
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

                for fuel in fuel_list:
                    fuel_info_list = fuel.split(',')
                    fuel_type = fuel_info_list[0]
                    fuel_rate = fuel_info_list[1]

                    if  fuel_type == "LNG(Otto Medium Speed)":
                        simulation_leg_actual_lng_oms = total_actual_foc * int(fuel_rate) / 100
                    elif  fuel_type == "LNG(Otto Slow Speed)":
                        simulation_leg_actual_lng_oss = total_actual_foc * int(fuel_rate) / 100
                    elif  fuel_type == "LNG(Otto Diesel Speed)":
                        simulation_leg_actual_lng_ods = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "HFO":
                        simulation_leg_actual_hfo = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "LFO":
                        simulation_leg_actual_lfo = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "MDO":
                        simulation_leg_actual_mdo = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "MGO":
                        simulation_leg_actual_mgo = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "LPG(Propane)":
                        simulation_leg_actual_lpg_p = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "LPG(Butane)":
                        simulation_leg_actual_lpg_b = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "H2(Natural gas)":
                        simulation_leg_actual_h2_ng = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "NH3(Natural gas)":
                        simulation_leg_actual_nh3_ng = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "Methanol(Natural gas)":
                        simulation_leg_actual_methanol_ng = total_actual_foc * int(fuel_rate) / 100
                    elif fuel_type == "NH3(e-fuel)":
                        simulation_leg_actual_nh3_ef = total_actual_foc * int(fuel_rate) / 100

                    # 表示用fuel_listに追加
                    output_fuel = {
                        "fuel_type" : fuel_type,
                        "fuel_rate" : fuel_rate,
                    }
                    output_fuel_list.append(output_fuel)

                if simulation_leg_eu_rate != 0:
                    simulation_leg_lng_oms = simulation_leg_actual_lng_oms * simulation_leg_eu_rate / 100
                    simulation_leg_lng_oss = simulation_leg_actual_lng_oss * simulation_leg_eu_rate / 100
                    simulation_leg_lng_ods = simulation_leg_actual_lng_ods * simulation_leg_eu_rate / 100
                    simulation_leg_hfo     = simulation_leg_actual_hfo * simulation_leg_eu_rate / 100
                    simulation_leg_lfo     = simulation_leg_actual_lfo * simulation_leg_eu_rate / 100
                    simulation_leg_mdo     = simulation_leg_actual_mdo * simulation_leg_eu_rate / 100
                    simulation_leg_mgo     = simulation_leg_actual_mgo * simulation_leg_eu_rate / 100
                    simulation_leg_lpg_p   = simulation_leg_actual_lpg_p * simulation_leg_eu_rate / 100
                    simulation_leg_lpg_b   = simulation_leg_actual_lpg_b * simulation_leg_eu_rate / 100
                    simulation_leg_h2_ng   = simulation_leg_actual_h2_ng * simulation_leg_eu_rate / 100
                    simulation_leg_nh3_ng  = simulation_leg_actual_nh3_ng * simulation_leg_eu_rate / 100
                    simulation_leg_methanol_ng = simulation_leg_actual_methanol_ng * simulation_leg_eu_rate / 100
                    simulation_leg_nh3_ef  = simulation_leg_actual_nh3_ef * simulation_leg_eu_rate / 100

                    total_lng_oms     += simulation_leg_lng_oms
                    total_lng_oss     += simulation_leg_lng_oss
                    total_lng_ods     += simulation_leg_lng_ods
                    total_hfo         += simulation_leg_hfo
                    total_lfo         += simulation_leg_lfo
                    total_mdo         += simulation_leg_mdo
                    total_mgo         += simulation_leg_mgo
                    total_lpg_p       += simulation_leg_lpg_p
                    total_lpg_b       += simulation_leg_lpg_b
                    total_h2_ng       += simulation_leg_h2_ng
                    total_nh3_ng      += simulation_leg_nh3_ng
                    total_methanol_ng += simulation_leg_actual_methanol_ng
                    total_nh3_ef      += simulation_leg_nh3_ef                            

                simulation_leg_eua = 0
                simulation_leg_eua = 0
                simulation_leg_cb  = 0
                simulation_leg_cb_cost = 0

                # シミュレーション部分で実際に排出したco2を算出する
                simulation_leg_actual_co2 = calc_co2(now_year, simulation_leg_actual_lng_ods, simulation_leg_actual_lng_oms, simulation_leg_actual_lng_oss, simulation_leg_actual_hfo, simulation_leg_actual_lfo, simulation_leg_actual_mdo, simulation_leg_actual_mgo, simulation_leg_actual_lpg_p, simulation_leg_actual_lpg_b, simulation_leg_actual_nh3_ng, simulation_leg_actual_nh3_ef, simulation_leg_actual_methanol_ng, simulation_leg_actual_h2_ng, fuel_oil_type_info_list)

                if simulation_leg_eu_rate != 0:
                    # シミュレーション部分で排出したEU Rate考慮済のco2を算出する
                    simulation_leg_co2 = simulation_leg_actual_co2 * simulation_leg_eu_rate / 100
                    # シミュレーション部分のEUAを算出する
                    simulation_leg_eua = calc_eua(now_year, simulation_leg_co2)

                    # シミュレーション部分のCBを算出する
                    simulation_leg_GHG = calc_GHG_Actual(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
                    simulation_energy  = calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
                    total_energy      += simulation_energy
                    simulation_leg_cb  = calc_cb(now_year, simulation_energy, simulation_leg_GHG)
                    # このlegのCBコストを算出する
                    if simulation_leg_cb < 0:
                        simulation_leg_cb_cost = abs(float(simulation_leg_cb)) * 2400 / (simulation_leg_GHG * 41000)

                # シミュレーション計算が必要な場合
                if calc_total_flg == "1":

                    # EUAList_YeartoDateにEUAをセット
                    EUAList_Simulation.append([this_year_leg_count + 1, simulation_leg_eua])

                    # 実測データを足したこのlegまでのCBを算出する
                    year_to_leg_GHG = calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_info_list)
                    year_to_leg_energy  = total_energy
                    year_to_leg_cb  = calc_cb(now_year, year_to_leg_energy, year_to_leg_GHG)
                    # CBList_Simulationに通年CBをセット
                    CBList_Simulation.append([this_year_leg_count + 1, float(year_to_leg_cb) / 1000000])

                    # 合計用変数に加算する
                    total_distance += total_ballast_laden_distance
                    total_foc      += (simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_lpg_b + simulation_leg_nh3_ng + simulation_leg_nh3_ef + simulation_leg_methanol_ng + simulation_leg_h2_ng)
                    total_eu_actual_foc += total_actual_foc
                    total_co2      += simulation_leg_actual_co2
                    total_eua      += simulation_leg_eua
                    # total_cb        = float(year_to_leg_cb) # 最終的な値を保持したいため、足さない。

                    # Y軸設定用の変数に値を設定
                    max_eua = simulation_leg_eua if max_eua < simulation_leg_eua else max_eua
                    max_cb = float(year_to_leg_cb) if max_cb < float(year_to_leg_cb) else max_cb
                    min_cb = float(year_to_leg_cb) if min_cb > float(year_to_leg_cb) else min_cb   

                    # 各レグをクリックした時のSimulation Result用Listに追加
                    simulation_result_data = {
                        "distance": str(round(total_ballast_laden_distance)),
                        "foc"     : str(round(total_actual_foc, 1)),
                        "ghg"     : str(round(simulation_leg_actual_co2)),
                        "eua"     : str(round(simulation_leg_eua, 1)),
                        "eua_cost": str(round(simulation_leg_eua * eua_price)),
                        "cb"      : str(round(simulation_leg_cb / 1000000, 1)),
                        "cb_cost" : str(round(simulation_leg_cb_cost))
                    }
                    SimulationResultUnit.append(simulation_result_data)             

                # Speed Planのシミュレーション用データ
                str_eua = str(round(simulation_leg_eua, 1))
                str_cb  = str(round(float(simulation_leg_cb) / 1000000, 1))

                simulation_data = {
                    "time_to_endofyear"            : str(round(time_to_end_of_year)),
                    "operator"                     : res_simulation[0]["operator"]["S"],
                    "servise_rate_salling"         : str(round(sailing_rate)),
                    "displacement_rate_ballast"    : str(round(displacement_rate)),
                    "avg_speed_ballast"            : res_simulation[0]["log_speed_ballast"]["S"],
                    "avg_speed_laden"              : res_simulation[0]["log_speed_laden"]["S"],
                    "fuel"                         : output_fuel_list,
                    "eu_rate"                      : str(round(simulation_leg_eu_rate)),
                    "distance"                     : str(round(total_ballast_laden_distance)),
                    "foc"                          : str(round(total_actual_foc)),
                    "eua"                          : str_eua,
                    "cb"                           : str_cb
                }
                SimulationInformationSpeedList.append(simulation_data)

            # FOC Formulasが無い場合
            else:
                # 燃料リストを整形する
                output_fuel_list = []
                fuel_list = convertFuelOileStringToList(res_simulation[0]["fuel"]["S"]) 

                for fuel in fuel_list:
                    fuel_info_list = fuel.split(',')
                    fuel_type = fuel_info_list[0]
                    fuel_rate = int(fuel_info_list[1])

                    # 表示用fuel_listに追加
                    output_fuel = {
                        "fuel_type" : fuel_type,
                        "fuel_rate" : fuel_rate,
                    }
                    output_fuel_list.append(output_fuel)

                simulation_data = {
                    "time_to_endofyear"         : str(round(time_to_end_of_year)),
                    "operator"                  : res_simulation[0]["operator"]["S"],
                    "servise_rate_salling"      : str(round(sailing_rate)),
                    "displacement_rate_ballast" : str(round(displacement_rate)),
                    "avg_speed_ballast"         : res_simulation[0]["log_speed_ballast"]["S"],
                    "avg_speed_laden"           : res_simulation[0]["log_speed_laden"]["S"],
                    "fuel"                      : output_fuel_list,
                    "eu_rate"                   : str(round(simulation_leg_eu_rate)),
                    "distance"                  : str(round(total_ballast_laden_distance)),
                    "foc"                       : "",
                    "eua"                       : "",
                    "cb"                        : ""
                }
                SimulationInformationSpeedList.append(simulation_data)

        # シミュレーション用のSpeed Planが無い場合
        else:
            # Time to End of Year算出（年末 - 現在）
            year_end = datetime(dt_now.year, 12, 31, 23, 59, 59)
            time_to_end_of_year = Util.calc_time_diff(dt_now, year_end)

            simulation_data = {
                "time_to_endofyear"         : str(round(time_to_end_of_year, 0)),
                "operator"                  : "",
                "servise_rate_salling"      : "",
                "displacement_rate_ballast" : "",
                "avg_speed_ballast"         : "",
                "avg_speed_laden"           : "",
                "fuel"                      : "",
                "eu_rate"                   : "",
                "distance"                  : "",
                "foc"                       : "",
                "eua"                       : "",
                "cb"                        : ""
            }
            SimulationInformationSpeedList.append(simulation_data)

    # 返却値を作成していく。

    # Total CBの算出
    total_GHG_Actual = calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_info_list)
    total_cb         = calc_cb(now_year, total_energy, total_GHG_Actual)

    # Total CB Costの算出
    total_cb_cost = 0
    if total_cb < 0:
        # ペナルティーファクターを調べる
        # 同一imoのyear-totalテーブルを取得（複数オペになったらどうする？）
        res_year_total_list    = select.get_year_total(imo)
        year_total_list_sorted = sorted(res_year_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 初期値設定
        consecutive_years = 0
        year_count = 0

        for year_total in year_total_list_sorted:
            # 先頭要素はスキップ（先頭要素は今年のレコード、前年以前を見たいため、スキップで良い）
            if year_count == 0:
                year_count += 1
                continue

            # １年ずつさかのぼる（年が飛んだ時点で確認不要のためbreak）
            if year_total["year_and_ope"]["S"][0:4] == str(int(now_year) - year_count):
                # 罰金フラグの確認
                fine_flag = year_total["fine_flag"]["S"]
                if fine_flag == "1":
                    consecutive_years += 1
                else:
                    break
            else:
                break

            year_count += 1

        penalty_factor = 1 + (consecutive_years) / 10
        if total_GHG_Actual != 0:
            total_cb_cost  = abs(float(total_cb)) * penalty_factor * 2400 / (total_GHG_Actual * 41000)

    # Simulation Resultの右側とX軸のラベル作成（Simulationの計算をしたときのみ設定）
    SimulationResultTotal = None
    XAxisList = []
    EUA_graph_data = []
    if calc_total_flg == "1":
        str_distance = str(round(total_distance)) if total_distance != "" else ""
        str_foc      = str(round(total_eu_actual_foc, 1))      if total_eu_actual_foc      != "" else ""
        str_co2      = str(round(total_co2))      if total_co2      != "" else ""
        str_eua      = str(round(total_eua, 1))      if total_eua      != "" else ""
        str_eua_cost = str(round(round(total_eua, 1) * eua_price)) if total_eua != "" else ""
        str_cb       = str(round(total_cb / 1000000, 1))
        str_cb_cost  = str(round(total_cb_cost))

        SimulationResultTotal = {
            "distance": str_distance,
            "foc"     : str_foc,
            "ghg"     : str_co2,
            "eua"     : str_eua,
            "eua_cost": str_eua_cost,
            "cb"      : str_cb,
            "cb_cost" : str_cb_cost
        }

        # X軸のラベル作成、グラフデータにも設定
        for i in range(len(EUAList_YeartoDate)):
            tmp_ytd_XAxis = "LEG " + str(i + 1)
            XAxisList.append(tmp_ytd_XAxis)

            # EUAのグラフの色を設定
            graph_data_eua = {
                "name": tmp_ytd_XAxis,
                "y": EUAList_YeartoDate[i][1],
                "color": {
                    "linearGradient": { "x1": 0, "x2": 0, "y1": 0, "y2": 1 },
                    "stops": [
                        [0, 'rgb(18, 87, 151, 1)'],
                        [1, 'rgb(19, 101, 255, 1)']
                    ]
                }
            }
            EUA_graph_data.append(graph_data_eua)

            graph_data_cb = { "name": tmp_ytd_XAxis, "y": CBList_YeartoDate[i][1]}
            CBList_YeartoDate[i] = graph_data_cb

            if i == (len(EUAList_YeartoDate) - 1):
                # 実績値の最終要素の場合、Simulationの先頭要素にも同じX軸で設定する。（実績の最後とSimulationの最初を重ねるため）
                graph_data_cb_simulation = { "name": tmp_ytd_XAxis, "y": CBList_Simulation[0][1] / 1000000}
                CBList_Simulation[0] = graph_data_cb_simulation

        
        for i in range(len(EUAList_Simulation)):
            tmp_simulation_XAxis = "(LEG E" + str(i + 1) + ")"
            XAxisList.append(tmp_simulation_XAxis)    

            # EUA_Simulationのグラフの色を設定
            graph_data_eua_simulation = { 
                "name": tmp_simulation_XAxis,
                "y": EUAList_Simulation[i][1], 
                "color": {
                    "linearGradient": { "x1": 0, "x2": 0, "y1": 0, "y2": 1 },
                    "stops": [
                        [0, 'rgb(18, 87, 151, 0.3)'],
                        [1, 'rgb(19, 101, 255, 0.3)']
                    ]
                }
            }
            EUA_graph_data.append(graph_data_eua_simulation)

            print(f"CBList_Simulation:{(CBList_Simulation)}, i = {(i)}")
            # EU対象の実測レグがある場合
            if ytd_eu_exist_flag == "1":
                graph_data_cb = { "name": tmp_simulation_XAxis, "y": CBList_Simulation[i + 1][1]}
                CBList_Simulation[i + 1] = graph_data_cb
            # EU対象の実測レグがない場合
            else:
                graph_data_cb = { "name": tmp_simulation_XAxis, "y": CBList_Simulation[i][1]}
                CBList_Simulation[i] = graph_data_cb

        # Y軸のtickInterval調整
        eua_tickInterval_tmp = math.ceil(round(max_eua / 2, 0) / 100) * 100
        # 数値の桁数を計算
        eua_digit_count = len(str(eua_tickInterval_tmp))
        # 最上位桁のみを抽出
        eua_first_digit = int(str(eua_tickInterval_tmp)[0])
        # 最上位桁以外を0にする
        eua_tickInterval = eua_first_digit * (10 ** (eua_digit_count - 1))

        # CB_YAXISの桁数を調整する
        max_cb = max_cb / 1000000
        min_cb = min_cb / 1000000
        # max/minについて、プラスマイナス場合分け
        if max_cb > 0 and min_cb > 0:
            sub_max_cb = max_cb
            sub_min_cb = 0
        elif max_cb < 0 and min_cb < 0:
            sub_max_cb = 0
            sub_min_cb = min_cb
        else:
            sub_max_cb = max_cb
            sub_min_cb = min_cb

        # CB範囲(subInterval)桁数によってtickIntervalを設定する
        subInterval  = abs(sub_max_cb - sub_min_cb)
        cb_tickInterval = maxDigitOnly(subInterval / 2)

    datas = {
        "VESSELMASTER"                      : VESSELMASTER,
        "VesselList"                        : VesselList,
        "FuelOilList"                       : FuelOilList,
        "FOCFormulas"                       : foc_formulas,
        "SimulationInformationVoyageList"   : SimulationInformationVoyageList,
        "SimulationInformationSpeedList"    : SimulationInformationSpeedList,
        "EUAList"                           : EUA_graph_data,
        "CBList_YeartoDate"                 : CBList_YeartoDate,
        "CBList_Simulation"                 : CBList_Simulation,
        "SimulationResultTotal"             : SimulationResultTotal,
        "SimulationResultUnit"              : SimulationResultUnit,
        "XAxisList"                         : XAxisList,
        "EUA_YAXIS"                         :{"max": round(max_eua, 0) , "tickInterval": eua_tickInterval },
        "CB_YAXIS"                          :{"max": max_cb, "min": min_cb, "tickInterval":cb_tickInterval}
    }

    datas = json.dumps(datas)
    print(f"datas{type(datas)}: {datas}")

    # リクエストペイロードのサイズを計算
    request_body_size = len(json.dumps(event['body']))

    # レスポンスペイロードのサイズを計算
    response_body = {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
    response_body_size = len(json.dumps(response_body))
    
    # ログにリクエストペイロードサイズとレスポンスペイロードサイズを記録
    logger.info(f"Request Payload Size: {request_body_size} bytes")
    logger.info(f"Response Payload Size: {response_body_size} bytes")
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
