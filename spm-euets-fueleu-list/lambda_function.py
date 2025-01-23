
from datetime import datetime
import json
import math
import re
import ast

from dynamodb import select
from vesselinfo import make_ytd_record
from calculate import calculate_function
from Util import Util
import auth

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Ecoで使用する燃料の情報リスト
LNG_ODS_info_list              = []
LNG_OMS_info_list              = []
LNG_OSS_info_list              = []
HFO_info_list                  = []
LFO_info_list                  = []
MDO_info_list                  = []
MGO_info_list                  = []
LPG_Propane_info_list          = []
LPG_Butane_info_list           = []
NH3_Natural_Gas_info_list      = []
NH3_eFuel_info_list            = []
Methanol_Natural_Gas_info_list = []
H2_Natural_Gas_info_list       = []

# EUAの算出メソッド
def calc_co2(year, eu_rate, lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng):

    # EUAの算出
    co2_total   = 0
    eu_ets_rate = 0
    eua         = 0

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

        if lng_ods > 0:
            lng_co2_factor =  float(LNG_ODS_info_list["emission_factor"]["S"])
            co2_total += lng_ods * lng_co2_factor
        if lng_oms > 0:
            lng_co2_factor =  float(LNG_OMS_info_list["emission_factor"]["S"])
            co2_total += lng_oms * lng_co2_factor
        if lng_oss > 0:
            lng_co2_factor =  float(LNG_OSS_info_list["emission_factor"]["S"])
            co2_total += lng_oss * lng_co2_factor
        if hfo > 0:
            hfo_co2_factor =  float(HFO_info_list["emission_factor"]["S"])
            co2_total += hfo * hfo_co2_factor
        if lfo > 0:
            lfo_co2_factor =  float(LFO_info_list["emission_factor"]["S"])
            co2_total += lfo * lfo_co2_factor
        if mdo > 0:
            mdo_co2_factor =  float(MDO_info_list["emission_factor"]["S"])
            co2_total += mdo * mdo_co2_factor
        if mgo > 0:
            mgo_co2_factor =  float(MGO_info_list["emission_factor"]["S"])
            co2_total += mgo * mgo_co2_factor
        if lpg_p > 0:
            lpg_p_co2_factor = float(LPG_Propane_info_list["emission_factor"]["S"])
            co2_total += lpg_p * lpg_p_co2_factor
        if lpg_b > 0:
            lpg_b_co2_factor = float(LPG_Butane_info_list["emission_factor"]["S"])
            co2_total += lpg_b * lpg_b_co2_factor
        if nh3_ng > 0:
            nh3_ng_co2_factor = float(NH3_Natural_Gas_info_list["emission_factor"]["S"])
            co2_total += nh3_ng * nh3_ng_co2_factor
        if nh3_ef > 0:
            nh3_ef_co2_factor = float(NH3_eFuel_info_list["emission_factor"]["S"])
            co2_total += nh3_ef * nh3_ef_co2_factor
        if methanol_ng > 0:
            methanol_ng_co2_factor = float(Methanol_Natural_Gas_info_list["emission_factor"]["S"])
            co2_total = methanol_ng * methanol_ng_co2_factor
        if h2_ng > 0:
            h2_ng_co2_factor = float(H2_Natural_Gas_info_list["emission_factor"]["S"])
            co2_total = h2_ng * h2_ng_co2_factor

        eua = total_co2 * eu_rate / 100

    return eua

# EUAの算出メソッド
def calc_eua(year, eu_rate, total_co2):

    # EUAの算出
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

        eua       = total_co2 * float(eu_ets_rate) / 100 * float(eu_rate) / 100
        print(f"eua{type(eua)}: {eua}")
    return eua

# エネルギーの総消費量を算出するメソッド
def calc_energy(eu_rate, lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng):
    total_energy = 0

    if lng_ods > 0:
        lng_ods_lcv =  float(LNG_ODS_info_list["lcv"]["S"])
        total_energy += lng_ods * lng_ods_lcv
    if lng_oms > 0:
        lng_oms_lcv =  float(LNG_OMS_info_list["lcv"]["S"])
        total_energy += lng_ods * lng_oms_lcv
    if lng_oss > 0:
        lng_oss_lcv =  float(LNG_OSS_info_list["lcv"]["S"])
        total_energy += lng_oss * lng_oss_lcv
    if hfo > 0:
        hfo_lcv =  float(HFO_info_list["lcv"]["S"])
        total_energy += hfo * hfo_lcv
    if lfo > 0:
        lfo_lcv =  float(LFO_info_list["lcv"]["S"])
        total_energy += lfo * lfo_lcv
    if mdo > 0:
        mdo_lcv =  float(MDO_info_list["lcv"]["S"])
        total_energy += mdo * mdo_lcv
    if mgo > 0:
        mgo_lcv =  float(MGO_info_list["lcv"]["S"])
        total_energy += mgo * mgo_lcv
    if lpg_p > 0:
        lpg_p_lcv = float(LPG_Propane_info_list["lcv"]["S"])
        total_energy += lpg_p * lpg_p_lcv
    if lpg_b > 0:
        lpg_b_lcv = float(LPG_Butane_info_list["lcv"]["S"])
        total_energy += lpg_b * lpg_b_lcv
    if nh3_ng > 0:
        nh3_ng_lcv = float(NH3_Natural_Gas_info_list["lcv"]["S"])
        total_energy += nh3_ng * nh3_ng_lcv
    if nh3_ef > 0:
        nh3_ef_lcv = float(NH3_eFuel_info_list["lcv"]["S"])
        total_energy += nh3_ef * nh3_ef_lcv
    if methanol_ng > 0:
        methanol_ng_lcv = float(Methanol_Natural_Gas_info_list["lcv"]["S"])
        total_energy += methanol_ng * methanol_ng_lcv
    if h2_ng > 0:
        h2_ng_lcv = float(H2_Natural_Gas_info_list["lcv"]["S"])
        total_energy += h2_ng * h2_ng_lcv

    return_energy = total_energy * float(eu_rate) / 100

    return return_energy

# 該当年のGHG強度上限値を算出するメソッド
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

# コンプライアンスバランスを算出するメソッド
def calc_cb(year_timestamp, energy, GHG_Actual):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    cb_formatted = str(round(float(cb), 1))
    print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")
    return cb_formatted

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    dt_now = datetime.now()
    now_year = str(dt_now.year)
    str_now = dt_now.strftime('%Y-%m-%dT%H:%M')

    body = event['body']
    token = event['headers']['Authorization']
    queryStringParameters = event['queryStringParameters']

    # パラメーターを取得
    para_user_id = queryStringParameters["user_id"]
    para_group   = queryStringParameters["group"]
    para_year    = queryStringParameters["year"]
    
    # 燃料の情報リスト
    global LNG_ODS_info_list
    global LNG_OMS_info_list
    global LNG_OSS_info_list
    global HFO_info_list
    global LFO_info_list
    global MDO_info_list
    global MGO_info_list
    global LPG_Propane_info_list
    global LPG_Butane_info_list
    global NH3_Natural_Gas_info_list
    global NH3_eFuel_info_list
    global Methanol_Natural_Gas_info_list
    global H2_Natural_Gas_info_list

    # 返却用リスト
    gidList    = []
    imoList    = []
    rows       = []

    # 取得したリスト
    res_simulation_voyage_list = []
    res_simulation_speed_list  = []
    res_foc_formulas           = []

    # 船リストの総数
    count_total_vessels = 0

    # 合計値用
    ytd_total_eua      = 0
    ytd_total_eua_cost = 0
    ytd_total_cb       = 0
    ytd_total_cb_cost  = 0
    eoy_total_eua      = 0
    eoy_total_eua_cost = 0
    eoy_total_cb       = 0
    eoy_total_cb_cost  = 0

    # fuel-oil-typeリストを取得（要修正⇒修正済み）
    fuel_oil_type_info_list = select.get_fuel_oil_type()
    for i in range(len(fuel_oil_type_info_list)):
        name = fuel_oil_type_info_list[i]["fuel_oil_type"]["S"]
        if  name == "LNG(Otto Diesel Speed)":
            LNG_ODS_info_list = fuel_oil_type_info_list[i]
        elif  name == "LNG(Otto Medium Speed)":
            LNG_OMS_info_list = fuel_oil_type_info_list[i]
        elif  name == "LNG(Otto Slow Speed)":
            LNG_OSS_info_list = fuel_oil_type_info_list[i]
        elif name == "HFO":
            HFO_info_list = fuel_oil_type_info_list[i]
        elif name == "LFO":
            LFO_info_list = fuel_oil_type_info_list[i]
        elif name == "MDO":
            MDO_info_list = fuel_oil_type_info_list[i]
        elif name == "MGO":
            MGO_info_list = fuel_oil_type_info_list[i]
        elif name == "LPG(Propane)":
            LPG_Propane_info_list = fuel_oil_type_info_list[i]
        elif name == "LPG(Butane)":
            LPG_Butane_info_list = fuel_oil_type_info_list[i]
        elif name == "NH3(Natural gas)":
            NH3_Natural_Gas_info_list = fuel_oil_type_info_list[i]
        elif name == "NH3(e-fuel)":
            NH3_eFuel_info_list = fuel_oil_type_info_list[i]
        elif name == "Methanol(Natural gas)":
            Methanol_Natural_Gas_info_list = fuel_oil_type_info_list[i]
        elif name == "H2(Natural gas)":
            H2_Natural_Gas_info_list = fuel_oil_type_info_list[i]

    fuel_oil_type_list = {
        "LNG_ODS_info_list"             : LNG_ODS_info_list, 
        "LNG_OMS_info_list"             : LNG_OMS_info_list, 
        "LNG_OSS_info_list"             : LNG_OSS_info_list, 
        "HFO_info_list"                 : HFO_info_list, 
        "LFO_info_list"                 : LFO_info_list, 
        "MDO_info_list"                 : MDO_info_list, 
        "MGO_info_list"                 : MGO_info_list, 
        "LPG_Propane_info_list"         : LPG_Propane_info_list, 
        "LPG_Butane_info_list"          : LPG_Butane_info_list, 
        "NH3_Natural_Gas_info_list"     : NH3_Natural_Gas_info_list, 
        "NH3_eFuel_info_list"           : NH3_eFuel_info_list, 
        "Methanol_Natural_Gas_info_list": Methanol_Natural_Gas_info_list, 
        "H2_Natural_Gas_info_list"      : H2_Natural_Gas_info_list
    }

    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # GHG上限値を算出する
    GHG_Max    = calculate_function.calc_GHG_Max(year_now)

    # userテーブル取得
    res_user_table = select.get_user(para_user_id)[0]
    gidList        = res_user_table["group_id"]["S"]
    company_id     = res_user_table["company_id"]["S"]

    # groupテーブル取得
    res_group = select.get_group(company_id, "admin")
    eua_price = res_group["eua_price"]["S"] if "eua_prise" in res_group and res_group["eua_price"]["S"] != "" else 0

    # 検索用imoリストの設定
    imo_list = []

    if para_group != "admin":
        imo_list = select.get_favolite_table(para_user_id)[0]["imo_list"]
    else:
        imo_list = select.get_group(company_id, "admin")[0]["imo_list"]

    # ---------- imo_listでループ ----------
    for loop_imo in imo_list:

        count_total_vessels += 1

        # パラメーターの西暦が現在の西暦と一致する場合
        if para_year == year_now:

            # imo + operator毎の表示項目をまとめたデータセットを作成
            dataset = make_ytd_record.make_recoed(eua_price, loop_imo, para_year, fuel_oil_type_list)

            # EUA、CBをytdとeoy毎に合計する
            ytd_total_eua      += dataset["ytd_eua"]
            ytd_total_eua_cost *= dataset["ytd_eua_cost"]
            ytd_total_cb       += dataset["ytd_cb"]
            ytd_total_cb_cost  += dataset["ytd_cb_cost"]
            eoy_total_eua      += dataset["eoy_eua"]
            eoy_total_eua_cost *= dataset["eoy_eua_cost"]
            eoy_total_cb       += dataset["eoy_cb"]
            eoy_total_cb_cost  += dataset["eoy_cb_cost"]

            rows.append(dataset)

        # パラメーターの西暦が過去の場合
        else:
            # imo + operator毎の表示項目をまとめたデータセットを作成
            dataset = make_ytd_record.make_recoed_past(eua_price, loop_imo, para_year, fuel_oil_type_list)

            # EUA、CBをytdとeoy毎に合計する（Year to Dateは全てゼロ）
            eoy_total_eua      += dataset["eoy_eua"]
            eoy_total_eua_cost *= dataset["eoy_eua_cost"]
            eoy_total_cb       += dataset["eoy_cb"]
            eoy_total_cb_cost  += dataset["eoy_cb_cost"]

    # ---------- imo_listでのループ終了 ----------

    total_list = {
        "ytd_total_eua"     : ytd_total_eua,
        "ytd_total_eua_cost": ytd_total_eua_cost,
        "ytd_total_cb"      : ytd_total_cb,
        "ytd_total_cb_cost" : ytd_total_cb_cost,
        "eoy_total_eua"     : eoy_total_eua,
        "eoy_total_eua_cost": eoy_total_eua_cost,
        "eoy_total_cb"      : eoy_total_cb,
        "eoy_total_cb_cost" : eoy_total_cb_cost
    }

    # 返却用データセットを設定する
    datas = {
        "user_id"      : para_user_id,
        "group_id"     : para_group,
        "company_id"   : company_id,
        "gid"          : para_group,
        "gidList"      : gidList,
        "imoList"      : imo_list,
        "total_vessels": count_total_vessels,
        "total_list"   : total_list,
        "rows"         : rows
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
