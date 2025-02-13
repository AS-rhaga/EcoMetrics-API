
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

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    # 返却用リスト
    gidList    = []
    imoList    = []
    rows       = []

    dt_now = datetime.now()
    now_year = str(dt_now.year)
    str_now = dt_now.strftime('%Y-%m-%dT%H:%M')

    body = event['body']
    token = event['headers']['Authorization']
    queryStringParameters = event['queryStringParameters']

    # パラメーターを取得
    para_user_id = queryStringParameters["user_id"]
    para_year    = queryStringParameters["year"]

    # userテーブル取得
    res_user_table = select.get_user(para_user_id)[0]
    gidList        = ast.literal_eval(res_user_table["group_id"]["S"])
    company_id     = res_user_table["company_id"]["S"]

    # 初回利用ログイン時 or ログイン時初期表示 or 画面遷移を判定---------------------------------------------
    if "init" in queryStringParameters:
        para_group = gidList[0]
    else:
        para_group = queryStringParameters['group']

    # groupテーブル取得
    res_group = select.get_group(company_id, "admin")
    eua_price = res_group[0]["eua_price"]["S"] if "eua_price" in res_group[0] and res_group[0]["eua_price"]["S"] != "" else 0

    # 検索用imoリストの設定
    imo_list = []

    # Imoリストを取得-------------------------------------------------------
    if para_group == "Favorite":
        # favoriteの場合、Favoriteテーブルからimoリストを取得
        res_favorite = select.get_favorite(para_user_id)
        imo_list = ast.literal_eval(res_favorite[0]["imo_list"]["S"])       
                    
    else:
        # 上記以外の場合（基本はALL（=admin）のはず）、Groupテーブル取得し、GIDに該当するimoリストを特定する。
        res_group = select.get_group(company_id, para_group)
        imo_list = ast.literal_eval(res_group[0]["imo_list"]["S"])  
    
    imo_list = list(set(imo_list))
    print(imo_list)

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

    # fuel-oil-typeリストを取得
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

    # ---------- imo_listでループ ----------
    for loop_imo in imo_list:

        count_total_vessels += 1

        # VesselMaster取得
        res_vessel_master = select.get_vessel_master(loop_imo)
        
        # パラメーターの西暦が現在の西暦と一致する場合
        if para_year == year_now:

            # imo + operator毎の表示項目をまとめたデータセットを作成
            dataset = make_ytd_record.make_recoed(eua_price, loop_imo, para_year, fuel_oil_type_list, res_vessel_master[0])

            for data in dataset:
                # EUA、CBをytdとeoy毎に合計する
                ytd_total_eua      += float(data["ytd_eua"])
                ytd_total_eua_cost += float(data["ytd_eua_cost"])
                ytd_total_cb       += float(data["ytd_cb"])
                ytd_total_cb_cost  += float(data["ytd_cb_cost"])
                eoy_total_eua      += float(data["eoy_eua"])
                eoy_total_eua_cost += float(data["eoy_eua_cost"])
                eoy_total_cb       += float(data["eoy_cb"])
                eoy_total_cb_cost  += float(data["eoy_cb_cost"])
                rows.append(data)

        # パラメーターの西暦が過去の場合
        else:
            # imo + operator毎の表示項目をまとめたデータセットを作成
            dataset = make_ytd_record.make_recoed_past(eua_price, loop_imo, para_year, fuel_oil_type_list, res_vessel_master[0])

            for data in dataset:
                # EUA、CBをytdとeoy毎に合計する（Year to Dateは全てゼロ）
                eoy_total_eua      += float(data["eoy_eua"])
                eoy_total_eua_cost += float(data["eoy_eua_cost"])
                eoy_total_cb       += float(data["eoy_cb"])
                eoy_total_cb_cost  += float(data["eoy_cb_cost"])
                rows.append(data)

        imoList.append({"imoNo":loop_imo,"VesselName": res_vessel_master[0]["VesselName"]["S"]})

    # ---------- imo_listでのループ終了 ----------

    # VesselNameでソート
    new_rows = sorted(rows, key=lambda x: x['vessel_name'])
    new_imoList = sorted(imoList, key=lambda x: x['VesselName'])

    total_list = {
        "ytd_total_eua"     : ytd_total_eua,
        "ytd_total_eua_cost": ytd_total_eua_cost,
        "ytd_total_cb"      : round(ytd_total_cb, 1),
        "ytd_total_cb_cost" : ytd_total_cb_cost,
        "eoy_total_eua"     : eoy_total_eua,
        "eoy_total_eua_cost": eoy_total_eua_cost,
        "eoy_total_cb"      : round(eoy_total_cb, 1),
        "eoy_total_cb_cost" : eoy_total_cb_cost
    }

    # 返却用データセットを設定する
    datas = {
        "user_id"      : para_user_id,
        "group_id"     : para_group,
        "company_id"   : company_id,
        "gid"          : para_group,
        "gidList"      : gidList,
        "imoList"      : new_imoList,
        "total_vessels": count_total_vessels,
        "total_list"   : total_list,
        "eua_price"    : eua_price,
        "rows"         : new_rows
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
