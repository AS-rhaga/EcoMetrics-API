
from datetime import datetime
import json
import math
import re

from dynamodb import select
from vesselinfo import make_ytd_record
from calculate import calculate_function
import auth

import logging
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Ecoで使用する燃料の情報リスト
LNG_info_list = {}
HFO_info_list = {}
LFO_info_list = {}
MDO_info_list = {}
MGO_info_list = {}

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    body = event['body']
    pathParameters = event['pathParameters']['proxy'].split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']

    # パラメーター取得
    user_id        = queryStringParameters['user_id']
    
    # マルチパートデータの境界を解析
    boundary = re.search(r'------WebKitFormBoundary[\w\d]+', body).group()
    
    # 各パートを抽出
    parts = body.split(boundary)
    
    # フォームデータの辞書
    form_data = {}

    # 各パートを解析して値を取得
    for part in parts:
        if 'Content-Disposition' in part:
            name_match = re.search(r'name="([^"]+)"', part)
            if name_match:
                name = name_match.group(1)
                value = part.split('\r\n\r\n')[1].strip()
                form_data[name] = value

    imo = form_data["imo"]

    data_pooling_list = form_data["edit_conditions_list"]
    
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
    
    # 燃料の情報リスト
    global LNG_info_list
    global HFO_info_list
    global LFO_info_list
    global MDO_info_list
    global MGO_info_list

    # 返却用リスト
    ytd_grouped_total_list       = []
    eoy_grouped_total_list       = []
    ytd_grouped_vessels_list     = []
    sub_ytd_grouped_vessels_list = []
    eoy_grouped_vessels_list     = []
    sub_eoy_grouped_vessels_list = []
    ytd_not_grouped_vessels_list = []
    eoy_not_grouped_vessels_list = []

    # 船リストの総数
    count_total_vessels = 0

    # グルーピングされていない船の合計値用
    ytd_not_grouped_vessels_total_lng    = 0
    ytd_not_grouped_vessels_total_hfo    = 0
    ytd_not_grouped_vessels_total_lfo    = 0
    ytd_not_grouped_vessels_total_mdo    = 0
    ytd_not_grouped_vessels_total_mgo    = 0
    ytd_not_grouped_vessels_total_energy = 0
    eoy_not_grouped_vessels_total_lng    = 0
    eoy_not_grouped_vessels_total_hfo    = 0
    eoy_not_grouped_vessels_total_lfo    = 0
    eoy_not_grouped_vessels_total_mdo    = 0
    eoy_not_grouped_vessels_total_mgo    = 0
    eoy_not_grouped_vessels_total_energy = 0

    ytd_not_grouped_vessels_total_cb      = 0
    ytd_not_grouped_vessels_total_cb_cost = 0
    eoy_not_grouped_vessels_total_cb      = 0
    eoy_not_grouped_vessels_total_cb_cost = 0

    # fuel-oil-typeリストを取得
    fuel_oil_type_info_list = select.get_fuel_oil_type()
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

    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # company_id取得
    company_id = select.get_user(user_id)[0]["company_id"]["S"]

    company_and_year = year_now + company_id

    # GHG上限値を算出する
    GHG_Max    = calculate_function.calc_GHG_Max(year_now)

    # groupテーブルのadminのimoリストを取得
    imo_list = select.get_group(company_id, "admin")[0]["imo_list"]


    # 登録されたプーリンググループを取得
    res_pooling_group_list = select.get_pooling_table(company_and_year)

    # 取得したプーリンググループから、合計値管理用DataFrameを設定する。
    pooling_group_name_list = []
    data1 = []
    # res_pooling_group_list = [
    #     {
    #         "company_and_year": "NYK2024",
    #         "group_name"      : "group1",
    #         "imo_list"        : []
    #     },
    #     {
    #         "company_and_year": "NYK2024",
    #         "group_name"      : "group2",
    #         "imo_list"        : []
    #     },
    #             {
    #         "company_and_year": "NYK2024",
    #         "group_name"      : "group3",
    #         "im0_list"        : []
    #     }
    # ]

    # プーリンググループの名前リストを作成
    for i in range(len(res_pooling_group_list)):
        pooling_group_name_list.append(res_pooling_group_list[i]["group_name"])

    columns1 =["ytd_total_lng", "ytd_total_hfo", "ytd_total_lfo", "ytd_total_mdo", "ytd_total_mgo", "ytd_total_energy", "eoy_total_lng", "eoy_total_hfo", "eoy_total_lfo", "eoy_total_mdo", "eoy_total_mgo", "eoy_total_energy"]
    data1 = [[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]
    df = pd.DataFrame(data = data1, index = pooling_group_name_list, columns = columns1)

    # adminの全船リストをimoでループさせる
    for imo in imo_list:

        # 合計船数を加算
        count_total_vessels += 1

        count_ytd_borrowing_vessels = 0
        ytd_borrowing_cb            = 0
        count_ytd_banking_vessels   = 0
        ytd_banking_cb              = 0
        count_eoy_borrowing_vessels = 0
        eoy_borrowing_cb            = 0
        count_eoy_banking_vessels   = 0
        eoy_banking_cb              = 0

        # プーリンググループリストにレコードがある間
        for pooling_group in res_pooling_group_list:
            group_name     = pooling_group["group_name"]
            group_imo_list = pooling_group["imo_list"]

            # そのプーリンググループの中に、該当imoが含まれる場合
            if imo in group_imo_list:

                # 1船舶あたりの年間情報と実測データを算出する
                ytd_grouped_vessel_info, ytd_lng, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, ytd_energy, eoy_grouped_vessel_info, eoy_lng, eoy_hfo, eoy_lfo, eoy_mdo, eoy_mgo, eoy_energy = make_ytd_record.make_recoed(imo, year_now, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)

                # グルーピングされている船の情報をリストに追加する
                for j in range(len(ytd_grouped_vessel_info)):
                    append_data = [group_name, ytd_grouped_vessel_info[j]]
                    sub_ytd_grouped_vessels_list.append(append_data)

                    append_data = [group_name, eoy_grouped_vessel_info[j]]
                    sub_eoy_grouped_vessels_list.append(append_data)

                # プーリンググループの合計値（加算前）を取得する。
                bk_ytd_total_lng    = df.at[group_name, "ytd_total_lng"]
                bk_ytd_total_hfo    = df.at[group_name, "ytd_total_hfo"]
                bk_ytd_total_lfo    = df.at[group_name, "ytd_total_lfo"]
                bk_ytd_total_mdo    = df.at[group_name, "ytd_total_mdo"]
                bk_ytd_total_mgo    = df.at[group_name, "ytd_total_mgo"]
                bk_ytd_total_energy = df.at[group_name, "ytd_total_energy"]

                bk_eoy_total_lng    = df.at[group_name, "eoy_total_lng"]
                bk_eoy_total_hfo    = df.at[group_name, "eoy_total_hfo"]
                bk_eoy_total_lfo    = df.at[group_name, "eoy_total_lfo"]
                bk_eoy_total_mdo    = df.at[group_name, "eoy_total_mdo"]
                bk_eoy_total_mgo    = df.at[group_name, "eoy_total_mgo"]
                bk_eoy_total_energy = df.at[group_name, "eoy_total_energy"]

                # プーリンググループの合計値に加算して更新する。
                df.at[group_name, "ytd_total_lng"]    = bk_ytd_total_lng + ytd_lng
                df.at[group_name, "ytd_total_hfo"]    = bk_ytd_total_hfo + ytd_hfo
                df.at[group_name, "ytd_total_lfo"]    = bk_ytd_total_lfo + ytd_lfo
                df.at[group_name, "ytd_total_mdo"]    = bk_ytd_total_mdo + ytd_mdo
                df.at[group_name, "ytd_total_mgo"]    = bk_ytd_total_mgo + ytd_mgo
                df.at[group_name, "ytd_total_energy"] = bk_ytd_total_energy + ytd_energy

                df.at[group_name, "eoy_total_lng"]    = bk_eoy_total_lng + eoy_lng
                df.at[group_name, "eoy_total_hfo"]    = bk_eoy_total_hfo + eoy_hfo
                df.at[group_name, "eoy_total_lfo"]    = bk_eoy_total_lfo + eoy_lfo
                df.at[group_name, "eoy_total_mdo"]    = bk_eoy_total_mdo + eoy_mdo
                df.at[group_name, "eoy_total_mgo"]    = bk_eoy_total_mgo + eoy_mgo
                df.at[group_name, "eoy_total_energy"] = bk_eoy_total_energy + eoy_energy
                
                break

            #  どのプーリンググループにも、該当imoが含まれない場合
            else:
                # 1船舶あたりの年間情報と実測データを算出する
                ytd_not_grouped_vessel_info, ytd_lng, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, ytd_energy, eoy_not_grouped_vessel_info, eoy_lng, eoy_hfo, eoy_lfo, eoy_mdo, eoy_mgo, eoy_energy = make_ytd_record.make_recoed(imo, year_now, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)

                # Year to Dateについて
                # グルーピングされていない船の情報をリストに追加する
                for vessel_info in ytd_not_grouped_vessel_info:

                    # banking, borrowingの確認をする
                    banking_cb   = vessel_info["banking"]
                    borrowing_cb = vessel_info["borrowing"]
                    if banking_cb > 0:
                        count_ytd_banking_vessels += 1
                        ytd_banking_cb            += banking_cb
                    elif borrowing_cb > 0:
                        count_ytd_borrowing_vessels += 1
                        ytd_borrowing_cb            += borrowing_cb

                    ytd_not_grouped_vessels_list.append(vessel_info)

                # グルーピングされていない船の消費燃料、エネルギーを足し合わせる
                ytd_not_grouped_vessels_total_lng    += ytd_lng
                ytd_not_grouped_vessels_total_hfo    += ytd_hfo
                ytd_not_grouped_vessels_total_lfo    += ytd_lfo
                ytd_not_grouped_vessels_total_mdo    += ytd_mdo
                ytd_not_grouped_vessels_total_mgo    += ytd_mgo
                ytd_not_grouped_vessels_total_energy += ytd_energy

                # End of Yearについて
                # グルーピングされていない船の情報をリストに追加する
                for vessel_info in eoy_not_grouped_vessel_info:

                    # banking, borrowingの確認をする
                    banking_cb   = vessel_info["banking"]
                    borrowing_cb = vessel_info["borrowing"]
                    if banking_cb > 0:
                        count_eoy_banking_vessels += 1
                        eoy_banking_cb            += banking_cb
                    elif borrowing_cb > 0:
                        count_eoy_borrowing_vessels += 1
                        eoy_borrowing_cb            += borrowing_cb

                    eoy_not_grouped_vessels_list.append(vessel_info)

                # グルーピングされていない船の消費燃料、エネルギーを足し合わせる
                eoy_not_grouped_vessels_total_lng    += eoy_lng
                eoy_not_grouped_vessels_total_hfo    += eoy_hfo
                eoy_not_grouped_vessels_total_lfo    += eoy_lfo
                eoy_not_grouped_vessels_total_mdo    += eoy_mdo
                eoy_not_grouped_vessels_total_mgo    += eoy_mgo
                eoy_not_grouped_vessels_total_energy += eoy_energy

    # ----------- ループ終了 -----------

    # グルーピングされていない船たちの合計CB、CB costを算出する
    # Year to Date
    ytd_not_grouped_vessels_total_GHG = calculate_function.calc_GHG_Actual(ytd_not_grouped_vessels_total_lng, ytd_not_grouped_vessels_total_hfo, ytd_not_grouped_vessels_total_lfo, ytd_not_grouped_vessels_total_mdo, ytd_not_grouped_vessels_total_mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)
    ytd_not_grouped_vessels_total_cb  = (GHG_Max - ytd_not_grouped_vessels_total_GHG) * ytd_not_grouped_vessels_total_energy

    if ytd_not_grouped_vessels_total_cb < 0:
        ytd_not_grouped_vessels_total_cb_cost = abs(ytd_not_grouped_vessels_total_cb) * 2400 / (ytd_not_grouped_vessels_total_GHG * 41000)

    ytd_not_grouped_total = {
        "total_cb"     : str(round(ytd_not_grouped_vessels_total_cb, 0)),
        "total_cb_cost": str(round(ytd_not_grouped_vessels_total_cb_cost, 0)),
    }

    # End of Year
    eoy_not_grouped_vessels_total_GHG = calculate_function.calc_GHG_Actual(eoy_not_grouped_vessels_total_lng, eoy_not_grouped_vessels_total_hfo, eoy_not_grouped_vessels_total_lfo, eoy_not_grouped_vessels_total_mdo, eoy_not_grouped_vessels_total_mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)
    eoy_not_grouped_vessels_total_cb  = (GHG_Max - eoy_not_grouped_vessels_total_GHG) * eoy_not_grouped_vessels_total_energy

    if eoy_not_grouped_vessels_total_cb < 0:
        eoy_not_grouped_vessels_total_cb_cost = abs(eoy_not_grouped_vessels_total_cb) * 2400 / (eoy_not_grouped_vessels_total_GHG * 41000)

    eoy_not_grouped_total = {
        "total_cb"     : str(round(eoy_not_grouped_vessels_total_cb, 0)),
        "total_cb_cost": str(round(eoy_not_grouped_vessels_total_cb_cost, 0)),
    }

    # プーリングされている船舶のimo+operator別レコードを、グループ毎に整理する
    ytd_grouped_vessels_total_lng = 0
    ytd_grouped_vessels_total_hfo = 0
    ytd_grouped_vessels_total_lfo = 0
    ytd_grouped_vessels_total_mdo = 0
    ytd_grouped_vessels_total_mgo = 0
    ytd_grouped_vessels_total_energy = 0

    eoy_grouped_vessels_total_lng = 0
    eoy_grouped_vessels_total_hfo = 0
    eoy_grouped_vessels_total_lfo = 0
    eoy_grouped_vessels_total_mdo = 0
    eoy_grouped_vessels_total_mgo = 0
    eoy_grouped_vessels_total_energy = 0

    # プーリンググループ名でループ
    for group_name in pooling_group_name_list:

        ytd_this_group_vessels = []
        eoy_this_group_vessels = []

        # Year to Date
        for grouped_vessel in sub_ytd_grouped_vessels_list:
            vessels_group_name = grouped_vessel[0]

            # 外ループのグループ名と船舶の所属するグループ名が一致する時
            if vessels_group_name == group_name:
                append_data = {
                    "imo"          : grouped_vessel["imo"],
                    "operator"     : grouped_vessel["operator"],
                    "distance"     : grouped_vessel["distance"],
                    "foc"          : grouped_vessel["foc"],
                    "year_to_date" : grouped_vessel["year_to_date"],
                    "last_year"    : grouped_vessel["last_year"],
                    "total"        : grouped_vessel["total"],
                    "penlty_factor": grouped_vessel["penlty_factor"]
                }
                ytd_this_group_vessels.append(append_data)
        
        ytd_grouped_vessels_list.append([group_name, ytd_this_group_vessels])

        # End of Year
        for grouped_vessel in sub_eoy_grouped_vessels_list:
            vessels_group_name = grouped_vessel[0]

            # 外ループのグループ名と船舶の所属するグループ名が一致する時
            if vessels_group_name == group_name:
                append_data = {
                    "imo"          : grouped_vessel["imo"],
                    "operator"     : grouped_vessel["operator"],
                    "distance"     : grouped_vessel["distance"],
                    "foc"          : grouped_vessel["foc"],
                    "year_to_date" : grouped_vessel["year_to_date"],
                    "last_year"    : grouped_vessel["last_year"],
                    "total"        : grouped_vessel["total"],
                    "penlty_factor": grouped_vessel["penlty_factor"]
                }
                eoy_this_group_vessels.append(append_data)
        
        eoy_grouped_vessels_list.append([group_name, eoy_this_group_vessels])

        # グルーピングされた船の燃料消費量とエネルギー消費量の合計値を算出する
        ytd_grouped_vessels_total_lng += df.at[group_name, "ytd_total_lng"]
        ytd_grouped_vessels_total_hfo += df.at[group_name, "ytd_total_hfo"]
        ytd_grouped_vessels_total_lfo += df.at[group_name, "ytd_total_lfo"]
        ytd_grouped_vessels_total_mdo += df.at[group_name, "ytd_total_mdo"]
        ytd_grouped_vessels_total_mgo += df.at[group_name, "ytd_total_mgo"]
        ytd_grouped_vessels_total_energy += df.at[group_name, "ytd_total_energy"]

        eoy_grouped_vessels_total_lng += df.at[group_name, "eoy_total_lng"]
        eoy_grouped_vessels_total_hfo += df.at[group_name, "eoy_total_hfo"]
        eoy_grouped_vessels_total_lfo += df.at[group_name, "eoy_total_lfo"]
        eoy_grouped_vessels_total_mdo += df.at[group_name, "eoy_total_mdo"]
        eoy_grouped_vessels_total_mgo += df.at[group_name, "eoy_total_mgo"]
        eoy_grouped_vessels_total_energy += df.at[group_name, "eoy_total_energy"]

        # グルーピングされている船たちの合計CB、CB costを算出する
        # Year to Date
        ytd_grouped_vessels_total_GHG = calculate_function.calc_GHG_Actual(ytd_grouped_vessels_total_lng, ytd_grouped_vessels_total_hfo, ytd_grouped_vessels_total_lfo, ytd_grouped_vessels_total_mdo, ytd_grouped_vessels_total_mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)
        ytd_grouped_vessels_total_cb  = (GHG_Max - ytd_grouped_vessels_total_GHG) * ytd_grouped_vessels_total_energy

        if ytd_grouped_vessels_total_cb < 0:
            ytd_grouped_vessels_total_cb_cost = abs(ytd_grouped_vessels_total_cb) * 2400 / (ytd_grouped_vessels_total_GHG * 41000)

        ytd_grouped_total = {
            "total_cb"     : str(round(ytd_grouped_vessels_total_cb, 0)),
            "total_cb_cost": str(round(ytd_grouped_vessels_total_cb_cost, 0))
        }
        ytd_grouped_total_list.append(ytd_grouped_total)

        # End of Year
        eoy_grouped_vessels_total_GHG = calculate_function.calc_GHG_Actual(eoy_grouped_vessels_total_lng, eoy_grouped_vessels_total_hfo, eoy_grouped_vessels_total_lfo, eoy_grouped_vessels_total_mdo, eoy_grouped_vessels_total_mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)
        eoy_grouped_vessels_total_cb  = (GHG_Max - eoy_grouped_vessels_total_GHG) * eoy_grouped_vessels_total_energy

        if eoy_grouped_vessels_total_cb < 0:
            eoy_grouped_vessels_total_cb_cost = abs(eoy_grouped_vessels_total_cb) * 2400 / (eoy_grouped_vessels_total_GHG * 41000)

        eoy_grouped_total = {
            "total_cb"     : str(round(eoy_grouped_vessels_total_cb, 0)),
            "total_cb_cost": str(round(eoy_grouped_vessels_total_cb_cost, 0))
        }
        eoy_grouped_total_list.append(eoy_grouped_total)

    # Year to Dateの合計値を算出する
    ytd_total_lng = ytd_not_grouped_vessels_total_lng + ytd_grouped_vessels_total_lng
    ytd_total_hfo = ytd_not_grouped_vessels_total_hfo + ytd_grouped_vessels_total_hfo
    ytd_total_lfo = ytd_not_grouped_vessels_total_lfo + ytd_grouped_vessels_total_lfo
    ytd_total_mdo = ytd_not_grouped_vessels_total_mdo + ytd_grouped_vessels_total_mdo
    ytd_total_mgo = ytd_not_grouped_vessels_total_mgo + ytd_grouped_vessels_total_mgo
    ytd_total_energy = ytd_not_grouped_vessels_total_energy + ytd_grouped_vessels_total_energy

    # End of Yearの合計値を算出する
    eoy_total_lng = eoy_not_grouped_vessels_total_lng + eoy_grouped_vessels_total_lng
    eoy_total_hfo = eoy_not_grouped_vessels_total_hfo + eoy_grouped_vessels_total_hfo
    eoy_total_lfo = eoy_not_grouped_vessels_total_lfo + eoy_grouped_vessels_total_lfo
    eoy_total_mdo = eoy_not_grouped_vessels_total_mdo + eoy_grouped_vessels_total_mdo
    eoy_total_mgo = eoy_not_grouped_vessels_total_mgo + eoy_grouped_vessels_total_mgo
    eoy_total_energy = eoy_not_grouped_vessels_total_energy + eoy_grouped_vessels_total_energy

    # CB, CB costを算出する
    ytd_total_GHG = calculate_function.calc_GHG_Actual(ytd_total_lng, ytd_total_hfo, ytd_total_lfo, ytd_total_mdo, ytd_total_mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)
    ytd_total_cb  = (GHG_Max - ytd_total_GHG) * ytd_total_energy
    ytd_total_cb_cost = 0
    if ytd_total_cb < 0:
        ytd_total_cb_cost = ytd_total_cb * 2400 / (ytd_total_GHG * 41000)

    eoy_total_GHG = calculate_function.calc_GHG_Actual(eoy_total_lng, eoy_total_hfo, eoy_total_lfo, eoy_total_mdo, eoy_total_mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)
    eoy_total_cb  = (GHG_Max - eoy_total_GHG) * eoy_total_energy
    eoy_total_cb_cost = 0
    if eoy_total_cb < 0:
        eoy_total_cb_cost = eoy_total_cb * 2400 / (eoy_total_GHG * 41000)

    ytd_total_list = {
        "total_cb"         : str(round(ytd_total_cb, 0)),
        "total_cb_cost"    : str(round(ytd_total_cb_cost, 0)),
        "pooling_group"    : str(len(pooling_group_name_list)),
        "banking_vessels"  : str(count_ytd_banking_vessels),
        "banking_cb"       : str(ytd_banking_cb),
        "borrowing vessels": str(count_ytd_borrowing_vessels),
        "borrowing_cb"     : str(ytd_borrowing_cb)
    }
    eoy_total_list = {
        "total_cb"         : str(round(eoy_total_cb, 0)),
        "total_cb_cost"    : str(round(eoy_total_cb_cost, 0)),
        "pooling_group"    : str(len(pooling_group_name_list)),
        "banking_vessels"  : str(count_eoy_banking_vessels),
        "banking_cb"       : str(eoy_banking_cb),
        "borrowing vessels": str(count_eoy_borrowing_vessels),
        "borrowing_cb"     : str(eoy_borrowing_cb)
    }

    # 返却用データセットを設定する
    datas = {
        "total_vessels"               : count_total_vessels,
        "ytd_total_list"              : ytd_total_list,
        "eoy_total_list"              : eoy_total_list,
        "ytd_group_total_list"        : ytd_grouped_total_list,
        "ytd_grouped_vessels_list"    : ytd_grouped_vessels_list,
        "ytd_not_grouped_total"       : ytd_not_grouped_total,
        "ytd_not_grouped_vessels_list": ytd_not_grouped_vessels_list,
        "eoy_group_total_list"        : eoy_grouped_total_list,
        "eoy_grouped_vessels_list"    : eoy_grouped_vessels_list,
        "eoy_not_grouped_total"       : eoy_not_grouped_total,
        "eoy_not_grouped_vessels_list": eoy_not_grouped_vessels_list
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
