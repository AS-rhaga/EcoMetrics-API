
from datetime import datetime
import json
import math
import re
import ast

from dynamodb import select
from vesselinfo import make_ytd_record
from calculate import calculate_function
import auth

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def make_fuel_oil_type_info_list():

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

    return fuel_oil_type_list

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    pathParameters = event['pathParameters']['proxy'].split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']

    user_id   = queryStringParameters["user"]
    para_year = queryStringParameters["year"]

    # 返却用リスト
    # ytd_grouped_total_list       = []
    # eoy_grouped_total_list       = []
    # ytd_grouped_vessels_list     = []
    sub_ytd_grouped_vessels_list = []
    # eoy_grouped_vessels_list     = []
    sub_eoy_grouped_vessels_list = []
    ytd_not_grouped_vessels_list = []
    eoy_not_grouped_vessels_list = []
    ytd_all_vessels_list         = []
    ytd_pooling_group_list       = []
    eoy_pooling_group_list       = []

    # 船リストの総数
    count_total_vessels = 0

    # グルーピングされていない船の合計値用
    ytd_not_grouped_vessels_total_cb      = 0
    ytd_not_grouped_vessels_total_cb_cost = 0
    eoy_not_grouped_vessels_total_cb      = 0
    eoy_not_grouped_vessels_total_cb_cost = 0

    ytd_total_cb = 0
    ytd_total_cb_cost = 0
    eoy_total_cb = 0
    eoy_total_cb_cost = 0

    # fuel-oil-typeリストを取得
    fuel_oil_type_info_list = make_fuel_oil_type_info_list()

    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # company_id取得
    company_id = select.get_user(user_id)[0]["company_id"]["S"]

    company_and_year = para_year + company_id

    # GHG上限値を算出する
    GHG_Max    = calculate_function.calc_GHG_Max(year_now)

    # groupテーブルのadminのimoリストを取得
    rep_group = select.get_group(company_id, "admin")
    imo_list = ast.literal_eval(rep_group[0]["imo_list"]["S"])
    imo_list = list(set(imo_list))


    # 登録されたプーリンググループを取得
    res_pooling_group_list = select.get_pooling_table(company_and_year)

    # 取得したプーリンググループから、合計値管理用Dataを設定する。
    pooling_group_name_list = []
    pooling_group_datalist = []

    # プーリンググループの名前・合計値リストを作成
    for i in range(len(res_pooling_group_list)):
        res_group_name = res_pooling_group_list[i]["group_name"]["S"]

        pooling_group_name_list.append(res_group_name)

        data = {
            "group_name"           : res_group_name,
            "ytd_total_hfo"        : 0,
            "ytd_total_lfo"        : 0,
            "ytd_total_mdo"        : 0,
            "ytd_total_mgo"        : 0,
            "ytd_total_lng_oms"    : 0,
            "ytd_total_energy"     : 0,
            "eoy_total_hfo"        : 0,
            "eoy_total_lfo"        : 0,
            "eoy_total_mdo"        : 0,
            "eoy_total_mgo"        : 0,
            "eoy_total_lng_oms"    : 0,
            "eoy_total_lng_oss"    : 0,
            "eoy_total_lng_ods"    : 0,
            "eoy_total_lpg_b"      : 0,
            "eoy_total_lpg_p"      : 0,
            "eoy_total_h2_ng"      : 0,
            "eoy_total_nh3_ng"     : 0,
            "eoy_total_methanol_ng": 0,
            "eoy_total_nh3_ef"     : 0,
            "eoy_total_energy"     : 0
        }
        pooling_group_datalist.append(data)

    # print(f"作成したpooling_group_datalist:{(pooling_group_datalist)}")　←OK

    # 合計値用の変数を設定する
    count_ytd_borrowing_vessels = 0
    ytd_borrowing_cb            = 0
    count_ytd_banking_vessels   = 0
    ytd_banking_cb              = 0
    count_eoy_borrowing_vessels = 0
    eoy_borrowing_cb            = 0
    count_eoy_banking_vessels   = 0
    eoy_banking_cb              = 0

    print(f"imo_list:{imo_list}")
    # adminの全船リストをimoでループさせる
    for imo in imo_list:

        # 合計船数を加算
        count_total_vessels += 1

        # PooligGroup未所属フラグ
        not_grouped_flg = True

        # vessel nameを取得
        vessel_name = select.get_vessel_master(imo)[0]["VesselName"]["S"]

        # DBから取得したプーリンググループリストにレコードがある間
        if res_pooling_group_list:
            for pooling_group in res_pooling_group_list:
                group_name     = pooling_group["group_name"]["S"]
                group_imo_list = pooling_group["imo_list"]["S"]

                # そのプーリンググループの中に、該当imoが含まれる場合
                if imo in group_imo_list:

                    # 未所属フラグをOFF
                    not_grouped_flg = False

                    # 1船舶あたりの年間情報と実測データを算出する
                    if year_now == para_year:
                        # 当年分の処理の場合
                        ytd_grouped_vessel_info, ytd_lng, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, ytd_energy, eoy_grouped_vessel_info, eoy_hfo, eoy_lfo, eoy_mdo, eoy_mgo, eoy_lng_oms, eoy_lng_oss, eoy_lng_ods, eoy_lpg_p, eoy_lpg_b, eoy_h2_ng, eoy_nh3_ng, eoy_methanol_ng, eoy_nh3_ef, eoy_energy = make_ytd_record.make_recoed(imo, vessel_name, year_now, para_year, fuel_oil_type_info_list)
                    else:
                        # 過去年分の処理の場合
                        ytd_grouped_vessel_info, ytd_lng, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, ytd_energy, eoy_grouped_vessel_info, eoy_hfo, eoy_lfo, eoy_mdo, eoy_mgo, eoy_lng_oms, eoy_lng_oss, eoy_lng_ods, eoy_lpg_p, eoy_lpg_b, eoy_h2_ng, eoy_nh3_ng, eoy_methanol_ng, eoy_nh3_ef, eoy_energy = make_ytd_record.make_recoed_past(imo, vessel_name, para_year, fuel_oil_type_info_list)

                    print(f"imo:{(imo)}, ytd_grouped_vessel_info:{(ytd_grouped_vessel_info)}, eoy_grouped_vessel_info:{(ytd_grouped_vessel_info)}")
                    # グルーピングされている船の情報を出力用リストに追加する
                    for j in range(len(ytd_grouped_vessel_info)):

                        # sub_ytd_grouped_vessels_listに追加
                        append_data = [group_name, ytd_grouped_vessel_info[j]]
                        sub_ytd_grouped_vessels_list.append(append_data)

                        # Year to Dateのbanking, borrowingの確認をする
                        tmp_ytd_banking_cb   = ytd_grouped_vessel_info[j]["banking"]
                        tmp_ytd_borrowing_cb = ytd_grouped_vessel_info[j]["borrowing"]
                        if tmp_ytd_banking_cb > 0:
                            count_ytd_banking_vessels += 1
                            ytd_banking_cb            += tmp_ytd_banking_cb
                        # elif tmp_ytd_borrowing_cb > 0:
                        #     count_ytd_borrowing_vessels += 1
                        #     ytd_borrowing_cb            += tmp_ytd_borrowing_cb

                        # sub_eoy_grouped_vessels_listに追加
                        append_data = [group_name, eoy_grouped_vessel_info[j]]
                        sub_eoy_grouped_vessels_list.append(append_data)

                        # banking, borrowingの確認をする
                        tmp_eoy_banking_cb   = eoy_grouped_vessel_info[j]["banking"]
                        tmp_eoy_borrowing_cb = eoy_grouped_vessel_info[j]["borrowing"]
                        if tmp_eoy_banking_cb > 0:
                            count_eoy_banking_vessels += 1
                            eoy_banking_cb            += tmp_eoy_banking_cb
                        # elif tmp_eoy_borrowing_cb > 0:
                        #     count_eoy_borrowing_vessels += 1
                        #     eoy_borrowing_cb            += tmp_eoy_borrowing_cb

                        # ytd_all_vessels_listに追加
                        ytd_vessel_data = {
                            "imo"           : imo,
                            "vessel_name"   : vessel_name,
                            "operator"      : ytd_grouped_vessel_info[j]["operator"],
                            "year_to_date"  : ytd_grouped_vessel_info[j]["year_to_date"],
                            "last_year"     : ytd_grouped_vessel_info[j]["last_year"],
                            "total"         : ytd_grouped_vessel_info[j]["total"],
                            "penalty_factor": ytd_grouped_vessel_info[j]["penalty_factor"],
                            "group"         : group_name
                        }
                        ytd_all_vessels_list.append(ytd_vessel_data)

                    # 燃料消費量を足し合わせるグループを探す
                    list_index = 0
                    for pooling_group_data in pooling_group_datalist:
                            
                        if pooling_group_data["group_name"] == group_name:
                            print(f"ループ({(list_index+1)})  pooling_group_datalist[list_index]:{(pooling_group_datalist[list_index])}")

                            # プーリンググループの合計値（加算前）を取得する。
                            bk_ytd_total_hfo     = pooling_group_data["ytd_total_hfo"]
                            bk_ytd_total_lfo     = pooling_group_data["ytd_total_lfo"]
                            bk_ytd_total_mdo     = pooling_group_data["ytd_total_mdo"]
                            bk_ytd_total_mgo     = pooling_group_data["ytd_total_mgo"]
                            bk_ytd_total_lng_oms = pooling_group_data["ytd_total_lng_oms"]
                            bk_ytd_total_energy  = pooling_group_data["ytd_total_energy"]

                            bk_eoy_total_hfo         = pooling_group_data["eoy_total_hfo"]
                            bk_eoy_total_lfo         = pooling_group_data["eoy_total_lfo"]
                            bk_eoy_total_mdo         = pooling_group_data["eoy_total_mdo"]
                            bk_eoy_total_mgo         = pooling_group_data["eoy_total_mgo"]
                            bk_eoy_total_lng_oms     = pooling_group_data["eoy_total_lng_oms"]
                            bk_eoy_total_lng_oss     = pooling_group_data["eoy_total_lng_oss"]
                            bk_eoy_total_lng_ods     = pooling_group_data["eoy_total_lng_ods"]
                            bk_eoy_total_lpg_b       = pooling_group_data["eoy_total_lpg_b"]
                            bk_eoy_total_lpg_p       = pooling_group_data["eoy_total_lpg_p"]
                            bk_eoy_total_h2_ng       = pooling_group_data["eoy_total_h2_ng"]
                            bk_eoy_total_nh3_ng      = pooling_group_data["eoy_total_nh3_ng"]
                            bk_eoy_total_methanol_ng = pooling_group_data["eoy_total_methanol_ng"]
                            bk_eoy_total_nh3_ef      = pooling_group_data["eoy_total_nh3_ef"]
                            bk_eoy_total_energy      = pooling_group_data["eoy_total_energy"]

                            # プーリンググループの合計値を加算した値に書き換える。
                            pooling_group_data["ytd_total_hfo"]     = bk_ytd_total_hfo + ytd_hfo
                            print(f"bk_ytd_total_hfo:{(bk_ytd_total_hfo)}, ytd_hfo:{(ytd_hfo)}, pooling_group_data[ytd_total_hfo]:{(pooling_group_data["ytd_total_hfo"])}")

                            pooling_group_data["ytd_total_lfo"]     = bk_ytd_total_lfo + ytd_lfo
                            pooling_group_data["ytd_total_mdo"]     = bk_ytd_total_mdo + ytd_mdo
                            pooling_group_data["ytd_total_mgo"]     = bk_ytd_total_mgo + ytd_mgo
                            pooling_group_data["ytd_total_lng_oms"] = bk_ytd_total_lng_oms + ytd_lng
                            pooling_group_data["ytd_total_energy"]  = bk_ytd_total_energy + ytd_energy

                            pooling_group_data["eoy_total_hfo"]         = bk_eoy_total_hfo + ytd_hfo + eoy_hfo
                            print(f"bk_eoy_total_hfo:{(bk_eoy_total_hfo)}, ytd_hfo: {(ytd_hfo)}, eoy_hfo:{(eoy_hfo)}, pooling_group_data[eoy_total_hfo]:{(pooling_group_data["eoy_total_hfo"])}")

                            pooling_group_data["eoy_total_lfo"]         = bk_eoy_total_lfo + ytd_lfo + eoy_lfo
                            pooling_group_data["eoy_total_mdo"]         = bk_eoy_total_mdo + ytd_mdo + eoy_mdo
                            pooling_group_data["eoy_total_mgo"]         = bk_eoy_total_mgo + ytd_mgo + eoy_mgo
                            pooling_group_data["eoy_total_lng_oms"]     = bk_eoy_total_lng_oms + ytd_lng + eoy_lng_oms
                            pooling_group_data["eoy_total_lng_oss"]     = bk_eoy_total_lng_oss + eoy_lng_oss
                            pooling_group_data["eoy_total_lng_ods"]     = bk_eoy_total_lng_ods + eoy_lng_ods
                            pooling_group_data["eoy_total_lpg_p"]       = bk_eoy_total_lpg_p + eoy_lpg_p
                            pooling_group_data["eoy_total_lpg_b"]       = bk_eoy_total_lpg_b + eoy_lpg_b
                            pooling_group_data["eoy_total_h2_ng"]       = bk_eoy_total_h2_ng + eoy_h2_ng
                            pooling_group_data["eoy_total_nh3_ng"]      = bk_eoy_total_nh3_ng + eoy_nh3_ng
                            pooling_group_data["eoy_total_methanol_ng"] = bk_eoy_total_methanol_ng + eoy_methanol_ng
                            pooling_group_data["eoy_total_nh3_ef"]      = bk_eoy_total_nh3_ef + eoy_nh3_ef
                            pooling_group_data["eoy_total_energy"]      = bk_eoy_total_energy + ytd_energy + eoy_energy

                            # リストを更新する。
                            pooling_group_datalist[list_index] = pooling_group_data
                            list_index += 1

                            print(f"pooling_group_datalist:{(pooling_group_datalist)}")
                            
                            break

                        list_index += 1
                    
            # ----------- ループ終了 -----------

        # プーリンググループの登録が無い場合、またはグループに未所属のimoの場合
        if not_grouped_flg == True:
            # 1船舶あたりの年間情報と実測データを算出する
            if year_now == para_year:
                # 当年分の処理の場合
                ytd_not_grouped_vessel_info, ytd_lng, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, ytd_energy, eoy_not_grouped_vessel_info, eoy_hfo, eoy_lfo, eoy_mdo, eoy_mgo, eoy_lng_oms, eoy_lng_oss, eoy_lng_ods, eoy_lpg_p, eoy_lpg_b, eoy_h2_ng, eoy_nh3_ng, eoy_methanol_ng, eoy_nh3_ef, eoy_energy = make_ytd_record.make_recoed(imo, vessel_name, year_now, para_year, fuel_oil_type_info_list)
            else:
                # 過去年分の処理の場合
                ytd_not_grouped_vessel_info, ytd_lng, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, ytd_energy, eoy_not_grouped_vessel_info, eoy_hfo, eoy_lfo, eoy_mdo, eoy_mgo, eoy_lng_oms, eoy_lng_oss, eoy_lng_ods, eoy_lpg_p, eoy_lpg_b, eoy_h2_ng, eoy_nh3_ng, eoy_methanol_ng, eoy_nh3_ef, eoy_energy = make_ytd_record.make_recoed_past(imo, vessel_name, para_year, fuel_oil_type_info_list)

            # Year to Dateについて
            # グルーピングされていない船の情報をリストに追加する
            for ytd_vessel_info in ytd_not_grouped_vessel_info:
                # banking, borrowingの確認をする
                banking_cb   = ytd_vessel_info["banking"]
                borrowing_cb = ytd_vessel_info["borrowing"]
                if banking_cb > 0:
                    count_ytd_banking_vessels += 1
                    ytd_banking_cb            += banking_cb
                elif borrowing_cb > 0:
                    count_ytd_borrowing_vessels += 1
                    ytd_borrowing_cb            += borrowing_cb

                ytd_not_grouped_vessels_list.append(ytd_vessel_info)

                # ytd_all_vessels_listに追加
                ytd_vessel_data = {
                    "imo"           : imo,
                    "vessel_name"   : vessel_name,
                    "operator"      : ytd_vessel_info["operator"],
                    "year_to_date"  : round(ytd_vessel_info["year_to_date"], 1),
                    "last_year"     : round(ytd_vessel_info["last_year"], 1),
                    "total"         : round(ytd_vessel_info["total"], 1),
                    "penalty_factor": ytd_vessel_info["penalty_factor"],
                    "group"         : "-"
                }
                ytd_all_vessels_list.append(ytd_vessel_data)

                # グルーピングされていない船のCB、CB_COSTを足し合わせる。
                ytd_not_grouped_vessels_total_cb += ytd_vessel_info["total"]
                ytd_not_grouped_vessels_total_cb_cost += ytd_vessel_info["cost"]

            # End of Yearについて
            # グルーピングされていない船の情報をリストに追加する
            for vessel_info in eoy_not_grouped_vessel_info:

                # banking, borrowingの確認をする
                banking_cb   = vessel_info["banking"]
                borrowing_cb = vessel_info["borrowing"]
                if banking_cb > 0:
                    count_eoy_banking_vessels += 1
                    eoy_banking_cb            += banking_cb
                if borrowing_cb > 0:
                    count_eoy_borrowing_vessels += 1
                    eoy_borrowing_cb            += borrowing_cb

                eoy_not_grouped_vessels_list.append(vessel_info)

                # グルーピングされていない船のCB、CB_COSTを足し合わせる。
                eoy_not_grouped_vessels_total_cb += vessel_info["total"]
                eoy_not_grouped_vessels_total_cb_cost += vessel_info["cost"]

    # グルーピングされていない船たちの合計CB、CB costを設定する
    ytd_not_grouped_total = {
        "total_cb"     : str(round(ytd_not_grouped_vessels_total_cb, 1)),
        "total_cb_cost": str(round(ytd_not_grouped_vessels_total_cb_cost)),
    }
    eoy_not_grouped_total = {
        "total_cb"     : str(round(eoy_not_grouped_vessels_total_cb, 1)),
        "total_cb_cost": str(round(eoy_not_grouped_vessels_total_cb_cost)),
    }

    # ytd_not_grouped_vessels_listのソート
    ytd_not_grouped_vessels_list = sorted(ytd_not_grouped_vessels_list, key=lambda x:x["vessel_name"])
    # eoy_not_grouped_vessels_listのソート
    eoy_not_grouped_vessels_list = sorted(eoy_not_grouped_vessels_list, key=lambda x:x["vessel_name"])

    print(f"pooling_group_datalist:{(pooling_group_datalist)}")
    # プーリングされている船舶のimo+operator別レコードを、グループ毎に整理する
    # プーリンググループ名でループ
    print(f"pooling_group_name_listループ開始")
    for pooling_group_name in pooling_group_name_list:

        print(f"pooling_group_name_listループ中① グループ名：{(pooling_group_name)}")

        ytd_this_group_vessels = []
        eoy_this_group_vessels = []
        index                  = 0
        ytd_grouped_vessels_total_cb     = 0
        ytd_grouped_vessels_total_cb_cost = 0

        # グルーピングされている船たちの合計CB、CB costを算出する

        # pooling_group_datalistからgroup_nameが一致するレコードのインデックスを取得する
        for pooling_group_data in pooling_group_datalist:
            if pooling_group_data["group_name"] == pooling_group_name:
                index = pooling_group_datalist.index(pooling_group_data)
                print(f"pooling_group_name_listループ中②  {(pooling_group_name)}のindex:{(index)}")

        # プーリンググループのytd燃料消費量を取得する
        # Year to Date
        print(f"pooling_group_name_listループ中③  {(pooling_group_name)}のYtD fuel total:{(pooling_group_datalist[index])}")
        ytd_grouped_vessels_total_lng_oms = pooling_group_datalist[index]["ytd_total_lng_oms"]
        ytd_grouped_vessels_total_hfo     = pooling_group_datalist[index]["ytd_total_hfo"]
        ytd_grouped_vessels_total_lfo     = pooling_group_datalist[index]["ytd_total_lfo"]
        ytd_grouped_vessels_total_mdo     = pooling_group_datalist[index]["ytd_total_mdo"]
        ytd_grouped_vessels_total_mgo     = pooling_group_datalist[index]["ytd_total_mgo"]
        ytd_grouped_vessels_total_energy  = pooling_group_datalist[index]["ytd_total_energy"]

        ytd_grouped_vessels_total_GHG = calculate_function.calc_GHG_Actual(0, ytd_grouped_vessels_total_lng_oms, 0, ytd_grouped_vessels_total_hfo, ytd_grouped_vessels_total_lfo, ytd_grouped_vessels_total_mdo, ytd_grouped_vessels_total_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
        ytd_grouped_vessels_total_cb  = (GHG_Max - ytd_grouped_vessels_total_GHG) * ytd_grouped_vessels_total_energy

        # 各プーリンググループのCBを足し合わせる。
        ytd_total_cb += (ytd_grouped_vessels_total_cb / 1000000)

        ytd_grouped_vessels_total_cb_cost = 0
        if ytd_grouped_vessels_total_cb < 0:
            ytd_grouped_vessels_total_cb_cost = abs(ytd_grouped_vessels_total_cb) * 2400 / (ytd_grouped_vessels_total_GHG * 41000)
            ytd_total_cb_cost += ytd_grouped_vessels_total_cb_cost

        # Year to Date
        for grouped_vessel in sub_ytd_grouped_vessels_list:
            vessels_group_name = grouped_vessel[0]

            # 外ループのグループ名と船舶の所属するグループ名が一致する時
            if vessels_group_name == pooling_group_name:
                append_data = {
                    "vessel_name"    : grouped_vessel[1]["vessel_name"],
                    "operator"       : grouped_vessel[1]["operator"],
                    "distance"       : grouped_vessel[1]["distance"],
                    "foc"            : grouped_vessel[1]["foc"],
                    "year_to_date"   : grouped_vessel[1]["year_to_date"],
                    "last_year"      : grouped_vessel[1]["last_year"],
                    "total"          : grouped_vessel[1]["year_to_date"] + grouped_vessel[1]["last_year"],
                    "penalty_factor" : grouped_vessel[1]["penalty_factor"]
                }
                ytd_this_group_vessels.append(append_data)

        # ytd_this_group_vesselsをソート
        if len(ytd_this_group_vessels) != 0:
            ytd_this_group_vessels = sorted(ytd_this_group_vessels, key=lambda x:x["vessel_name"])

        ytd_group_list = {
            "group_name"   : pooling_group_name,
            "total_cb"     : str(round(ytd_grouped_vessels_total_cb / 1000000, 1)),
            "total_cb_cost": str(round(ytd_grouped_vessels_total_cb_cost)),
            "GroupedVesselList": ytd_this_group_vessels
        }
        ytd_pooling_group_list.append(ytd_group_list)

        # End of Year
        eoy_grouped_vessels_total_hfo         = pooling_group_datalist[index]["eoy_total_hfo"]
        eoy_grouped_vessels_total_lfo         = pooling_group_datalist[index]["eoy_total_lfo"]
        eoy_grouped_vessels_total_mdo         = pooling_group_datalist[index]["eoy_total_mdo"]
        eoy_grouped_vessels_total_mgo         = pooling_group_datalist[index]["eoy_total_mgo"]
        eoy_grouped_vessels_total_lng_oms     = pooling_group_datalist[index]["eoy_total_lng_oms"]
        eoy_grouped_vessels_total_lng_oss     = pooling_group_datalist[index]["eoy_total_lng_oss"]
        eoy_grouped_vessels_total_lng_ods     = pooling_group_datalist[index]["eoy_total_lng_ods"]
        eoy_grouped_vessels_total_lpg_p       = pooling_group_datalist[index]["eoy_total_lpg_p"]
        eoy_grouped_vessels_total_lpg_b       = pooling_group_datalist[index]["eoy_total_lpg_b"]
        eoy_grouped_vessels_total_h2_ng       = pooling_group_datalist[index]["eoy_total_h2_ng"]
        eoy_grouped_vessels_total_nh3_ng      = pooling_group_datalist[index]["eoy_total_nh3_ng"]
        eoy_grouped_vessels_total_methanol_ng = pooling_group_datalist[index]["eoy_total_methanol_ng"]
        eoy_grouped_vessels_total_nh3_ef      = pooling_group_datalist[index]["eoy_total_nh3_ef"]
        eoy_grouped_vessels_total_energy      = pooling_group_datalist[index]["eoy_total_energy"]

        print(f"eoy_grouped_vessels_total_hfo:{(eoy_grouped_vessels_total_hfo)}, eoy_grouped_vessels_total_lfo:{(eoy_grouped_vessels_total_lfo)}")
        eoy_grouped_vessels_total_GHG = calculate_function.calc_GHG_Actual(eoy_grouped_vessels_total_lng_ods, eoy_grouped_vessels_total_lng_oms, eoy_grouped_vessels_total_lng_oss, eoy_grouped_vessels_total_hfo, eoy_grouped_vessels_total_lfo, eoy_grouped_vessels_total_mdo, eoy_grouped_vessels_total_mgo, eoy_grouped_vessels_total_lpg_p, eoy_grouped_vessels_total_lpg_b, eoy_grouped_vessels_total_nh3_ng, eoy_grouped_vessels_total_nh3_ef, eoy_grouped_vessels_total_methanol_ng, eoy_grouped_vessels_total_h2_ng, fuel_oil_type_info_list)
        eoy_grouped_vessels_total_cb  = (GHG_Max - eoy_grouped_vessels_total_GHG) * eoy_grouped_vessels_total_energy

        # 各プーリンググループのCBを足し合わせる。
        eoy_total_cb += (eoy_grouped_vessels_total_cb / 1000000)

        eoy_grouped_vessels_total_cb_cost = 0
        if eoy_grouped_vessels_total_cb < 0:
            eoy_grouped_vessels_total_cb_cost = abs(eoy_grouped_vessels_total_cb) * 2400 / (eoy_grouped_vessels_total_GHG * 41000)
            eoy_total_cb_cost += eoy_grouped_vessels_total_cb_cost

        # End of Year
        for grouped_vessel in sub_eoy_grouped_vessels_list:
            vessels_group_name = grouped_vessel[0]

            # 外ループのグループ名と船舶の所属するグループ名が一致する時
            if vessels_group_name == pooling_group_name:
                append_data = {
                    "vessel_name"     : grouped_vessel[1]["vessel_name"],
                    "operator"        : grouped_vessel[1]["operator"],
                    "distance"        : grouped_vessel[1]["distance"],
                    "foc"             : grouped_vessel[1]["foc"],
                    "end_of_year"     : grouped_vessel[1]["end_of_year"],
                    "last_year"       : grouped_vessel[1]["last_year"],
                    "total"           : grouped_vessel[1]["end_of_year"] + grouped_vessel[1]["last_year"],
                    "penalty_factor"  : grouped_vessel[1]["penalty_factor"]
                }
                eoy_this_group_vessels.append(append_data)

        # eoy_this_group_vesselsをソート
        if len(eoy_this_group_vessels) != 0:
            eoy_this_group_vessels = sorted(eoy_this_group_vessels, key=lambda x:x["vessel_name"])

        eoy_group_list = {
            "group_name"   : pooling_group_name,
            "total_cb"     : str(round(eoy_grouped_vessels_total_cb / 1000000, 1)),
            "total_cb_cost": str(round(eoy_grouped_vessels_total_cb_cost)),
            "GroupedVesselList": eoy_this_group_vessels
        }
        eoy_pooling_group_list.append(eoy_group_list)

    

    # グルーピング済みと未所属のcb、cb_costについて、Year to Dateの合計値を算出する
    ytd_total_cb += ytd_not_grouped_vessels_total_cb
    ytd_total_cb_cost += ytd_not_grouped_vessels_total_cb_cost

    # グルーピング済みと未所属のcb、cb_costについて、End of Yearの合計値を算出する
    eoy_total_cb += eoy_not_grouped_vessels_total_cb
    eoy_total_cb_cost += eoy_not_grouped_vessels_total_cb_cost

    ytd_total_list = {
        "total_cb"         : str(round(ytd_total_cb, 1)),
        "total_cb_cost"    : str(round(ytd_total_cb_cost)),
        "pooling_group"    : str(len(pooling_group_name_list)),
        "banking_vessels"  : str(count_ytd_banking_vessels),
        "banking_cb"       : str(ytd_banking_cb),
        "borrowing_vessels": str(count_ytd_borrowing_vessels),
        "borrowing_cb"     : str(ytd_borrowing_cb)
    }
    eoy_total_list = {
        "total_cb"         : str(round(eoy_total_cb, 1)),
        "total_cb_cost"    : str(round(eoy_total_cb_cost)),
        "pooling_group"    : str(len(pooling_group_name_list)),
        "banking_vessels"  : str(count_eoy_banking_vessels),
        "banking_cb"       : str(eoy_banking_cb),
        "borrowing_vessels": str(count_eoy_borrowing_vessels),
        "borrowing_cb"     : str(eoy_borrowing_cb)
    }

    # ytd_all_vessels_listをソート
    ytd_all_vessels_list = sorted(ytd_all_vessels_list, key=lambda x:x["vessel_name"])

    # 返却用データセットを設定する
    datas = {
        "total_vessels"               : count_total_vessels,
        "ytd_total_list"              : ytd_total_list,
        "eoy_total_list"              : eoy_total_list,
        "ytd_pooling_group_list"      : ytd_pooling_group_list,
        "ytd_not_grouped_total"       : ytd_not_grouped_total,
        "ytd_not_grouped_vessels_list": ytd_not_grouped_vessels_list,
        "eoy_pooling_group_list"      : eoy_pooling_group_list,
        "eoy_not_grouped_total"       : eoy_not_grouped_total,
        "eoy_not_grouped_vessels_list": eoy_not_grouped_vessels_list,
        "ytd_all_vessels_list"        : ytd_all_vessels_list
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
