
import ast
from datetime import datetime
import json
import math
import re
import copy

from dynamodb import select
from Util import Util
from calculate import calculate_function
from vesselinfo import make_eoy_record

def make_recoed(eua_price, imo, year, fuel_oil_type_list, vessel_master):

    # 必要な変数・リストを作成
    voyage_flag = "0"
    speed_flag  = "0"
    penalty_factor = 1.0

    operator_total_list      = []
    ytd_dataset_list         = []

    # 返却用データセット
    record_data_list = []

    # imoとyear（現在年）をキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_year(imo, year)

    # FOCFormulas取得
    res_foc_formulas = select.get_foc_formulas(imo)

    # シミュレーション用テーブルを取得
    simulation_plan_voyage_list = select.get_simulation_voyage(imo, year)
    simulation_plan_speed       = select.get_simulation_speed(imo, year)

    # どちらのSimulationを使用しているか確認
    if simulation_plan_voyage_list:
        voyage_flag = simulation_plan_voyage_list[0]["flag"]["S"] if "flag" in simulation_plan_voyage_list[0] else "0"
    
    # speed-planのシミュレーション使用フラグ確認
    if simulation_plan_speed:
        speed_flag = simulation_plan_speed[0]["flag"]["S"] if "flag" in simulation_plan_speed[0] else "0"

    # シミュレーションプラン管理用リスト
    ytd_exist_voyage_list     = []
    ytd_not_exist_voyage_list = []
    ytd_exist_speed_list      = []
    ytd_not_exist_speed_list  = []

    # 各種燃料の消費量と、消費エネルギーの合計値用変数を設定する。
    ytd_lng    = 0
    ytd_hfo    = 0
    ytd_lfo    = 0
    ytd_mdo    = 0
    ytd_mgo    = 0
    energy     = 0

    # 今年分のyear-totalレコード分ループ
    for rec in total_year_total_list:

        # オペレータ
        operator = rec["year_and_ope"]["S"][4:50]

        print(f"total_year_total_list:{total_year_total_list}")

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        lng       = float(rec["total_lng"]["S"])
        hfo       = float(rec["total_hfo"]["S"])
        lfo       = float(rec["total_lfo"]["S"])
        mdo       = float(rec["total_mdo"]["S"])
        mgo       = float(rec["total_mgo"]["S"])
        foc       = float(rec["total_foc"]["S"])
        eu_actual_foc = float(rec["eu_actual_foc"]["S"])
        distance  = float(rec["distance"]["S"])
        eua       = float(rec["eua"]["S"])
        cb        = float(rec["cb"]["S"])

        # EUAからEUA costを算出する
        eua_cost  = round(eua) * int(eua_price)

        # 消費量エネルギー（EU Rate考慮済）を算出する
        GHG_Actual = calculate_function.calc_GHG_Actual(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
        energy     = calculate_function.calc_energy(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)

        # 必要な計算を行う
        total_cb        = cb

        cb_cost = 0
        if total_cb < 0 and GHG_Actual != 0:
            cb_cost    = abs(float(total_cb)) * 2400 / (GHG_Actual * 41000) 

        ytd_dataset = {
            "imo"            : imo,
            "operator"       : operator,
            "distance"       : distance,
            "foc"            : eu_actual_foc,
            "eua"            : eua,
            "eua_cost"       : eua_cost,
            "cb"             : total_cb,
            "cb_cost"        : cb_cost
        }
        ytd_dataset_list.append(ytd_dataset)

        # 合計用変数に加算する。
        ytd_lng    += lng
        ytd_hfo    += hfo
        ytd_lfo    += lfo
        ytd_mdo    += mdo
        ytd_mgo    += mgo
        # ytd_energy += energy

        # シミュレーション結果を反映したEnd of Yearのデータを作成する。

        # voyage-planのシミュレーション使用フラグ確認
        if voyage_flag == "1":

            # ループ用に保持しておく
            keep_simulation_plan_voyage_list = simulation_plan_voyage_list.copy()

            # voyageのオペレーターを確認する
            for simulation_voyage in keep_simulation_plan_voyage_list:
                simulation_operator = simulation_voyage["operator"]["S"]

                # 基準のyear-totalレコードのオペレーターと一致する場合
                if simulation_operator == operator:
                    # 実測データが存在するリストに追加
                    ytd_exist_voyage_list.append(simulation_voyage)
                    # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                    simulation_plan_voyage_list.remove(simulation_voyage)
        
        # speed-planのシミュレーション使用フラグ確認
        if speed_flag == "1":

            # オペレーターを確認する
            simulation_operator = simulation_plan_speed[0]["operator"]["S"]

            # 基準のyear-totalレコードのオペレーターと一致する場合
            if simulation_operator == operator:
                # 実測データが存在するリストに追加
                ytd_exist_speed_list.append(simulation_plan_speed[0])
                # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                simulation_plan_speed.remove(simulation_plan_speed[0])
       
        # シミュレーションリストの処理
        # 実測データ有り かつ voyage-planの場合
        if len(ytd_exist_voyage_list) > 0:
            # VoyagePlanのシミュレーション実施
            eoy_vessel_data_list = make_eoy_record.make_voyage_plans_data(imo, rec, ytd_exist_voyage_list, res_foc_formulas, fuel_oil_type_list, energy, year, total_year_total_list)

            if len(eoy_vessel_data_list) > 0:
                for i in range(len(eoy_vessel_data_list)):
                # operatorが一致するytdデータと合わせて、データセットを作成
                    dataset = {
                        "imo"                : ytd_dataset["imo"],
                        "vessel_name"        : vessel_master["VesselName"]["S"],
                        "operator"           : ytd_dataset["operator"],
                        "ytd_distance"       : round(ytd_dataset["distance"]),
                        "ytd_foc"            : round(ytd_dataset["foc"]),
                        "ytd_eua"            : round(ytd_dataset["eua"]),
                        "ytd_eua_cost"       : round(ytd_dataset["eua"]) * int(eua_price),
                        "ytd_cb"             : round(ytd_dataset["cb"] / 1000000, 1),
                        "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                        "eoy_distance"       : round(eoy_vessel_data_list[i]["eoy_distance"]),
                        "eoy_foc"            : round(eoy_vessel_data_list[i]["eoy_foc"]),
                        "eoy_eua"            : round(eoy_vessel_data_list[i]["eoy_eua"]),
                        "eoy_eua_cost"       : round(float(eoy_vessel_data_list[i]["eoy_eua"])) * int(eua_price),
                        "eoy_cb"             : round(float(eoy_vessel_data_list[i]["eoy_cb"]) / 1000000, 1),
                        "eoy_cb_cost"        : round(eoy_vessel_data_list[i]["eoy_cb_cost"])
                    }
                    record_data_list.append(dataset)
            else:
                # 全てのレグが期間外の場合は、ytdと同じ値をeoyに設定
                dataset = {
                    "imo"                : ytd_dataset["imo"],
                    "vessel_name"        : vessel_master["VesselName"]["S"],
                    "operator"           : ytd_dataset["operator"],
                    "ytd_distance"       : round(ytd_dataset["distance"]),
                    "ytd_foc"            : round(ytd_dataset["foc"]),
                    "ytd_eua"            : round(ytd_dataset["eua"]),
                    "ytd_eua_cost"       : round(ytd_dataset["eua"]) * int(eua_price),
                    "ytd_cb"             : round(ytd_dataset["cb"] / 1000000, 1),
                    "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                    "eoy_distance"       : round(ytd_dataset["distance"]),
                    "eoy_foc"            : round(ytd_dataset["foc"]),
                    "eoy_eua"            : round(ytd_dataset["eua"]),
                    "eoy_eua_cost"       : round(float(ytd_dataset["eua"])) * int(eua_price),
                    "eoy_cb"             : round(float(ytd_dataset["cb"]) / 1000000, 1),
                    "eoy_cb_cost"        : round(ytd_dataset["cb_cost"])
                }
                record_data_list.append(dataset)

        # 実測データ有りspeed-plan
        elif len(ytd_exist_speed_list) > 0:
            eoy_vessel_data = make_eoy_record.make_speed_plans_data(rec, ytd_exist_speed_list, res_foc_formulas, fuel_oil_type_list, energy, year, total_year_total_list)

            dataset = {
                "imo"                : ytd_dataset["imo"],
                "vessel_name"        : vessel_master["VesselName"]["S"],
                "operator"           : ytd_dataset["operator"],
                "ytd_distance"       : round(ytd_dataset["distance"]),
                "ytd_foc"            : round(ytd_dataset["foc"]),
                "ytd_eua"            : round(ytd_dataset["eua"]),
                "ytd_eua_cost"       : round(ytd_dataset["eua"]) * int(eua_price),
                "ytd_cb"             : round(ytd_dataset["cb"] / 1000000, 1),
                "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                "eoy_distance"       : round(eoy_vessel_data["eoy_distance"]),
                "eoy_foc"            : round(eoy_vessel_data["eoy_foc"]),
                "eoy_eua"            : round(eoy_vessel_data["eoy_eua"]),
                "eoy_eua_cost"       : round(float(eoy_vessel_data["eoy_eua"])) * int(eua_price),
                "eoy_cb"             : round(float(eoy_vessel_data["eoy_cb"]) / 1000000, 1),
                "eoy_cb_cost"        : round(eoy_vessel_data["eoy_cb_cost"])
            }
            record_data_list.append(dataset)

        # 実測データあり かつ Simulationなし
        else:
            # Simulation無の場合は、ytdと同じ値をeoyに設定
            dataset = {
                "imo"                : ytd_dataset["imo"],
                "vessel_name"        : vessel_master["VesselName"]["S"],
                "operator"           : ytd_dataset["operator"],
                "ytd_distance"       : round(ytd_dataset["distance"]),
                "ytd_foc"            : round(ytd_dataset["foc"]),
                "ytd_eua"            : round(ytd_dataset["eua"]),
                "ytd_eua_cost"       : round(ytd_dataset["eua"]) * int(eua_price),
                "ytd_cb"             : round(ytd_dataset["cb"] / 1000000, 1),
                "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                "eoy_distance"       : round(ytd_dataset["distance"]),
                "eoy_foc"            : round(ytd_dataset["foc"]),
                "eoy_eua"            : round(ytd_dataset["eua"]),
                "eoy_eua_cost"       : round(float(ytd_dataset["eua"])) * int(eua_price),
                "eoy_cb"             : round(float(ytd_dataset["cb"]) / 1000000, 1),
                "eoy_cb_cost"        : round(ytd_dataset["cb_cost"])
            }
            record_data_list.append(dataset)
    
    # year-totalループ終了後、各シミュレーションリストに残っているものは、実測データ無しオペレーター
    if voyage_flag == "1" and len(simulation_plan_voyage_list) > 0:
        for simulation_voyage in simulation_plan_voyage_list:
            # 実測データが存在しないリストに追加
            ytd_not_exist_voyage_list.append(simulation_voyage)
    if speed_flag == "1" and len(simulation_plan_speed) > 0:
        ytd_not_exist_speed_list.append(simulation_plan_speed[0])
    
    # 実測データ無しvoyage-plan
    if len(ytd_not_exist_voyage_list) > 0:

        # VoyagePlanのシミュレーション実施
        eoy_vessel_data_list = make_eoy_record.make_voyage_plans_data(imo, None, ytd_not_exist_voyage_list, res_foc_formulas, fuel_oil_type_list, 0, year, total_year_total_list)
        print(f"戻り値eoy_vessel_data_list:{(eoy_vessel_data_list)}")
        print(f"imo:{(imo)} len(eoy_vessel_data_list):{(len(eoy_vessel_data_list))}")

        if len(eoy_vessel_data_list) > 0:
            print(f"eoy_vessel_data_list:{(eoy_vessel_data_list)}")
            print(f"eoy_vessel_data_listは{(len(eoy_vessel_data_list))}オペある")
            for i in range(len(eoy_vessel_data_list)):
                # ytdは0、eoyはSimulation結果を設定
                print(f"eoy_vessel_data_list[{(i)}][operator]:{(eoy_vessel_data_list[i]["operator"])}")
                dataset = {
                    "imo"                : imo,
                    "vessel_name"        : vessel_master["VesselName"]["S"],
                    "operator"           : eoy_vessel_data_list[i]["operator"],
                    "ytd_distance"       : "0",
                    "ytd_foc"            : "0",
                    "ytd_eua"            : "0",
                    "ytd_eua_cost"       : "0",
                    "ytd_cb"             : "0",
                    "ytd_cb_cost"        : "0",
                    "eoy_distance"       : round(eoy_vessel_data_list[i]["eoy_distance"]),
                    "eoy_foc"            : round(eoy_vessel_data_list[i]["eoy_foc"]),
                    "eoy_eua"            : round(eoy_vessel_data_list[i]["eoy_eua"]),
                    "eoy_eua_cost"       : round(float(eoy_vessel_data_list[i]["eoy_eua"])) * int(eua_price),
                    "eoy_cb"             : round(float(eoy_vessel_data_list[i]["eoy_cb"]) / 1000000, 1),
                    "eoy_cb_cost"        : round(eoy_vessel_data_list[i]["eoy_cb_cost"])
                }
                record_data_list.append(dataset)

    # 実測データ無しspeed-plan
    if len(ytd_not_exist_speed_list) > 0:

        eoy_vessel_data = make_eoy_record.make_speed_plans_data(None, ytd_not_exist_speed_list, res_foc_formulas, fuel_oil_type_list, 0, year, total_year_total_list)

        # ytdは0、eoyはSimulation結果を設定
        dataset = {
            "imo"                : imo,
            "vessel_name"        : vessel_master["VesselName"]["S"],
            "operator"           : ytd_not_exist_speed_list[0]["operator"]["S"],
            "ytd_distance"       : "0",
            "ytd_foc"            : "0",
            "ytd_eua"            : "0",
            "ytd_eua_cost"       : "0",
            "ytd_cb"             : "0",
            "ytd_cb_cost"        : "0",
            "eoy_distance"       : round(eoy_vessel_data["eoy_distance"]),
            "eoy_foc"            : round(eoy_vessel_data["eoy_foc"]),
            "eoy_eua"            : round(eoy_vessel_data["eoy_eua"]),
            "eoy_eua_cost"       : round(float(eoy_vessel_data["eoy_eua"])) * int(eua_price),
            "eoy_cb"             : round(float(eoy_vessel_data["eoy_cb"]) / 1000000, 1),
            "eoy_cb_cost"        : round(eoy_vessel_data["eoy_cb_cost"])
        }
        record_data_list.append(dataset)

    # 実測値なし、シミュレーションもなし
    if len(record_data_list) == 0:
        dataset = {
            "imo"                : imo,
            "vessel_name"        : vessel_master["VesselName"]["S"],
            "operator"           : "",
            "ytd_distance"       : "0",
            "ytd_foc"            : "0",
            "ytd_eua"            : "0",
            "ytd_eua_cost"       : "0",
            "ytd_cb"             : "0",
            "ytd_cb_cost"        : "0",
            "eoy_distance"       : "0",
            "eoy_foc"            : "0",
            "eoy_eua"            : "0",
            "eoy_eua_cost"       : "0",
            "eoy_cb"             : "0",
            "eoy_cb_cost"        : "0"
        }
        record_data_list.append(dataset)
    
    return record_data_list

def make_recoed_past(eua_price, imo, year, fuel_oil_type_list, vessel_master):

    # 必要な変数・リストを作成
    # 返却用データセット
    record_data_list = []

    # imoとyear（現在年）をキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_year(imo, year)

    # 今年分のyear-totalレコード分ループ
    for rec in total_year_total_list:

        operator = rec["year_and_ope"]["S"][4:50]

        print(f"total_year_total_list:{total_year_total_list}")

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        operator  = rec["year_and_ope"]["S"][4:50]
        lng       = float(rec["total_lng"]["S"])
        hfo       = float(rec["total_hfo"]["S"])
        lfo       = float(rec["total_lfo"]["S"])
        mdo       = float(rec["total_mdo"]["S"])
        mgo       = float(rec["total_mgo"]["S"])
        foc       = float(rec["total_foc"]["S"])
        eu_actual_foc = float(rec["eu_actual_foc"]["S"])
        distance  = float(rec["distance"]["S"])
        eua       = float(rec["eua"]["S"])
        cb        = float(rec["cb"]["S"])

        # EUAからEUA costを算出する
        eua_cost  = round(eua) * int(eua_price)

        # 必要な計算を行う
        GHG_Actual = calculate_function.calc_GHG_Actual(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
        total_cb        = cb

        cb_cost = 0
        if total_cb < 0 and GHG_Actual != 0:
            cb_cost = abs(total_cb) * 2400 / (GHG_Actual * 41000)

        # 全てゼロのytdデータを合わせて、データセットを作成
        dataset = {
            "imo"                : rec["imo"]["S"],
            "vessel_name"        : vessel_master["VesselName"]["S"],
            "operator"           : operator,
            "ytd_distance"       : "",
            "ytd_foc"            : "",
            "ytd_eua"            : "",
            "ytd_eua_cost"       : "",
            "ytd_cb"             : "",
            "ytd_cb_cost"        : "",
            "eoy_distance"       : str(round(distance)),
            "eoy_foc"            : str(round(eu_actual_foc)),
            "eoy_eua"            : str(round(eua)),
            "eoy_eua_cost"       : str(round(eua_cost)),
            "eoy_cb"             : str(round(total_cb / 1000000, 1)),
            "eoy_cb_cost"        : str(round(cb_cost))
        }
        record_data_list.append(dataset)

    if len(record_data_list) == 0:
        # yearテーブルが取れない場合（その年の航行実績なしの場合）、ALL0のデータをセット
        dataset = {
            "imo"                : imo,
            "vessel_name"        : vessel_master["VesselName"]["S"],
            "operator"           : "",
            "ytd_distance"       : "",
            "ytd_foc"            : "",
            "ytd_eua"            : "",
            "ytd_eua_cost"       : "",
            "ytd_cb"             : "",
            "ytd_cb_cost"        : "",
            "eoy_distance"       : "0",
            "eoy_foc"            : "0",
            "eoy_eua"            : "0",
            "eoy_eua_cost"       : "0",
            "eoy_cb"             : "0",
            "eoy_cb_cost"        : "0"
        }
        record_data_list.append(dataset)

    dataset = record_data_list

    return dataset