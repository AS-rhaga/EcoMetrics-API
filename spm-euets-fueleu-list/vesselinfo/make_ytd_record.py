
import ast
from datetime import datetime
import json
import math
import re

from dynamodb import select
from Util import Util
from calculate import calculate_function
from vesselinfo import make_eoy_record

def make_recoed(eua_price, imo, year, fuel_oil_type_list, vessel_master):

    # 必要な変数・リストを作成
    last_year = 0
    voyage_flag = "0"
    speed_flag  = "0"
    penalty_factor = 1.0

    thisyear_year_total_list = []
    operator_total_list      = []
    ytd_dataset_list         = []

    # 返却用データセット
    record_data_list = []

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_imo(imo)

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

    # 同一imoのyear-totalリストでループ
    for year_rec in total_year_total_list:

        # 処理対象年のレコードを抽出。
        if year_rec["year_and_ope"]["S"][0:4] == year:
            thisyear_year_total_list.append(year_rec)

        # operator = year_rec["year_and_ope"]["S"][4:50]

        # # 現時点ではオペレーターは複数ないので飛ばす
        # # if operator not in operator_list : # 上限は適当
        #     # オペレーター毎に振り分ける
        #     # operator_list.append(operator)
        #     # index = operator_list.index(operator)

        #     # operator_total_list.append([])
        # operator_total_list.append(year_rec)

    # 各種燃料の消費量と、消費エネルギーの合計値用変数を設定する。
    ytd_lng    = 0
    ytd_hfo    = 0
    ytd_lfo    = 0
    ytd_mdo    = 0
    ytd_mgo    = 0
    # ytd_energy = 0

    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:

        # オペレータ
        operator = rec["year_and_ope"]["S"][4:50]
        
        # 昨年分のレコードを入れるリスト
        last_year_rec = []

        print(f"total_year_total_list:{total_year_total_list}")
        # 同一imoのyear-totalリストでループ
        for year_rec in total_year_total_list:

            tmp_operator = year_rec["year_and_ope"]["S"][4:50]

            # 同一オペレータのレコードを抽出
            if tmp_operator == operator:
                operator_total_list.append(year_rec)

                # 西暦部分の確認、昨年のレコードであれば保持しておく。
                tmp_year = year_rec["year_and_ope"]["S"][0:4]
                if tmp_year == str(int(year) - 1):
                    last_year_rec = year_rec

        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0
        year_count = 0

        for operator_rec in operator_total_list:
            
            # 先頭要素はスキップ（先頭要素は今年のレコード、前年以前を見たいため、スキップで良い）
            if year_count == 0:
                year_count += 1
                continue

            # １年ずつさかのぼる（年が飛んだ時点で確認不要のためbreak）
            if operator_rec["year_and_ope"]["S"][0:4] == str(int(year) - year_count):
                # 罰金フラグの確認
                fine_flag = operator_rec["fine_flag"]["S"]

                if fine_flag == "1":
                    consecutive_years += 1
                else:
                    break
            else:
                break
            
            year_count += 1


        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"])
            last_year_borrowing = float(last_year_rec["borrowing"]["S"] if "borrowing" in last_year_rec else "0")

            if last_year_borrowing > 0:
                last_year = last_year_borrowing * (-1.1)
            elif last_year_banking > 0:
                last_year = last_year_banking
            else:
                last_year = 0

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        lng       = float(rec["total_lng"]["S"])
        hfo       = float(rec["total_hfo"]["S"])
        lfo       = float(rec["total_lfo"]["S"])
        mdo       = float(rec["total_mdo"]["S"])
        mgo       = float(rec["total_mgo"]["S"])
        distance  = float(rec["distance"]["S"])
        eua       = float(rec["eua"]["S"])
        cb        = float(rec["cb"]["S"])
        banking   = float(rec["banking"]["S"])
        borrowing = float(rec["borrowing"]["S"] if "borrowing" in last_year_rec else "0")

        # EUAからEUA costを算出する
        eua_cost  = eua * float(eua_price)

        # CBから消費量エネルギー（EU Rate考慮済）を算出する
        GHG_Max    = calculate_function.calc_GHG_Max(year)
        GHG_Actual = calculate_function.calc_GHG_Actual(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
        energy          = cb / (GHG_Max - GHG_Actual)

        # 必要な計算を行う
        foc             = lng + hfo + lfo + mdo + mgo
        total_cb        = cb + borrowing + banking + last_year
        penalty_factor  = (consecutive_years) / 10 + 1
        print(f"penalty_factor:{penalty_factor}")
        print(f"consecutive_years:{consecutive_years}")

        cb_cost = 0
        if cb < 0:
            cb_cost = abs(cb) * 2400 * penalty_factor / (GHG_Actual * 41000)

        ytd_dataset = {
            "imo"            : imo,
            "operator"       : operator,
            "distance"       : distance,
            "foc"            : foc,
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

            # voyageのオペレーターを確認する
            for simulation_voyage in simulation_plan_voyage_list:
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
            eoy_vessel_data = make_eoy_record.make_voyage_plans_data(rec, ytd_exist_voyage_list, res_foc_formulas, fuel_oil_type_list, energy, penalty_factor)

            # operatorが一致するytdデータと合わせて、データセットを作成
            dataset = {
                "imo"                : ytd_dataset["imo"],
                "vessel_name"        : vessel_master["VesselName"]["S"],
                "operator"           : ytd_dataset["operator"],
                "ytd_distance"       : round(ytd_dataset["distance"]),
                "ytd_foc"            : round(ytd_dataset["foc"]),
                "ytd_eua"            : round(ytd_dataset["eua"]),
                "ytd_eua_cost"       : round(ytd_dataset["eua"] * float(eua_price)),
                "ytd_cb"             : round(ytd_dataset["cb"], 1),
                "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                "eoy_distance"       : round(eoy_vessel_data["eoy_distance"]),
                "eoy_foc"            : round(eoy_vessel_data["eoy_foc"]),
                "eoy_eua"            : round(eoy_vessel_data["eoy_eua"]),
                "eoy_eua_cost"       : round(round(eoy_vessel_data["eoy_eua"]) * float(eua_price)),
                "eoy_cb"             : round(float(eoy_vessel_data["eoy_cb"]), 1),
                "eoy_cb_cost"        : round(eoy_vessel_data["eoy_cb_cost"])
            }
            record_data_list.append(dataset)

        # 実測データ有りspeed-plan
        elif len(ytd_exist_speed_list) > 0:
            eoy_vessel_data = make_eoy_record.make_speed_plans_data(rec, ytd_exist_speed_list, res_foc_formulas, fuel_oil_type_list, energy, penalty_factor)

            dataset = {
                "imo"                : ytd_dataset["imo"],
                "vessel_name"        : vessel_master["VesselName"]["S"],
                "operator"           : ytd_dataset["operator"],
                "ytd_distance"       : round(ytd_dataset["distance"]),
                "ytd_foc"            : round(ytd_dataset["foc"]),
                "ytd_eua"            : round(ytd_dataset["eua"]),
                "ytd_eua_cost"       : round(ytd_dataset["eua"] * float(eua_price)),
                "ytd_cb"             : round(ytd_dataset["cb"], 1),
                "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                "eoy_distance"       : round(eoy_vessel_data["eoy_distance"]),
                "eoy_foc"            : round(eoy_vessel_data["eoy_foc"]),
                "eoy_eua"            : round(eoy_vessel_data["eoy_eua"]),
                "eoy_eua_cost"       : round(round(eoy_vessel_data["eoy_eua"]) * float(eua_price)),
                "eoy_cb"             : round(float(eoy_vessel_data["eoy_cb"]), 1),
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
                "ytd_eua_cost"       : round(ytd_dataset["eua"] * float(eua_price)),
                "ytd_cb"             : round(ytd_dataset["cb"], 1),
                "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                "eoy_distance"       : round(ytd_dataset["distance"]),
                "eoy_foc"            : round(ytd_dataset["foc"]),
                "eoy_eua"            : round(ytd_dataset["eua"]),
                "eoy_eua_cost"       : round(round(ytd_dataset["eua"]) * float(eua_price)),
                "eoy_cb"             : round(float(ytd_dataset["cb"]), 1),
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
        eoy_vessel_data = make_eoy_record.make_voyage_plans_data(None, ytd_not_exist_voyage_list, res_foc_formulas, fuel_oil_type_list, energy, penalty_factor)

        # ytdは0、eoyはSimulation結果を設定
        dataset = {
            "imo"                : imo,
            "vessel_name"        : vessel_master["VesselName"]["S"],
            "operator"           : ytd_not_exist_voyage_list[0]["operator"]["S"],
            "ytd_distance"       : "0",
            "ytd_foc"            : "0",
            "ytd_eua"            : "0",
            "ytd_eua_cost"       : "0",
            "ytd_cb"             : "0",
            "ytd_cb_cost"        : "0",
            "eoy_distance"       : round(eoy_vessel_data["eoy_distance"]),
            "eoy_foc"            : round(eoy_vessel_data["eoy_foc"]),
            "eoy_eua"            : round(eoy_vessel_data["eoy_eua"]),
            "eoy_eua_cost"       : round(round(eoy_vessel_data["eoy_eua"]) * float(eua_price)),
            "eoy_cb"             : round(float(eoy_vessel_data["eoy_cb"]), 1),
            "eoy_cb_cost"        : round(eoy_vessel_data["eoy_cb_cost"])
        }
        record_data_list.append(dataset)

    # 実測データ無しspeed-plan
    if len(ytd_not_exist_speed_list) > 0:

        eoy_vessel_data = make_eoy_record.make_speed_plans_data(None, ytd_not_exist_speed_list, res_foc_formulas, fuel_oil_type_list, energy, penalty_factor)

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
            "eoy_eua_cost"       : round(round(eoy_vessel_data["eoy_eua"], 1) * float(eua_price)),
            "eoy_cb"             : round(float(eoy_vessel_data["eoy_cb"]), 1),
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
    last_year = 0

    thisyear_year_total_list = []
    operator_total_list      = []

    # 返却用データセット
    record_data_list = []

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_imo(imo)

    # 同一imoのyear-totalリストでループ
    for year_rec in total_year_total_list:

        if year_rec["year_and_ope"]["S"][0:4] == year:
            thisyear_year_total_list.append(year_rec)

        # operator = year_rec["year_and_ope"]["S"][4:50]

        # # 現時点ではオペレーターは複数ないので飛ばす
        # # if operator not in operator_list : # 上限は適当
        #     # オペレーター毎に振り分ける
        #     # operator_list.append(operator)
        #     # index = operator_list.index(operator)

        #     # operator_total_list.append([])
        # operator_total_list.append(year_rec)

    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:

        operator = rec["year_and_ope"]["S"][4:50]

        # 昨年分のレコードを入れるリスト
        last_year_rec = []

        print(f"total_year_total_list:{total_year_total_list}")
        # 同一imoのyear-totalリストでループ
        for year_rec in total_year_total_list:

            tmp_operator = year_rec["year_and_ope"]["S"][4:50]

            # 同一オペレータのレコードを抽出
            if tmp_operator == operator:
                operator_total_list.append(year_rec)

                # 西暦部分の確認、昨年のレコードであれば保持しておく。
                tmp_year = year_rec["year_and_ope"]["S"][0:4]
                if tmp_year == str(int(year) - 1):
                    last_year_rec = year_rec
        
        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0
        year_count = 0

        for operator_rec in operator_total_list:
            
            # 先頭要素はスキップ（先頭要素は今年のレコード、前年以前を見たいため、スキップで良い）
            if year_count == 0:
                year_count += 1
                continue

            # １年ずつさかのぼる（年が飛んだ時点で確認不要のためbreak）
            if operator_rec["year_and_ope"]["S"][0:4] == str(int(year) - year_count):
                # 罰金フラグの確認
                fine_flag = operator_rec["fine_flag"]["S"]

                if fine_flag == "1":
                    consecutive_years += 1
                else:
                    break
            else:
                break
            
            year_count += 1

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"])
            last_year_borrowing = float(last_year_rec["borrowing"]["S"] if "borrowing" in last_year_rec else "0")

            if last_year_borrowing > 0:
                last_year += last_year_borrowing * (-1.1)
                thisYear_borrowing = False
            elif last_year_banking > 0:
                last_year += last_year_borrowing
            else:
                last_year += 0

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        operator  = rec["year_and_ope"]["S"][4:50]
        lng       = float(rec["total_lng"]["S"])
        hfo       = float(rec["total_hfo"]["S"])
        lfo       = float(rec["total_lfo"]["S"])
        mdo       = float(rec["total_mdo"]["S"])
        mgo       = float(rec["total_mgo"]["S"])
        distance  = float(rec["distance"]["S"])
        eua       = float(rec["eua"]["S"])
        cb        = float(rec["cb"]["S"])
        banking   = float(rec["banking"]["S"])
        borrowing = float(rec["borrowing"]["S"] if "borrowing" in rec else "0")

        # EUAからEUA costを算出する
        eua_cost  = round(eua, 1) * float(eua_price)

        # CBから消費量エネルギー（EU Rate考慮済）を算出する
        GHG_Max    = calculate_function.calc_GHG_Max(year)
        GHG_Actual = calculate_function.calc_GHG_Actual(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
        energy          = cb / (GHG_Max - GHG_Actual)

        # 必要な計算を行う
        foc             = lng + hfo + lfo + mdo + mgo
        total_cb        = cb + borrowing + banking + last_year
        penalty_factor  = (consecutive_years) / 10 + 1

        cb_cost = 0
        if total_cb < 0:
            cb_cost = abs(total_cb) * 2400 * penalty_factor / (GHG_Actual * 41000)

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
            "eoy_foc"            : str(round(foc)),
            "eoy_eua"            : str(round(eua)),
            "eoy_eua_cost"       : str(round(eua_cost)),
            "eoy_cb"             : str(round(total_cb, 1)),
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