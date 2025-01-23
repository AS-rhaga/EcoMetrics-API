
import ast
from datetime import datetime
import json
import math
import re

from dynamodb import select
from Util import Util
from calculate import calculate_function
from vesselinfo import make_eoy_record

def make_recoed(eua_price, imo, year, fuel_oil_type_list):

    # 必要な変数・リストを作成
    last_year = 0
    voyage_flag = "0"
    speed_flag  = "0"

    eoy_lng_ods     = 0
    eoy_lng_oms     = 0
    eoy_lng_oss     = 0
    eoy_hfo         = 0
    eoy_lfo         = 0
    eoy_mdo         = 0
    eoy_mgo         = 0
    eoy_lpg_p       = 0
    eoy_lpg_b       = 0
    eoy_nh3_ng      = 0
    eoy_nh3_ef      = 0
    eoy_methanol_ng = 0
    eoy_h2_ng       = 0
    eoy_energy      = 0

    thisyear_year_total_list = []
    operator_total_list      = []
    ytd_dataset_list         = []
    ytd_vessel_info  = []
    eoy_vessel_info  = []

    # 返却用データセット
    record_data_list = []
    ytd_fuel_list    = []
    eoy_fuel_list    = []

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_imo(imo)

    # FOCFormulas取得
    res_foc_formulas = select.get_foc_formulas(imo)

    # シミュレーション用テーブルを取得
    simulation_plan_voyage_list = select.get_simulation_voyage(imo, year)
    simulation_plan_speed       = select.get_simulation_speed(imo, year)

    # シミュレーションプラン管理用リスト
    ytd_exist_voyage_list     = []
    ytd_not_exist_voyage_list = []
    ytd_exist_speed_list      = []
    ytd_not_exist_speed_list  = []

    # 同一imoのyear-totalリストでループ
    for year_rec in total_year_total_list:

        if year_rec["year_and_ope"]["S"][0:4] == year:
            thisyear_year_total_list.append(year_rec)

        operator = year_rec["year_and_ope"]["S"][4:50]

        # 現時点ではオペレーターは複数ないので飛ばす
        # if operator not in operator_list : # 上限は適当
            # オペレーター毎に振り分ける
            # operator_list.append(operator)
            # index = operator_list.index(operator)

            # operator_total_list.append([])
        operator_total_list.append(year_rec)

    # 各種燃料の消費量と、消費エネルギーの合計値用変数を設定する。
    ytd_lng    = 0
    ytd_hfo    = 0
    ytd_lfo    = 0
    ytd_mdo    = 0
    ytd_mgo    = 0
    ytd_energy = 0

    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:
        operator = rec["year_and_ope"]["S"][4:50]

        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0

        # 昨年分・今年分のレコードを入れるリスト
        last_year_rec = []
        this_year_rec = []

        for operator_rec in operator_total_list:

            # 罰金フラグの確認
            fine_flag = operator_rec["fine_flag"]["S"]

            if fine_flag == "1":
                consecutive_years += 1
            else:
                consecutive_years = 0
                break

            # 西暦部分の確認
            rec_year = operator_rec["year_and_ope"]["S"][0:4]
            if rec_year == str(int(year - 1)):
                last_year_rec = operator_rec
            elif rec_year == year:
                this_year_rec = operator_rec

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"])
            last_year_borrowing = float(last_year_rec["borrowing"]["S"])


            if last_year_borrowing > 0:
                last_year += last_year_borrowing * (-1.1)
                thisYear_borrowing = False
            elif last_year_banking > 0:
                last_year += last_year_borrowing
            else:
                last_year += 0

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        operator  = this_year_rec["year_and_ope"]["S"][4:50]
        lng       = float(this_year_rec["total_lng"]["S"])
        hfo       = float(this_year_rec["total_hfo"]["S"])
        lfo       = float(this_year_rec["total_lfo"]["S"])
        mdo       = float(this_year_rec["total_mdo"]["S"])
        mgo       = float(this_year_rec["total_mgo"]["S"])
        distance  = float(this_year_rec["distance"]["S"])
        eua       = float(this_year_rec["eua"]["S"])
        cb        = float(this_year_rec["cb"]["S"])
        banking   = float(this_year_rec["banking"]["S"])
        borrowing = float(this_year_rec["banking"]["S"])

        # EUAからEUA costを算出する
        eua_cost  = eua * eua_price

        # CBから消費量エネルギー（EU Rate考慮済）を算出する
        GHG_Max    = calculate_function.calc_GHG_Max(year)
        GHG_Actual = calculate_function.calc_GHG_Actual(lng, 0, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
        energy          = cb / (GHG_Max - GHG_Actual)

        # 必要な計算を行う
        foc             = lng + hfo + lfo + mdo + mgo
        total_cb        = cb + borrowing + banking + last_year
        penalty_factor  = (consecutive_years - 1) / 10 + 1

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
            "cb"             : total_cb + last_year,
            "cb_cost"        : cb_cost
        }
        ytd_dataset_list.append(ytd_dataset)

        # 合計用変数に加算する。
        ytd_lng    += lng
        ytd_hfo    += hfo
        ytd_lfo    += lfo
        ytd_mdo    += mdo
        ytd_mgo    += mgo
        ytd_energy += energy

        # シミュレーション結果を反映したEnd of Yearのデータを作成する。

        # voyage-planのシミュレーション使用フラグ確認
        voyage_flag = simulation_plan_voyage_list[0]["flag"]["S"] if "flag" in simulation_plan_voyage_list[0] else "0"
        if voyage_flag == "1":

            # voyageのオペレーターのオペレーターを確認する
            for simulation_voyage in simulation_plan_voyage_list:
                simulation_operator = simulation_voyage["operator"]["S"]

                # 基準のyear-totalレコードのオペレーターと一致する場合
                if simulation_operator == operator:
                    # 実測データが存在するリストに追加
                    ytd_exist_voyage_list.append(simulation_voyage)
                    # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                    simulation_plan_voyage_list.remove(simulation_voyage)
        
        # speed-planのシミュレーション使用フラグ確認
        speed_flag = simulation_plan_speed[0]["flag"]["S"] if "flag" in simulation_plan_speed[0] else "0"
        if speed_flag == "1":

            # オペレーターのオペレーターを確認する
            simulation_operator = simulation_plan_speed[0]["operator"]["S"]

            # 基準のyear-totalレコードのオペレーターと一致する場合
            if simulation_operator == operator:
                # 実測データが存在するリストに追加
                ytd_exist_speed_list.append(simulation_plan_speed[0])
                # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                simulation_plan_speed.remove(simulation_plan_speed[0])
        
    # year-totalループ終了後、各シミュレーションリストに残っているものは、実測データ無しオペレーター
    if voyage_flag == "1" and len(simulation_plan_voyage_list) > 0:
        for simulation_voyage in simulation_plan_voyage_list:
            # 実測データが存在しないリストに追加
            ytd_not_exist_voyage_list.append(simulation_voyage)
    if speed_flag == "1" and len(simulation_plan_speed) > 0:
        ytd_not_exist_speed_list.append(simulation_plan_speed[0])

    # シミュレーションリストの処理
    # 実測データ有りvoyage-plan
    if len(ytd_exist_voyage_list) > 0:
        for i in range(len(ytd_exist_voyage_list)):
            eoy_vessel_data, eoy_fuel_list = make_eoy_record.make_voyage_plans_data(imo, year, thisyear_year_total_list, ytd_exist_voyage_list[i], res_foc_formulas, fuel_oil_type_list)

            # operatorが一致するytdデータと合わせて、データセットを作成
            for ytd_dataset in ytd_dataset_list:
                if ytd_dataset["operator"] == eoy_vessel_data["operator"]:
                    dataset = {
                        "imo"                : ytd_dataset["imo"],
                        "operator"           : ytd_dataset["operator"],
                        "ytd_distance"       : round(ytd_dataset["distance"]),
                        "ytd_foc"            : round(ytd_dataset["foc"]),
                        "ytd_eua"            : round(ytd_dataset["eua"]),
                        "ytd_eua_cost"       : round(ytd_dataset["eua"] * eua_price),
                        "ytd_cb"             : round(ytd_dataset["cb"], 1),
                        "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                        "eoy_distance"       : round(eoy_vessel_data["eoy_distance"]),
                        "eoy_foc"            : round(eoy_vessel_data["eoy_foc"]),
                        "eoy_eua"            : round(eoy_vessel_data["eoy_eua"]),
                        "eoy_eua_cost"       : round(eoy_vessel_data["eoy_eua_cost"]),
                        "eoy_cb"             : round(eoy_vessel_data["eoy_cb"], 1),
                        "eoy_cb_cost"        : round(eoy_vessel_data["eoy_cb_cost"])
                    }
                    record_data_list.append(dataset)

    # 実測データ有りspeed-plan
    if len(ytd_exist_speed_list) > 0:
        eoy_vessel_data, eoy_fuel_list = make_eoy_record.make_speed_plans_data(imo, year, thisyear_year_total_list, ytd_exist_speed_list, res_foc_formulas, fuel_oil_type_list)

        # operatorが一致するytdデータと合わせて、データセットを作成
        for ytd_dataset in ytd_dataset_list:
            if ytd_dataset["operator"] == eoy_vessel_data["operator"]:
                dataset = {
                    "imo"                : ytd_dataset["imo"],
                    "operator"           : ytd_dataset["operator"],
                    "ytd_distance"       : round(ytd_dataset["distance"]),
                    "ytd_foc"            : round(ytd_dataset["foc"]),
                    "ytd_eua"            : round(ytd_dataset["eua"]),
                    "ytd_eua_cost"       : round(ytd_dataset["eua"] * eua_price),
                    "ytd_cb"             : round(ytd_dataset["cb"], 1),
                    "ytd_cb_cost"        : round(ytd_dataset["cb_cost"]),
                    "eoy_distance"       : round(eoy_vessel_data["eoy_distance"]),
                    "eoy_foc"            : round(eoy_vessel_data["eoy_foc"]),
                    "eoy_eua"            : round(eoy_vessel_data["eoy_eua"]),
                    "eoy_eua_cost"       : round(eoy_vessel_data["eoy_eua_cost"]),
                    "eoy_cb"             : round(eoy_vessel_data["eoy_cb"], 1),
                    "eoy_cb_cost"        : round(eoy_vessel_data["eoy_cb_cost"])
                }
                record_data_list.append(dataset)
    
    # 実測データ無しvoyage-plan
    if len(ytd_not_exist_voyage_list) > 0:
        for i in range(len(ytd_not_exist_voyage_list)):
            eoy_vessel_data, eoy_fuel_list = make_eoy_record.make_voyage_plans_data(imo, year, thisyear_year_total_list, ytd_not_exist_voyage_list[i], res_foc_formulas, fuel_oil_type_list)

        # 全てゼロのytdデータを合わせて、データセットを作成
        dataset = {
            "imo"                : eoy_vessel_data["imo"],
            "operator"           : eoy_vessel_data["operator"],
            "ytd_distance"       : 0,
            "ytd_foc"            : 0,
            "ytd_eua"            : 0,
            "ytd_eua_cost"       : 0,
            "ytd_cb"             : 0,
            "ytd_cb_cost"        : 0,
            "eoy_distance"       : eoy_vessel_data["eoy_distance"],
            "eoy_foc"            : eoy_vessel_data["eoy_foc"],
            "eoy_eua"            : eoy_vessel_data["eoy_eua"],
            "eoy_eua_cost"       : eoy_vessel_data["eoy_eua_cost"],
            "eoy_cb"             : eoy_vessel_data["eoy_cb"],
            "eoy_cb_cost"        : eoy_vessel_data["eoy_cb_cost"]
        }
        record_data_list.append(dataset)

    # 実測データ無しspeed-plan
    if len(ytd_not_exist_speed_list) > 0:
        eoy_vessel_data, eoy_fuel_list = make_eoy_record.make_speed_plans_data(imo, year, thisyear_year_total_list, ytd_not_exist_speed_list, res_foc_formulas, fuel_oil_type_list)

        # 全てゼロのytdデータを合わせて、データセットを作成
        dataset = {
            "imo"                : eoy_vessel_data["imo"],
            "operator"           : eoy_vessel_data["operator"],
            "ytd_distance"       : 0,
            "ytd_foc"            : 0,
            "ytd_eua"            : 0,
            "ytd_eua_cost"       : 0,
            "ytd_cb"             : 0,
            "ytd_cb_cost"        : 0,
            "eoy_distance"       : eoy_vessel_data["eoy_distance"],
            "eoy_foc"            : eoy_vessel_data["eoy_foc"],
            "eoy_eua"            : eoy_vessel_data["eoy_eua"],
            "eoy_eua_cost"       : eoy_vessel_data["eoy_eua_cost"],
            "eoy_cb"             : eoy_vessel_data["eoy_cb"],
            "eoy_cb_cost"        : eoy_vessel_data["eoy_cb_cost"]
        }
        record_data_list.append(dataset)

    dataset = record_data_list

    return dataset

def make_recoed_past(eua_price, imo, year, fuel_oil_type_list):

    # 必要な変数・リストを作成
    last_year = 0

    thisyear_year_total_list = []
    operator_total_list      = []

    # 返却用データセット
    record_data_list = []

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_imo(imo)

    # FOCFormulas取得
    res_foc_formulas = select.get_foc_formulas(imo)
    FOCFormulas      = Util.util_focformulas(res_foc_formulas)

    # 同一imoのyear-totalリストでループ
    for year_rec in total_year_total_list:

        if year_rec["year_and_ope"]["S"][0:4] == year:
            thisyear_year_total_list.append(year_rec)

        operator = year_rec["year_and_ope"]["S"][4:50]

        # 現時点ではオペレーターは複数ないので飛ばす
        # if operator not in operator_list : # 上限は適当
            # オペレーター毎に振り分ける
            # operator_list.append(operator)
            # index = operator_list.index(operator)

            # operator_total_list.append([])
        operator_total_list.append(year_rec)

    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:
        operator = rec["year_and_ope"]["S"][4:50]

        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0

        # 昨年分・今年分のレコードを入れるリスト
        last_year_rec = []
        this_year_rec = []

        for operator_rec in operator_total_list:

            # 罰金フラグの確認
            fine_flag = operator_rec["fine_flag"]["S"]

            if fine_flag == "1":
                consecutive_years += 1
            else:
                consecutive_years = 0
                break

            # 西暦部分の確認
            rec_year = operator_rec["year_and_ope"]["S"][0:4]
            if rec_year == str(int(year - 1)):
                last_year_rec = operator_rec
            elif rec_year == year:
                this_year_rec = operator_rec

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"])
            last_year_borrowing = float(last_year_rec["borrowing"]["S"])


            if last_year_borrowing > 0:
                last_year += last_year_borrowing * (-1.1)
                thisYear_borrowing = False
            elif last_year_banking > 0:
                last_year += last_year_borrowing
            else:
                last_year += 0

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        operator  = this_year_rec["year_and_ope"]["S"][4:50]
        lng       = float(this_year_rec["total_lng"]["S"])
        hfo       = float(this_year_rec["total_hfo"]["S"])
        lfo       = float(this_year_rec["total_lfo"]["S"])
        mdo       = float(this_year_rec["total_mdo"]["S"])
        mgo       = float(this_year_rec["total_mgo"]["S"])
        distance  = float(this_year_rec["distance"]["S"])
        eua       = float(this_year_rec["eua"]["S"])
        cb        = float(this_year_rec["cb"]["S"])
        banking   = float(this_year_rec["banking"]["S"])
        borrowing = float(this_year_rec["banking"]["S"])

        # EUAからEUA costを算出する
        eua_cost  = eua * eua_price

        # CBから消費量エネルギー（EU Rate考慮済）を算出する
        GHG_Max    = calculate_function.calc_GHG_Max(year)
        GHG_Actual = calculate_function.calc_GHG_Actual(lng, 0, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
        energy          = cb / (GHG_Max - GHG_Actual)

        # 必要な計算を行う
        foc             = lng + hfo + lfo + mdo + mgo
        total_cb        = cb + borrowing + banking + last_year
        penalty_factor  = (consecutive_years - 1) / 10 + 1

        cb_cost = 0
        if total_cb < 0:
            cb_cost = abs(total_cb) * 2400 * penalty_factor / (GHG_Actual * 41000)

        # 全てゼロのytdデータを合わせて、データセットを作成
        dataset = {
            "imo"                : this_year_rec["imo"],
            "operator"           : this_year_rec["operator"],
            "ytd_distance"       : 0,
            "ytd_foc"            : 0,
            "ytd_eua"            : 0,
            "ytd_eua_cost"       : 0,
            "ytd_cb"             : 0,
            "ytd_cb_cost"        : 0,
            "eoy_distance"       : distance,
            "eoy_foc"            : foc,
            "eoy_eua"            : eua,
            "eoy_eua_cost"       : eua_cost,
            "eoy_cb"             : total_cb,
            "eoy_cb_cost"        : cb_cost
        }
        record_data_list.append(dataset)

    dataset = record_data_list

    return dataset