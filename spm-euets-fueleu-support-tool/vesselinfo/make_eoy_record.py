
import math
import json
from datetime import datetime
import ast
import re

from dynamodb import select
from Util import Util
from calculate import calculate_function

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def make_voyage_plans_data(imo, vessel_name, thisyear_year_total, voyage_plan_list, res_foc_formulas, fuel_oil_type_list, year, total_year_total_list, ytd_energy):

    # 各種合計値用変数のセット
    ytd_lng_oms       = 0
    ytd_hfo           = 0
    ytd_lfo           = 0
    ytd_mdo           = 0
    ytd_mgo           = 0
    ytd_eu_actual_foc = 0
    ytd_distance      = 0
    eoy_cb            = 0

    dataset_list         = []
    simuletion_fuel_list = []
    count_target_voyage  = 0
    operator_list        = []
    operator_total_list  = []

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    # str_now = dt_now.strftime('%Y-%m-%dT%H:%M')
    str_now = dt_now.strftime('%Y/%m/%d %H:%M')

    if thisyear_year_total:
        # 実績値を合計値用変数に加算
        ytd_lng_oms          = float(thisyear_year_total["total_lng"]["S"])
        ytd_hfo              = float(thisyear_year_total["total_hfo"]["S"])
        ytd_lfo              = float(thisyear_year_total["total_lfo"]["S"])
        ytd_mdo              = float(thisyear_year_total["total_mdo"]["S"])
        ytd_mgo              = float(thisyear_year_total["total_mgo"]["S"])
        ytd_eu_actual_foc    = float(thisyear_year_total["eu_actual_foc"]["S"])
        ytd_distance         = float(thisyear_year_total["distance"]["S"])
        count_target_voyage += 1

    for i in range(len(voyage_plan_list)):

        # 変数の設定
        leg_rate                = 0
        leg_total_time          = 0
        simulation_foc_per_day  = 0
        leg_part_time           = 0

        # legの開始・終了時刻を取得する
        str_departure_time = voyage_plan_list[i]["departure_time"]["S"]     # "2024-12-10 12:30"
        str_arrival_time   = voyage_plan_list[i]["arrival_time"]["S"]       # "2024-12-19 17:30"

        # legの開始・終了時刻からlegの時間を算出する
        dt_departure_time = Util.to_datetime(str_departure_time)
        # test_departure_time = dt_departure_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        dt_arrival_time = Util.to_datetime(str_arrival_time)
        # test_arrival_time = dt_arrival_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        # print(f"departure_time: {(test_departure_time)}, arrival_time: {(test_arrival_time)}")     
        leg_total_time = Util.calc_time_diff(dt_departure_time, dt_arrival_time)

        # 各legの期間から、反映割合を算出する
        # リスト項目の時刻はlocal時刻。UTCと比較してもJTCと比較しても多少ズレる
        if str_now <= str_departure_time:
            print(f"str_now:{str_now}, departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは完全に先時刻")
            leg_rate              = 1

        elif str_now <= str_arrival_time:
            print(f"str_now:{str_now}, departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは現在進行中")
            # 表示する範囲の時間を算出し、leg全体に対する割合を求める。
            dt_time_from  = Util.to_datetime(str_now)
            dt_time_to    = Util.to_datetime(str_arrival_time)
            leg_part_time = Util.calc_time_diff(dt_time_from, dt_time_to)

            leg_rate              = float(leg_part_time / leg_total_time)

            # 割合を考慮した時間で上書き
            leg_total_time = leg_part_time

        else:
            print("このレグは範囲外")
            print(f"str_now:{str_now}, departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは完結済")
            # 以降の処理は行わず、次のlegを確認
            continue

        # 各項目を取得
        operator     = voyage_plan_list[i]["operator"]["S"]
        displacement = voyage_plan_list[i]["dispracement"]["S"]
        leg_distance = float(voyage_plan_list[i]["distance"]["S"]) * leg_rate
        print(f"voyage_plan_list[i][distance][S]:{(voyage_plan_list[i]["distance"]["S"])}, leg_rate:{(leg_rate)}, leg_distance:{(leg_distance)}")
        leg_eu_rate  = int(voyage_plan_list[i]["eu_rate"]["S"])

        # log_speedを算出
        leg_log_speed = leg_distance / leg_total_time

        # if res_foc_formulas and leg_eu_rate != 0:    ←EU Rateゼロのレグしかない時にEnd of Yearが作られない
        if res_foc_formulas:

            # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
            auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])

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
            eu_actual_foc      = simulation_foc_per_hour * leg_total_time
            simulation_leg_foc = eu_actual_foc * leg_eu_rate / 100

            # 燃料別消費量を算出する
            fuel_list = Util.convertFuelOileStringToList(voyage_plan_list[i]["fuel"]["S"])  

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

                if  fuel_type == "LNG(Otto Diesel Speed)":
                    simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                elif  fuel_type == "LNG(Otto Medium Speed)":
                    simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                elif  fuel_type == "LNG(Otto Slow Speed)":
                    simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "HFO":
                    simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "LFO":
                    simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "MDO":
                    simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "MGO":
                    simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "LPG(Propane)":
                    simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "LPG(Butane)":
                    simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "NH3(Natural gas)":
                    simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "NH3(e-fuel)":
                    simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "Methanol(Natural gas)":
                    simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                elif fuel_type == "H2(Natural gas)":
                    simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100

            # シミュレーション部分のエネルギー総消費量を算出する
            simulation_energy  = calculate_function.calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)

            # opeリストを作成する
            if operator in operator_list:

                # 燃料消費量を足し合わせるグループを探す
                list_index = 0
                for operator_total in operator_total_list:

                    if operator_total["operator"] == operator:
                        print(f"ループ({(list_index + 1)})  operator_total_list[{(list_index)}]:{(operator_total_list[list_index])}")

                        # 合計値（加算前）を取得する。
                        bk_operator_total_distance = operator_total["total_distance"]
                        bk_operator_voyage_count   = operator_total["voyage_count"]
                        bk_operator_total_lng_ods  = operator_total["total_lng_ods"]
                        bk_operator_total_lng_oms  = operator_total["total_lng_oms"]
                        bk_operator_total_lng_oss  = operator_total["total_lng_oss"]
                        bk_operator_total_hfo      = operator_total["total_hfo"]
                        bk_operator_total_lfo      = operator_total["total_lfo"]
                        bk_operator_total_mdo      = operator_total["total_mdo"]
                        bk_operator_total_mgo      = operator_total["total_mgo"]
                        bk_operator_total_lpg_p    = operator_total["total_lpg_p"]
                        bk_operator_total_lpg_b    = operator_total["total_lpg_b"]
                        bk_operator_total_nh3_ng   = operator_total["total_nh3_ng"]
                        bk_operator_total_nh3_ef   = operator_total["total_nh3_ef"]
                        bk_operator_total_methanol_ng = operator_total["total_methanol_ng"]
                        bk_operator_total_h2_ng         = operator_total["total_h2_ng"]
                        bk_operator_total_foc           = operator_total["total_foc"]
                        bk_operator_total_eu_actual_foc = operator_total["total_eu_actual_foc"]
                        bk_operator_total_energy        = operator_total["total_energy"]

                        # プーリンググループの合計値を加算した値に書き換える。
                        operator_total["total_distance"]      = bk_operator_total_distance + leg_distance
                        operator_total["voyage_count"]        = bk_operator_voyage_count + 1
                        operator_total["total_lng_ods"]       = bk_operator_total_lng_ods + simulation_leg_lng_ods
                        operator_total["total_lng_oms"]       = bk_operator_total_lng_oms + simulation_leg_lng_oms
                        operator_total["total_lng_oss"]       = bk_operator_total_lng_oss + simulation_leg_lng_oss
                        operator_total["total_hfo"]           = bk_operator_total_hfo + simulation_leg_hfo
                        operator_total["total_lfo"]           = bk_operator_total_lfo + simulation_leg_lfo
                        operator_total["total_mdo"]           = bk_operator_total_mdo + simulation_leg_mdo
                        operator_total["total_mgo"]           = bk_operator_total_mgo + simulation_leg_mgo
                        operator_total["total_lpg_p"]         = bk_operator_total_lpg_p + simulation_leg_lpg_p
                        operator_total["total_lpg_b"]         = bk_operator_total_lpg_b + simulation_leg_lpg_b
                        operator_total["total_nh3_ng"]        = bk_operator_total_nh3_ng + simulation_leg_nh3_ng
                        operator_total["total_nh3_ef"]        = bk_operator_total_nh3_ef + simulation_leg_nh3_ef
                        operator_total["total_methanol_ng"]   = bk_operator_total_methanol_ng + simulation_leg_methanol_ng
                        operator_total["total_h2_ng"]         = bk_operator_total_h2_ng + simulation_leg_h2_ng
                        operator_total["total_foc"]           = bk_operator_total_foc + simulation_leg_foc
                        operator_total["total_eu_actual_foc"] = bk_operator_total_eu_actual_foc + eu_actual_foc
                        operator_total["total_energy"]        = bk_operator_total_energy + simulation_energy

                        # リストを更新する。
                        operator_total_list[list_index] = operator_total

                    list_index += 1

            # opeリストに追加、新規データセットを作成する
            else:
                operator_list.append(operator)
                print(f"operator_list[{type(operator_list)}]: {operator_list}")

                data_list = {
                    "operator"            : operator,
                    "voyage_count"        : count_target_voyage + 1,
                    "total_distance"      : leg_distance,
                    "total_lng_ods"       : simulation_leg_lng_ods,
                    "total_lng_oms"       : simulation_leg_lng_oms,
                    "total_lng_oss"       : simulation_leg_lng_oss,
                    "total_hfo"           : simulation_leg_hfo,
                    "total_lfo"           : simulation_leg_lfo,
                    "total_mdo"           : simulation_leg_mdo,
                    "total_mgo"           : simulation_leg_mgo,
                    "total_lpg_p"         : simulation_leg_lpg_p,
                    "total_lpg_b"         : simulation_leg_lpg_b,
                    "total_nh3_ng"        : simulation_leg_nh3_ng,
                    "total_nh3_ef"        : simulation_leg_nh3_ef,
                    "total_methanol_ng"   : simulation_leg_methanol_ng,
                    "total_h2_ng"         : simulation_leg_h2_ng,
                    "total_foc"           : simulation_leg_foc,
                    "total_eu_actual_foc" : eu_actual_foc,
                    "total_energy"        : simulation_energy,
                }
                operator_total_list.append(data_list)

    # 以下、合計値データセットでオペレーター分ループ
    print(f"imo:{(imo)} operator_total_list:{(operator_total_list)}")

    for operator_total in operator_total_list:

        # 該当オペレーターの過去実績を取得する。
        operators_year_total_list = []
        last_year_rec             = []

        for year_rec in total_year_total_list:

            tmp_operator = year_rec["year_and_ope"]["S"][4:50]

            # 同一オペレータのレコードを抽出
            if tmp_operator == operator_total["operator"]:
                operators_year_total_list.append(year_rec)

                # 西暦部分の確認、昨年のレコードであれば保持しておく。
                tmp_year = year_rec["year_and_ope"]["S"][0:4]
                if tmp_year == str(int(year) - 1):
                    last_year_rec = year_rec

        operators_year_total_list = sorted(operators_year_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)
        print(f"operator_total[operator]:{(operator_total["operator"])}, last_year_rec:{(last_year_rec)}")

        # 初期値設定
        consecutive_years = 0
        year_count = 0
        last_year = 0
        
        for operator_rec in operators_year_total_list:

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

        # End of Yearにborrowingができるかどうかのフラグを設定
        endofYear_borrowing = True

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"]) if "banking" in last_year_rec and last_year_rec["banking"]["S"] != "" else 0
            last_year_borrowing = float(last_year_rec["borrowing"]["S"]) if "borrowing" in last_year_rec and last_year_rec["borrowing"]["S"] != "" else 0
            print(f"last_year_banking:{(last_year_banking)}, last_year_borrowing:{(last_year_borrowing)}")

            if last_year_borrowing > 0:
                last_year = last_year_borrowing * (-1.1)
                endofYear_borrowing = False
            elif last_year_banking > 0:
                last_year = last_year_borrowing
            else:
                last_year = 0

        if operator_total["voyage_count"] > 0:

            total_distance      = operator_total["total_distance"] + ytd_distance
            total_lng_ods       = operator_total["total_lng_ods"]
            total_lng_oms       = operator_total["total_lng_oms"] + ytd_lng_oms
            total_lng_oss       = operator_total["total_lng_oss"]
            total_hfo           = operator_total["total_hfo"] + ytd_hfo
            total_lfo           = operator_total["total_lfo"] + ytd_lfo
            print(f"operator_total[total_lfo]:{(operator_total["total_lfo"])}, ytd_lfo:{(ytd_lfo)}, total_lfo:{(total_lfo)}")
            total_mdo           = operator_total["total_mdo"] + ytd_mdo
            total_mgo           = operator_total["total_mgo"] + ytd_mgo
            total_lpg_p         = operator_total["total_lpg_p"]
            total_lpg_b         = operator_total["total_lpg_b"]
            total_nh3_ng        = operator_total["total_nh3_ng"]
            total_nh3_ef        = operator_total["total_nh3_ef"]
            total_methanol_ng   = operator_total["total_methanol_ng"]
            total_h2_ng         = operator_total["total_h2_ng"]
            total_eu_actual_foc = operator_total["total_eu_actual_foc"] + ytd_eu_actual_foc
            total_energy        = operator_total["total_energy"] + ytd_energy

            print(f"imo+ope:{(imo)}+{(operator_total["operator"])} simulation:{(operator_total["total_energy"])} ytd_energy:{(ytd_energy)}")
            print(f"total_energy:{(total_energy)}")

            simulation_total_lng_oms     = operator_total["total_lng_oms"]
            simulation_total_lng_oss     = operator_total["total_lng_oss"]
            simulation_total_lng_ods     = operator_total["total_lng_ods"]
            simulation_total_hfo         = operator_total["total_hfo"]
            simulation_total_lfo         = operator_total["total_lfo"]
            simulation_total_mdo         = operator_total["total_mdo"]
            simulation_total_mgo         = operator_total["total_mgo"]
            simulation_total_lpg_p       = operator_total["total_lpg_p"]
            simulation_total_lpg_b       = operator_total["total_lpg_b"]
            simulation_total_h2_ng       = operator_total["total_h2_ng"]
            simulation_total_nh3_ng      = operator_total["total_nh3_ng"]
            simulation_total_methanol_ng = operator_total["total_methanol_ng"]
            simulation_total_nh3_ef      = operator_total["total_nh3_ng"]
            simulation_total_energy      = operator_total["total_energy"]

            # 最終的なCBを算出
            total_GHG = calculate_function.calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
            # print(f"レコード作成用データ  imo:{(imo)} eoy_total_lfo:{(total_lfo)}, eoy_total_mgo:{(total_mgo)}, eoy_total_energy:{(total_energy)}")
            eoy_cb    = calculate_function.calc_cb(now_year, total_energy, total_GHG)

            # eoy_borrowingを取得
            eoy_borrowing = float(thisyear_year_total["eoy_borrowing"]["S"]) if thisyear_year_total and "eoy_borrowing" in thisyear_year_total and thisyear_year_total["eoy_borrowing"]["S"] != "" else 0

            total_cb  = eoy_cb + eoy_borrowing + last_year
            print(f"total_cb:{(total_cb)}, last_year:{(last_year)}")

            # CB Costの算出
            last_year_cost = 0
            eoy_cb_cost = 0
            penalty_factor  = (consecutive_years) / 10 + 1

            # 昨年分のGHG強度を算出
            last_year_lng   = float(last_year_rec["total_lng"]["S"]) if "total_lng" in last_year_rec and last_year_rec["total_lng"]["S"] != "" else 0
            last_year_hfo   = float(last_year_rec["total_hfo"]["S"]) if "total_hfo" in last_year_rec and last_year_rec["total_hfo"]["S"] != "" else 0
            last_year_lfo   = float(last_year_rec["total_lfo"]["S"]) if "total_lfo" in last_year_rec and last_year_rec["total_lfo"]["S"] != "" else 0
            last_year_mdo   = float(last_year_rec["total_mdo"]["S"]) if "total_mdo" in last_year_rec and last_year_rec["total_mdo"]["S"] != "" else 0
            last_year_mgo   = float(last_year_rec["total_mgo"]["S"]) if "total_mgo" in last_year_rec and last_year_rec["total_mgo"]["S"] != "" else 0
            
            GHG_last_year = calculate_function.calc_GHG_Actual(0, last_year_lng, 0, last_year_hfo, last_year_lfo, last_year_mdo, last_year_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)

            if last_year < 0:
                last_year_cost  = abs(float(last_year)) * 2400 / (GHG_last_year * 41000) * penalty_factor
            
            if total_cb >= 0:
                eoy_cb_cost = 0
            else:
                # CBコストの算出場合分け
                if last_year >= 0:
                    eoy_cb_cost = abs(float(total_cb)) * 2400 / (total_GHG * 41000) * penalty_factor
                else:
                    eoy_cb_cost = abs(float(total_cb)) * 2400 / (GHG_last_year * 41000) * penalty_factor

            borrowing_limit = calculate_function.calc_borrowing_limit(endofYear_borrowing, now_year, total_energy)

            # banking確認
            eoy_banking = 0
            if total_cb > 0:
                eoy_banking = total_cb

            # Voyage Planのシミュレーション用データ
            dataset = {
                "imo"            : imo,
                "vessel_name"    : vessel_name,
                "operator"       : operator_total["operator"],
                "distance"       : round(total_distance),
                "foc"            : round(total_eu_actual_foc),
                "end_of_year"    : round(float(eoy_cb) / 1000000, 1),
                "last_year"      : round(last_year / 1000000, 1),
                "last_year_noadjust": last_year,
                "last_year_cost" : round(last_year_cost),
                "borrowing_limit": round(borrowing_limit / 1000000),
                "borrowing"      : round(eoy_borrowing / 1000000, 1),
                "banking"        : round(eoy_banking / 1000000, 1),
                "total"          : round((total_cb - eoy_banking) / 1000000, 1),
                "penalty_factor" : penalty_factor,
                "cost"           : round(eoy_cb_cost, 0)
            }
            dataset_list.append(dataset)

            # ytdも入った燃料消費量 → シミュレーション部分だけの燃料消費量
            simuletion_fuel = {
                "simulation_hfo"        : simulation_total_hfo,
                "simulation_lfo"        : simulation_total_lfo,
                "simulation_mdo"        : simulation_total_mdo,
                "simulation_mgo"        : simulation_total_mgo,
                "simulation_lng_oms"    : simulation_total_lng_oms,
                "simulation_lng_oss"    : simulation_total_lng_oss,
                "simulation_lng_ods"    : simulation_total_lng_ods,
                "simulation_lpg_p"      : simulation_total_lpg_p,
                "simulation_lpg_b"      : simulation_total_lpg_b,
                "simulation_h2_ng"      : simulation_total_h2_ng,
                "simulation_nh3_ng"     : simulation_total_nh3_ng,
                "simulation_methanol_ng": simulation_total_methanol_ng,
                "simulation_nh3_ef"     : simulation_total_nh3_ef, 
                "simulation_energy"     : simulation_total_energy
            }
            simuletion_fuel_list.append(simuletion_fuel)

    return dataset_list, simuletion_fuel_list

def make_speed_plans_data(imo, vessel_name, year, thisyear_year_total, speed_plan, res_foc_formulas, fuel_oil_type_list, total_year_total_list, ytd_energy):

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
    total_distance    = 0
    total_eua         = 0
    total_energy      = 0
    eoy_cb            = 0
    total_cb          = 0

    # simulation部分だけの合計値
    simulation_total_lng_oms     = 0
    simulation_total_lng_oss     = 0
    simulation_total_lng_ods     = 0
    simulation_total_hfo         = 0
    simulation_total_lfo         = 0
    simulation_total_mdo         = 0
    simulation_total_mgo         = 0
    simulation_total_lpg_p       = 0
    simulation_total_lpg_b       = 0
    simulation_total_h2_ng       = 0
    simulation_total_nh3_ng      = 0
    simulation_total_methanol_ng = 0
    simulation_total_nh3_ef      = 0
    simulation_total_energy      = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)

    if thisyear_year_total:
        # 実績値を合計値用変数に加算
        total_lng_oms  += float(thisyear_year_total["total_lng"]["S"])
        total_hfo      += float(thisyear_year_total["total_hfo"]["S"])
        total_lfo      += float(thisyear_year_total["total_lfo"]["S"])
        total_mdo      += float(thisyear_year_total["total_mdo"]["S"])
        total_mgo      += float(thisyear_year_total["total_mgo"]["S"])
        total_foc      += float(thisyear_year_total["total_foc"]["S"])
        total_eu_actual_foc += float(thisyear_year_total["eu_actual_foc"]["S"])
        total_distance += float(thisyear_year_total["distance"]["S"])
        total_eua      += float(thisyear_year_total["eua"]["S"])
        total_energy   += ytd_energy

    # SpeedPlanのSimulation処理実施   
    # Time to End of Year算出（年末 - 現在）
    year_end = datetime(dt_now.year, 12, 31, 23, 59, 59)
    time_to_end_of_year = calculate_function.calc_time_diff(dt_now, year_end)

    # 航海時間を算出
    sailing_rate = float(speed_plan[0]["salling_rate"]["S"])
    sailing_time = time_to_end_of_year * (sailing_rate / 100)
    port_time    = time_to_end_of_year - sailing_time
    print(f"sailing_time:{(sailing_time)} port_time:{(port_time)}")

    # Ballast、Ladenそれぞれの航海距離を算出
    displacement_rate    = float(speed_plan[0]["dispracement_rate"]["S"])
    ballast_sailing_time = sailing_time * (displacement_rate / 100)
    laden_sailing_time   = sailing_time - ballast_sailing_time

    # 時間×速さで距離を算出
    ballast_logspeed = float(speed_plan[0]["log_speed_ballast"]["S"])
    laden_logspeed   = float(speed_plan[0]["log_speed_laden"]["S"])
    ballast_ditance  = ballast_sailing_time * ballast_logspeed
    laden_distance    = laden_sailing_time * laden_logspeed

    # BallastDisancen、LadenDistanceを加算
    total_ballast_laden_distance = ballast_ditance + laden_distance

    # 必要項目を取得
    leg_eu_rate = int(speed_plan[0]["eu_rate"]["S"])
    operator    = speed_plan[0]["operator"]["S"]

    if res_foc_formulas: 

        # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
        auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])

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
        leg_total_FOC_speed  = leg_total_actual_foc * leg_eu_rate / 100
        # 停泊中の総FOCを算出
        port_total_actual_foc = auxiliary_equipment / 24 * port_time
        port_total_FOC_speed  = port_total_actual_foc * leg_eu_rate / 100
        # 総FOCを算出
        total_actual_foc = leg_total_actual_foc + port_total_actual_foc
        total_FOC_speed  = leg_total_FOC_speed + port_total_FOC_speed
        
        # 燃料別消費量を算出する
        fuel_list = Util.convertFuelOileStringToList(speed_plan[0]["fuel"]["S"]) 

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

            if  fuel_type == "LNG(Otto Diesel Speed)":
                simulation_leg_lng_ods = total_FOC_speed * int(fuel_rate) / 100
                total_lng_ods     += simulation_leg_lng_ods
                simulation_total_lng_ods += simulation_leg_lng_ods
            elif  fuel_type == "LNG(Otto Medium Speed)":
                simulation_leg_lng_oms = total_FOC_speed * int(fuel_rate) / 100
                total_lng_oms     += simulation_leg_lng_oms
                simulation_total_lng_oms += simulation_leg_lng_oms
            elif  fuel_type == "LNG(Otto Slow Speed)":
                simulation_leg_lng_oss = total_FOC_speed * int(fuel_rate) / 100
                total_lng_oss     += simulation_leg_lng_oss
                simulation_total_lng_oss += simulation_leg_lng_oss
            elif fuel_type == "HFO":
                simulation_leg_hfo = total_FOC_speed * int(fuel_rate) / 100
                total_hfo          += simulation_leg_hfo
                simulation_total_hfo += simulation_leg_hfo
            elif fuel_type == "LFO":
                simulation_leg_lfo = total_FOC_speed * int(fuel_rate) / 100
                total_lfo         += simulation_leg_lfo
                simulation_total_lfo += simulation_leg_lfo
            elif fuel_type == "MDO":
                simulation_leg_mdo = total_FOC_speed * int(fuel_rate) / 100
                total_mdo         += simulation_leg_mdo
                simulation_total_mdo += simulation_leg_mdo
            elif fuel_type == "MGO":
                simulation_leg_mgo = total_FOC_speed * int(fuel_rate) / 100
                total_mgo         += simulation_leg_mgo
                simulation_total_mgo += simulation_leg_mgo
            elif fuel_type == "LPG(Propane)":
                simulation_leg_lpg_p = total_FOC_speed * int(fuel_rate) / 100
                total_lpg_p         += simulation_leg_lpg_p
                simulation_total_lpg_p += simulation_leg_lpg_p
            elif fuel_type == "LPG(Butane)":
                simulation_leg_lpg_b = total_FOC_speed * int(fuel_rate) / 100
                total_lpg_b         += simulation_leg_lpg_b
                simulation_total_lpg_b += simulation_leg_lpg_b
            elif fuel_type == "NH3(Natural gas)":
                simulation_leg_nh3_ng = total_FOC_speed * int(fuel_rate) / 100
                total_nh3_ng         += simulation_leg_nh3_ng
                simulation_total_nh3_ng += simulation_leg_nh3_ng
            elif fuel_type == "NH3(e-fuel)":
                simulation_leg_nh3_ef = total_FOC_speed * int(fuel_rate) / 100
                total_nh3_ef         += simulation_leg_nh3_ef
                simulation_total_nh3_ef += simulation_leg_nh3_ef
            elif fuel_type == "Methanol(Natural gas)":
                simulation_leg_methanol_ng = total_FOC_speed * int(fuel_rate) / 100
                total_methanol_ng         += simulation_leg_methanol_ng
                simulation_total_methanol_ng += simulation_leg_methanol_ng
            elif fuel_type == "H2(Natural gas)":
                simulation_leg_h2_ng = total_FOC_speed * int(fuel_rate) / 100
                total_h2_ng         += simulation_leg_h2_ng
                simulation_total_h2_ng += simulation_leg_h2_ng

        # シミュレーション部分のエネルギー総消費量を算出する
        simulation_energy  = calculate_function.calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
        total_energy      += simulation_energy
        simulation_total_energy += simulation_energy

        # 合計用変数に加算する
        total_distance += total_ballast_laden_distance
        total_foc      += (simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_lpg_b + simulation_leg_nh3_ng + simulation_leg_nh3_ef + simulation_leg_methanol_ng + simulation_leg_h2_ng)
        total_eu_actual_foc += total_actual_foc

        # 該当オペレーターの過去実績を取得する。
        operators_year_total_list = []
        last_year_rec       = []
        for year_rec in total_year_total_list:

            tmp_operator = year_rec["year_and_ope"]["S"][4:50]

            # 同一オペレータのレコードを抽出
            if tmp_operator == operator:
                operators_year_total_list.append(year_rec)

                # 西暦部分の確認、昨年のレコードであれば保持しておく。
                tmp_year = year_rec["year_and_ope"]["S"][0:4]
                if tmp_year == str(int(year) - 1):
                    last_year_rec = year_rec

        operators_year_total_list = sorted(operators_year_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0
        year_count = 0

        for operator_rec in operators_year_total_list:
            
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

        # End of Yearにborrowingができるかどうかのフラグを設定
        endofYear_borrowing = True

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"]) if "banking" in last_year_rec and last_year_rec["banking"]["S"] != "" else 0
            last_year_borrowing = float(last_year_rec["borrowing"]["S"]) if "borrowing" in last_year_rec and last_year_rec["borrowing"]["S"] != "" else 0

            if last_year_borrowing > 0:
                last_year = last_year_borrowing * (-1.1)
                endofYear_borrowing = False
            elif last_year_banking > 0:
                last_year = last_year_borrowing
            else:
                last_year = 0

        # CB算出
        total_GHG = calculate_function.calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
        eoy_cb    = calculate_function.calc_cb(now_year, total_energy, total_GHG)

        # eoy_borrowingを取得
        eoy_borrowing = float(thisyear_year_total["eoy_borrowing"]["S"]) if thisyear_year_total and "eoy_borrowing" in thisyear_year_total and thisyear_year_total["eoy_borrowing"]["S"] != "" else 0

        total_cb  = eoy_cb + eoy_borrowing + last_year
        # print(f"imo:{(imo)} total_cb:{(total_cb)}, eoy_cb:{(eoy_cb)}, borrowing:{(borrowing)}, last_year:{(last_year)}")

        # CB Costの算出
        last_year_cost = 0
        eoy_cb_cost = 0
        penalty_factor  = (consecutive_years) / 10 + 1

        # 昨年分のGHG強度を算出
        last_year_lng   = float(last_year_rec["total_lng"]["S"]) if "total_lng" in last_year_rec and last_year_rec["total_lng"]["S"] != "" else 0
        last_year_hfo   = float(last_year_rec["total_hfo"]["S"]) if "total_hfo" in last_year_rec and last_year_rec["total_hfo"]["S"] != "" else 0
        last_year_lfo   = float(last_year_rec["total_lfo"]["S"]) if "total_lfo" in last_year_rec and last_year_rec["total_lfo"]["S"] != "" else 0
        last_year_mdo   = float(last_year_rec["total_mdo"]["S"]) if "total_mdo" in last_year_rec and last_year_rec["total_mdo"]["S"] != "" else 0
        last_year_mgo   = float(last_year_rec["total_mgo"]["S"]) if "total_mgo" in last_year_rec and last_year_rec["total_mgo"]["S"] != "" else 0
        
        GHG_last_year = calculate_function.calc_GHG_Actual(0, last_year_lng, 0, last_year_hfo, last_year_lfo, last_year_mdo, last_year_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)

        if last_year < 0:
            last_year_cost  = abs(float(last_year)) * 2400 / (GHG_last_year * 41000) * penalty_factor

        if total_cb >= 0:
            eoy_cb_cost = 0
        else:
            # CBコストの算出場合分け
            if last_year >= 0:
                eoy_cb_cost = abs(float(total_cb)) * 2400 / (total_GHG * 41000) * penalty_factor
            else:
                # 昨年分のGHG強度を算出
                last_year_lng   = float(last_year_rec["total_lng"]["S"]) if "total_lng" in last_year_rec and last_year_rec["total_lng"]["S"] != "" else 0
                last_year_hfo   = float(last_year_rec["total_hfo"]["S"]) if "total_hfo" in last_year_rec and last_year_rec["total_hfo"]["S"] != "" else 0
                last_year_lfo   = float(last_year_rec["total_lfo"]["S"]) if "total_lfo" in last_year_rec and last_year_rec["total_lfo"]["S"] != "" else 0
                last_year_mdo   = float(last_year_rec["total_mdo"]["S"]) if "total_mdo" in last_year_rec and last_year_rec["total_mdo"]["S"] != "" else 0
                last_year_mgo   = float(last_year_rec["total_mgo"]["S"]) if "total_mgo" in last_year_rec and last_year_rec["total_mgo"]["S"] != "" else 0
                
                GHG_last_year = calculate_function.calc_GHG_Actual(0, last_year_lng, 0, last_year_hfo, last_year_lfo, last_year_mdo, last_year_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
                eoy_cb_cost = abs(float(total_cb)) * 2400 / (GHG_last_year * 41000) * penalty_factor

    borrowing_limit = calculate_function.calc_borrowing_limit(endofYear_borrowing, year, total_energy)

    # banking確認
    eoy_banking = 0
    if total_cb > 0:
        eoy_banking = total_cb

    # Speed Planのシミュレーション用データ
    dataset = {
        "imo"            : imo,
        "vessel_name"    : vessel_name,
        "operator"       : thisyear_year_total["year_and_ope"]["S"][4:50] if thisyear_year_total else speed_plan[0]["operator"]["S"],
        "distance"       : round(total_distance),
        "foc"            : round(total_eu_actual_foc),
        "end_of_year"    : round(float(eoy_cb) / 1000000, 1),
        "last_year"      : round(last_year / 1000000, 1),
        "last_year_noadjust":  last_year,
        "last_year_cost" : round(last_year_cost),
        "borrowing_limit": round(borrowing_limit / 1000000, 1),
        "borrowing"      : round(eoy_borrowing / 1000000, 1),
        "banking"        : round(eoy_banking / 1000000, 1),
        "total"          : round((total_cb - eoy_banking) / 1000000, 1),
        "penalty_factor" : penalty_factor,
        "cost"           : round(eoy_cb_cost)
    }

    # ytdも入った燃料消費量 → シミュレーション部分だけの燃料消費量
    simuletion_fuel_list = {
        "simulation_hfo": simulation_total_hfo,
        "simulation_lfo": simulation_total_lfo,
        "simulation_mdo": simulation_total_mdo,
        "simulation_mgo": simulation_total_mgo,
        "simulation_lng_oms": simulation_total_lng_oms,
        "simulation_lng_oss": simulation_total_lng_oss,
        "simulation_lng_ods": simulation_total_lng_ods,
        "simulation_lpg_p": simulation_total_lpg_p,
        "simulation_lpg_b": simulation_total_lpg_b,
        "simulation_h2_ng": simulation_total_h2_ng,
        "simulation_nh3_ng": simulation_total_nh3_ng,
        "simulation_methanol_ng": simulation_total_methanol_ng,
        "simulation_nh3_ef": simulation_total_nh3_ef, 
        "simulation_energy": simulation_total_energy
    }

    return dataset, simuletion_fuel_list