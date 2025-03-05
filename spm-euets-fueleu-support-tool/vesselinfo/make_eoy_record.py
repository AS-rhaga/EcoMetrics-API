
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

def make_voyage_plans_data(imo, vessel_name, thisyear_year_total, voyage_plan_list, res_foc_formulas, fuel_oil_type_list, penalty_factor, last_year, ytd_energy):

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

    dataset              = []
    simuletion_fuel_list = []
    count_target_voyage  = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    # str_now = dt_now.strftime('%Y-%m-%dT%H:%M')
    str_now = dt_now.strftime('%Y/%m/%d %H:%M')

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
        count_target_voyage += 1

    for i in range(len(voyage_plan_list)):

        # 変数の設定
        leg_rate                = 0
        leg_total_time          = 0
        simulation_foc_per_day  = 0

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
            count_target_voyage  += 1

        elif str_now <= str_arrival_time:
            print(f"str_now:{str_now}, departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは現在進行中")
            # 表示する範囲の時間を算出し、leg全体に対する割合を求める。
            dt_time_from  = Util.to_datetime(str_now)
            dt_time_to    = Util.to_datetime(str_arrival_time)
            leg_part_time = Util.calc_time_diff(dt_time_from, dt_time_to)

            # 割合を考慮した時間で上書き
            leg_total_time = leg_part_time

            leg_rate              = float(leg_part_time / leg_total_time)
            count_target_voyage  += 1

        else:
            print("このレグは範囲外")
            print(f"str_now:{str_now}, departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは完結済")
            # 以降の処理は行わず、次のlegを確認
            continue

        # 各項目を取得
        displacement           = voyage_plan_list[i]["dispracement"]["S"]
        leg_distance        = float(voyage_plan_list[i]["distance"]["S"]) * leg_rate
        leg_eu_rate                = int(voyage_plan_list[i]["eu_rate"]["S"])

        # log_speedを算出
        leg_log_speed = leg_distance / leg_total_time

        if res_foc_formulas and leg_eu_rate != 0:

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
            simulation_leg_foc = simulation_foc_per_hour * leg_total_time * leg_eu_rate / 100

            # 燃料別消費量を算出する
            fuel_list = Util.convertFuelOileStringToList(voyage_plan_list[i]["fuel"]["S"])  

            for fuel in fuel_list:
                fuel_info_list = fuel.split(',')
                fuel_type = fuel_info_list[0]
                fuel_rate = int(fuel_info_list[1])

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

                if  fuel_type == "LNG(Otto Diesel Speed)":
                    simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                    total_lng_ods     += simulation_leg_lng_ods
                    simulation_total_lng_ods += simulation_leg_lng_ods
                elif  fuel_type == "LNG(Otto Medium Speed)":
                    simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                    total_lng_oms     += simulation_leg_lng_oms
                    simulation_total_lng_oms += simulation_leg_lng_oms
                elif  fuel_type == "LNG(Otto Slow Speed)":
                    simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                    total_lng_oss     += simulation_leg_lng_oss
                    simulation_total_lng_oss += simulation_leg_lng_oss
                elif fuel_type == "HFO":
                    simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                    total_hfo          += simulation_leg_hfo
                    simulation_total_hfo += simulation_leg_hfo
                elif fuel_type == "LFO":
                    simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                    total_lfo         += simulation_leg_lfo
                    simulation_total_lfo += simulation_leg_lfo
                elif fuel_type == "MDO":
                    simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                    total_mdo         += simulation_leg_mdo
                    simulation_total_mdo += simulation_leg_mdo
                elif fuel_type == "MGO":
                    simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                    total_mgo         += simulation_leg_mgo
                    simulation_total_mgo += simulation_leg_mgo
                elif fuel_type == "LPG(Propane)":
                    simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                    total_lpg_p         += simulation_leg_lpg_p
                    simulation_total_lpg_p += simulation_leg_lpg_p
                elif fuel_type == "LPG(Butane)":
                    simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                    total_lpg_b         += simulation_leg_lpg_b
                    simulation_total_lpg_b += simulation_leg_lpg_b
                elif fuel_type == "NH3(Natural gas)":
                    simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                    total_nh3_ng         += simulation_leg_nh3_ng
                    simulation_total_nh3_ng += simulation_leg_nh3_ng
                elif fuel_type == "NH3(e-fuel)":
                    simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                    total_nh3_ef         += simulation_leg_nh3_ef
                    simulation_total_nh3_ef += simulation_leg_nh3_ef
                elif fuel_type == "Methanol(Natural gas)":
                    simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                    total_methanol_ng         += simulation_leg_methanol_ng
                    simulation_total_methanol_ng += simulation_leg_methanol_ng
                elif fuel_type == "H2(Natural gas)":
                    simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100
                    total_h2_ng         += simulation_leg_h2_ng
                    simulation_total_h2_ng += simulation_leg_h2_ng

            # シミュレーション部分のエネルギー総消費量を算出する
            simulation_energy  = calculate_function.calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
            total_energy      += simulation_energy
            simulation_total_energy += simulation_energy

            # 合計用変数に加算する
            total_distance += leg_distance
            total_foc      += (simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_lpg_b + simulation_leg_nh3_ng + simulation_leg_nh3_ef + simulation_leg_methanol_ng + simulation_leg_h2_ng)
            total_eu_actual_foc += simulation_leg_foc / (leg_eu_rate / 100)

    if count_target_voyage > 0:
        # 最終的なCBを算出
        total_GHG = calculate_function.calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
        print(f"レコード作成用データ  imo:{(imo)} eoy_total_lfo:{(total_lfo)}, eoy_total_mgo:{(total_mgo)}, eoy_total_energy:{(total_energy)}")
        eoy_cb    = calculate_function.calc_cb(now_year, total_energy, total_GHG)

        # banking, borrowingを取得
        banking   = float(thisyear_year_total["banking"]["S"]) if thisyear_year_total and "banking" in thisyear_year_total and thisyear_year_total["banking"]["S"] != "" else 0
        borrowing = float(thisyear_year_total["borrowing"]["S"]) if thisyear_year_total and "borrowing" in thisyear_year_total and thisyear_year_total["borrowing"]["S"] != "" else 0

        total_cb  = eoy_cb + borrowing + last_year

        # CB Costの算出
        if float(total_cb) >= 0:
            eoy_cb_cost = 0
        else:
            eoy_cb_cost = abs(float(total_cb)) * 2400 / (total_GHG * 41000) * penalty_factor

        borrowing_limit = calculate_function.calc_borrowing_limit(True, now_year, total_energy)

        # Voyage Planのシミュレーション用データ
        dataset = {
            "imo"            : imo,
            "vessel_name"    : vessel_name,
            "operator"       : thisyear_year_total["year_and_ope"]["S"][4:50] if thisyear_year_total else voyage_plan_list[0]["operator"]["S"],
            "distance"       : round(total_distance),
            "foc"            : round(total_eu_actual_foc),
            "end_of_year"    : round(float(eoy_cb) / 1000000, 1),
            "last_year"      : round(last_year / 1000000, 1),
            "borrowing_limit": round(borrowing_limit / 1000000),
            "borrowing"      : round(borrowing / 1000000, 1),
            "banking"        : round(banking / 1000000, 1),
            "total"          : round(float(total_cb) / 1000000, 1),
            "penalty_factor" : penalty_factor,
            "cost"        : round(eoy_cb_cost, 0)
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

def make_speed_plans_data(imo, vessel_name, year, thisyear_year_total, speed_plan, res_foc_formulas, fuel_oil_type_list,penalty_factor, last_year, ytd_energy):

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

    leg_eu_rate = int(speed_plan[0]["eu_rate"]["S"])

    if res_foc_formulas and leg_eu_rate != 0: 

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
        # Leg内総FOCを算出
        simulation_leg_foc = (ballast_foc + laden_foc) * leg_eu_rate / 100
        
        # 燃料別消費量を算出する
        fuel_list = Util.convertFuelOileStringToList(speed_plan[0]["fuel"]["S"]) 

        for fuel in fuel_list:
            fuel_info_list = fuel.split(',')
            fuel_type = fuel_info_list[0]
            fuel_rate = fuel_info_list[1]

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

            if  fuel_type == "LNG(Otto Diesel Speed)":
                simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                total_lng_ods     += simulation_leg_lng_ods
                simulation_total_lng_ods += simulation_leg_lng_ods
            elif  fuel_type == "LNG(Otto Medium Speed)":
                simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                total_lng_oms     += simulation_leg_lng_oms
                simulation_total_lng_oms += simulation_leg_lng_oms
            elif  fuel_type == "LNG(Otto Slow Speed)":
                simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                total_lng_oss     += simulation_leg_lng_oss
                simulation_total_lng_oss += simulation_leg_lng_oss
            elif fuel_type == "HFO":
                simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                total_hfo          += simulation_leg_hfo
                simulation_total_hfo += simulation_leg_hfo
            elif fuel_type == "LFO":
                simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                total_lfo         += simulation_leg_lfo
                simulation_total_lfo += simulation_leg_lfo
            elif fuel_type == "MDO":
                simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                total_mdo         += simulation_leg_mdo
                simulation_total_mdo += simulation_leg_mdo
            elif fuel_type == "MGO":
                simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                total_mgo         += simulation_leg_mgo
                simulation_total_mgo += simulation_leg_mgo
            elif fuel_type == "LPG(Propane)":
                simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                total_lpg_p         += simulation_leg_lpg_p
                simulation_total_lpg_p += simulation_leg_lpg_p
            elif fuel_type == "LPG(Butane)":
                simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                total_lpg_b         += simulation_leg_lpg_b
                simulation_total_lpg_b += simulation_leg_lpg_b
            elif fuel_type == "NH3(Natural gas)":
                simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                total_nh3_ng         += simulation_leg_nh3_ng
                simulation_total_nh3_ng += simulation_leg_nh3_ng
            elif fuel_type == "NH3(e-fuel)":
                simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                total_nh3_ef         += simulation_leg_nh3_ef
                simulation_total_nh3_ef += simulation_leg_nh3_ef
            elif fuel_type == "Methanol(Natural gas)":
                simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                total_methanol_ng         += simulation_leg_methanol_ng
                simulation_total_methanol_ng += simulation_leg_methanol_ng
            elif fuel_type == "H2(Natural gas)":
                simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100
                total_h2_ng         += simulation_leg_h2_ng
                simulation_total_h2_ng += simulation_leg_h2_ng

        # シミュレーション部分のエネルギー総消費量を算出する
        simulation_energy  = calculate_function.calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
        total_energy      += simulation_energy
        simulation_total_energy += simulation_energy

        # 合計用変数に加算する
        total_distance += total_ballast_laden_distance
        total_foc      += (simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_lpg_b + simulation_leg_nh3_ng + simulation_leg_nh3_ef + simulation_leg_methanol_ng + simulation_leg_h2_ng)
        total_eu_actual_foc += simulation_leg_foc / (leg_eu_rate / 100)

        # CB算出
        total_GHG = calculate_function.calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
        eoy_cb    = calculate_function.calc_cb(now_year, total_energy, total_GHG)

        # banking, borrowingを取得
        banking   = float(thisyear_year_total["banking"]["S"]) if thisyear_year_total and "banking" in thisyear_year_total and thisyear_year_total["banking"]["S"] != "" else 0
        borrowing = float(thisyear_year_total["borrowing"]["S"]) if thisyear_year_total and "borrowing" in thisyear_year_total and thisyear_year_total["borrowing"]["S"] != "" else 0

        total_cb  = eoy_cb + borrowing + last_year
        print(f"imo:{(imo)} total_cb:{(total_cb)}, eoy_cb:{(eoy_cb)}, borrowing:{(borrowing)}, last_year:{(last_year)}")

        # CB Costの算出
        if float(total_cb) >= 0:
            eoy_cb_cost = 0
        else:
            eoy_cb_cost = abs(float(total_cb)) * 2400 / (total_GHG * 41000) * penalty_factor

    borrowing_limit = calculate_function.calc_borrowing_limit(True, year, total_energy)

    # Speed Planのシミュレーション用データ
    dataset = {
        "imo"            : imo,
        "vessel_name"    : vessel_name,
        "operator"       : thisyear_year_total["year_and_ope"]["S"][4:50] if thisyear_year_total else speed_plan[0]["operator"]["S"],
        "distance"       : round(total_distance),
        "foc"            : round(total_eu_actual_foc),
        "end_of_year"    : round(float(eoy_cb) / 1000000, 1),
        "last_year"      : round(last_year / 1000000, 1),
        "borrowing_limit": round(borrowing_limit / 1000000, 1),
        "borrowing"      : round(borrowing / 1000000, 1),
        "banking"        : round(banking / 1000000, 1),
        "total"          : round(float(total_cb) / 1000000, 1),
        "penalty_factor" : penalty_factor,
        "cost"        : round(eoy_cb_cost)
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