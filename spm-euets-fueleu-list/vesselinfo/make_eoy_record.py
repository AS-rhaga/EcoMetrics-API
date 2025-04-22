
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

def make_voyage_plans_data(imo, thisyear_year_total, voyage_plan_list, res_foc_formulas, fuel_oil_type_list, ytd_energy, year, total_year_total_list):

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

    # dataset           = []
    # count_target_voyage = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    # str_now = dt_now.strftime('%Y-%m-%dT%H:%M')
    str_now = dt_now.strftime('%Y/%m/%d %H:%M')

    if thisyear_year_total:
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

        leg_part_time           = 0

        # legの開始・終了時刻を取得する
        str_departure_time = voyage_plan_list[i]["departure_time"]["S"]     # "2024/12/10 12:30"
        str_arrival_time   = voyage_plan_list[i]["arrival_time"]["S"]       # "2024/12/19 17:30"

        # legの開始・終了時刻からlegの時間を算出する
        dt_departure_time = Util.to_datetime(str_departure_time)
        # test_departure_time = dt_departure_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        dt_arrival_time = Util.to_datetime(str_arrival_time)
        # test_arrival_time = dt_arrival_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        # print(f"departure_time: {(test_departure_time)}, arrival_time: {(test_arrival_time)}")     
        leg_total_time = Util.calc_time_diff(dt_departure_time, dt_arrival_time)

        # 各項目を取得
        operator     = voyage_plan_list[i]["operator"]["S"]
        displacement = voyage_plan_list[i]["dispracement"]["S"]
        leg_distance = float(voyage_plan_list[i]["distance"]["S"])
        leg_eu_rate  = int(voyage_plan_list[i]["eu_rate"]["S"])

        # log_speedを算出
        leg_log_speed = 0
        if leg_total_time != 0:
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

            # シミュレーション部分で実際に排出したco2を算出する
            simulation_leg_co2 = calculate_function.calc_co2(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
            # シミュレーション部分のEUAを算出する
            simulation_leg_eua = calculate_function.calc_eua(now_year, simulation_leg_co2)

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

            
        # シミュレーション対象のvoyageがある場合のみ
        if operator_total["voyage_count"] > 0:

            total_distance      = operator_total["total_distance"] + ytd_distance
            total_lng_ods       = operator_total["total_lng_ods"]
            total_lng_oms       = operator_total["total_lng_oms"] + ytd_lng_oms
            total_lng_oss       = operator_total["total_lng_oss"]
            total_hfo           = operator_total["total_hfo"] + ytd_hfo
            total_lfo           = operator_total["total_lfo"] + ytd_lfo
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

            # 最終的なEUA, CBを算出
            total_co2 = calculate_function.calc_co2(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
            total_eua = calculate_function.calc_eua(year, total_co2)
            total_GHG = calculate_function.calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
            eoy_cb    = calculate_function.calc_cb(now_year, total_energy, total_GHG)

            total_cb  = eoy_cb

            # CB Costの算出
            eoy_cb_cost = 0
            if total_cb < 0 and total_GHG != 0:
                eoy_cb_cost    = abs(float(total_cb)) * 2400 / (total_GHG * 41000)

            # Voyage Planのシミュレーション用データ
            dataset = {
                "operator"       : operator_total["operator"],
                "eoy_distance"   : total_distance,
                "eoy_foc"        : total_eu_actual_foc,
                "eoy_eua"        : total_eua,
                "eoy_cb"         : total_cb,
                "eoy_cb_cost"    : eoy_cb_cost
            }
            dataset_list.append(dataset)
    
    return dataset_list

def make_speed_plans_data(thisyear_year_total, speed_plan, res_foc_formulas, fuel_oil_type_list, ytd_energy, year, total_year_total_list):

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
    eoy_cb_cost       = 0
    total_cb          = 0

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

    leg_eu_rate = int(speed_plan[0]["eu_rate"]["S"])
    operator    = speed_plan[0]["operator"]["S"]

    # if res_foc_formulas and leg_eu_rate != 0:    ←EU Rateゼロのレグしかない時にEnd of Yearが作られない
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
        laden_foc_per_hour   = laden_foc_per_day / 24
        port_foc_per_hour    = auxiliary_equipment / 24
        # FOC算出
        ballast_foc = ballast_foc_per_hour * ballast_sailing_time
        laden_foc = laden_foc_per_hour * laden_sailing_time
        # 航海Leg内総FOCを算出
        simulation_leg_actual_foc = ballast_foc + laden_foc
        simulation_leg_foc        = simulation_leg_actual_foc * leg_eu_rate / 100
        # 停泊中の総FOCを算出
        simulation_port_actual_foc = port_foc_per_hour * port_time
        simulation_port_foc = simulation_port_actual_foc * leg_eu_rate / 100
        # 総FOCを算出
        total_actual_foc = simulation_leg_actual_foc + simulation_port_actual_foc
        total_FOC_speed  = simulation_leg_foc + simulation_port_foc
        
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
            elif  fuel_type == "LNG(Otto Medium Speed)":
                simulation_leg_lng_oms = total_FOC_speed * int(fuel_rate) / 100
                total_lng_oms     += simulation_leg_lng_oms
            elif  fuel_type == "LNG(Otto Slow Speed)":
                simulation_leg_lng_oss = total_FOC_speed * int(fuel_rate) / 100
                total_lng_oss     += simulation_leg_lng_oss
            elif fuel_type == "HFO":
                simulation_leg_hfo = total_FOC_speed * int(fuel_rate) / 100
                total_hfo          += simulation_leg_hfo
            elif fuel_type == "LFO":
                simulation_leg_lfo = total_FOC_speed * int(fuel_rate) / 100
                total_lfo         += simulation_leg_lfo
            elif fuel_type == "MDO":
                simulation_leg_mdo = total_FOC_speed * int(fuel_rate) / 100
                total_mdo         += simulation_leg_mdo
            elif fuel_type == "MGO":
                simulation_leg_mgo = total_FOC_speed * int(fuel_rate) / 100
                total_mgo         += simulation_leg_mgo
            elif fuel_type == "LPG(Propane)":
                simulation_leg_lpg_p = total_FOC_speed * int(fuel_rate) / 100
                total_lpg_p         += simulation_leg_lpg_p
            elif fuel_type == "LPG(Butane)":
                simulation_leg_lpg_b = total_FOC_speed * int(fuel_rate) / 100
                total_lpg_b         += simulation_leg_lpg_b
            elif fuel_type == "NH3(Natural gas)":
                simulation_leg_nh3_ng = total_FOC_speed * int(fuel_rate) / 100
                total_nh3_ng         += simulation_leg_nh3_ng
            elif fuel_type == "NH3(e-fuel)":
                simulation_leg_nh3_ef = total_FOC_speed * int(fuel_rate) / 100
                total_nh3_ef         += simulation_leg_nh3_ef
            elif fuel_type == "Methanol(Natural gas)":
                simulation_leg_methanol_ng = total_FOC_speed * int(fuel_rate) / 100
                total_methanol_ng         += simulation_leg_methanol_ng
            elif fuel_type == "H2(Natural gas)":
                simulation_leg_h2_ng = total_FOC_speed * int(fuel_rate) / 100
                total_h2_ng         += simulation_leg_h2_ng

        simulation_leg_co2 = calculate_function.calc_co2(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)

        # シミュレーション部分のEUAを算出する
        simulation_leg_eua = calculate_function.calc_eua(now_year, simulation_leg_co2)

        # シミュレーション部分のエネルギー総消費量を算出する
        simulation_energy  = calculate_function.calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
        total_energy      += simulation_energy

        # 合計用変数に加算する
        total_distance += total_ballast_laden_distance
        total_foc      += (simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_lpg_b + simulation_leg_nh3_ng + simulation_leg_nh3_ef + simulation_leg_methanol_ng + simulation_leg_h2_ng)
        total_eu_actual_foc += total_actual_foc
        total_eua      += simulation_leg_eua


        # CB算出
        total_GHG = calculate_function.calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
        eoy_cb    = calculate_function.calc_cb(now_year, total_energy, total_GHG)
        print(f"simulation_energy:{simulation_energy}, ytd_energy:{ytd_energy}, total_energy:{total_energy}, total_GHG:{total_GHG}")

        total_cb  = eoy_cb

        # CB Costの算出
        eoy_cb_cost = 0
        if total_cb < 0 and total_GHG != 0:
            eoy_cb_cost    = abs(float(total_cb)) * 2400 / (total_GHG * 41000)

        # Voyage Planのシミュレーション用データ
        dataset = {
            "eoy_distance"   : total_distance,
            "eoy_foc"        : total_eu_actual_foc,
            "eoy_eua"        : total_eua,
            "eoy_cb"         : total_cb,
            "eoy_cb_cost"    : eoy_cb_cost
        }

    return dataset
