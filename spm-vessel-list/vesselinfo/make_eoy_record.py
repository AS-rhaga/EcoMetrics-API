
import math
import json
from datetime import datetime
import ast

from Util import Util
from calculate import calculate_function

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def make_voyage_plans_data(voyage_plan_list, res_foc_formulas, fuel_oil_type_info_list):

    # 変数の設定
    leg_total_time             = 0

    simulation_total_lng_ods     = 0
    simulation_total_lng_oms     = 0
    simulation_total_lng_oss     = 0
    simulation_total_hfo         = 0
    simulation_total_lfo         = 0
    simulation_total_mdo         = 0
    simulation_total_mgo         = 0
    simulation_total_lpg_p       = 0
    simulation_total_lpg_b       = 0
    simulation_total_nh3_ng      = 0
    simulation_total_nh3_ef      = 0
    simulation_total_methanol_ng = 0
    simulation_total_h2_ng       = 0
    simulation_total_co2         = 0
    simulation_total_energy      = 0

    simulation_foc_per_day  = 0
    eoy_foc                 = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    str_now = dt_now.strftime('%Y/%m/%d %H:%M')

    for voyage_plan in voyage_plan_list:

        # 初期化
        simulation_leg_lng_ods     = 0
        simulation_leg_lng_oms     = 0
        simulation_leg_lng_oss     = 0
        simulation_leg_hfo         = 0
        simulation_leg_lfo         = 0
        simulation_leg_mdo         = 0
        simulation_leg_mgo         = 0
        simulation_leg_lpg_p       = 0
        simulation_leg_lpg_b       = 0
        simulation_leg_nh3_ng      = 0
        simulation_leg_nh3_ef      = 0
        simulation_leg_methanol_ng = 0
        simulation_leg_h2_ng       = 0
        simulation_leg_energy      = 0

        # legの開始・終了時刻を取得する
        str_departure_time = voyage_plan["departure_time"]["S"]     # "2024-12-10 12:30"
        str_arrival_time   = voyage_plan["arrival_time"]["S"]       # "2024-12-19 17:30"

        # legの開始・終了時刻からlegの時間を算出する
        dt_departure_time = Util.to_datetime(str_departure_time)
        dt_arrival_time = Util.to_datetime(str_arrival_time)
        leg_total_time = Util.calc_time_diff(dt_departure_time, dt_arrival_time)

        # 各項目を取得
        distance               = float(voyage_plan["distance"]["S"])
        displacement           = voyage_plan["dispracement"]["S"]
        eu_rate                = int(voyage_plan["eu_rate"]["S"])

        # log_speedを算出
        leg_log_speed = 0
        if leg_total_time != 0:
            leg_log_speed = distance / leg_total_time

        if res_foc_formulas:

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
            simulation_leg_foc = simulation_foc_per_hour * leg_total_time * eu_rate / 100
            eoy_foc += simulation_leg_foc

            # 燃料別消費量を算出する
            fuel_list = Util.convertFuelOileStringToList(voyage_plan["fuel"]["S"]) 

            for fuel in fuel_list:
                fuel_info_list = fuel.split(',')
                fuel_type = fuel_info_list[0]
                fuel_rate = int(fuel_info_list[1])

                if  fuel_type == "LNG(Otto Diesel Speed)":
                    simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_lng_ods += simulation_leg_lng_ods
                elif  fuel_type == "LNG(Otto Medium Speed)":
                    simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_lng_oms += simulation_leg_lng_oms
                elif  fuel_type == "LNG(Otto Slow Speed)":
                    simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_lng_oss += simulation_leg_lng_oss
                elif fuel_type == "HFO":
                    simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_hfo += simulation_leg_hfo
                elif fuel_type == "LFO":
                    simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_lfo += simulation_leg_lfo
                elif fuel_type == "MDO":
                    simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_mdo += simulation_leg_mdo
                elif fuel_type == "MGO":
                    simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_mgo += simulation_leg_mgo
                elif fuel_type == "LPG(Propane)":
                    simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_lpg_p += simulation_leg_lpg_p
                elif fuel_type == "LPG(Butane)":
                    simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_lpg_b += simulation_leg_lpg_b
                elif fuel_type == "NH3(Natural gas)":
                    simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_nh3_ng += simulation_leg_nh3_ng
                elif fuel_type == "NH3(e-fuel)":
                    simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_nh3_ef += simulation_leg_nh3_ef
                elif fuel_type == "Methanol(Natural gas)":
                    simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_methanol_ng += simulation_leg_methanol_ng
                elif fuel_type == "H2(Natural gas)":
                    simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100
                    simulation_total_h2_ng += simulation_leg_h2_ng

            # co2排出量（EU Rate考慮済）を算出する
            simulation_leg_co2 = calculate_function.calc_co2(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
            simulation_total_co2 += simulation_leg_co2
            
            simulation_leg_energy = calculate_function.calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
            simulation_total_energy += simulation_leg_energy

        simulation_fuel_list = {
            "simulation_lng_ods"    : simulation_total_lng_ods,
            "simulation_lng_oms"    : simulation_total_lng_oms,
            "simulation_lng_oss"    : simulation_total_lng_oss,
            "simulation_hfo"        : simulation_total_hfo,
            "simulation_lfo"        : simulation_total_lfo,
            "simulation_mdo"        : simulation_total_mdo,
            "simulation_mgo"        : simulation_total_mgo,
            "simulation_lpg_p"      : simulation_total_lpg_p,
            "simulation_lpg_b"      : simulation_total_lpg_b,
            "simulation_nh3_ng"     : simulation_total_nh3_ng,
            "simulation_nh3_ef"     : simulation_total_nh3_ef,
            "simulation_methanol_ng": simulation_total_methanol_ng,
            "simulation_h2_ng"      : simulation_total_h2_ng,
            "simulation_co2"        : simulation_total_co2,
            "simulation_energy"     : simulation_total_energy
        }
        print(f"simulation_fuel_list:{(simulation_fuel_list)}")

    return simulation_fuel_list

def make_speed_plans_data(speed_plan, res_foc_formulas, fuel_oil_type_info_list):

    # 変数の設定
    simulation_leg_lng_ods     = 0
    simulation_leg_lng_oms     = 0
    simulation_leg_lng_oss     = 0
    simulation_leg_hfo         = 0
    simulation_leg_lfo         = 0
    simulation_leg_mdo         = 0
    simulation_leg_mgo         = 0
    simulation_leg_lpg_p       = 0
    simulation_leg_lpg_b       = 0
    simulation_leg_nh3_ng      = 0
    simulation_leg_nh3_ef      = 0
    simulation_leg_methanol_ng = 0
    simulation_leg_h2_ng       = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    str_now = dt_now.strftime('%Y-%m-%dT%H:%M')

    # 最新NoonReportのタイムスタンプ（初期値は適当に古い日付を設定している）
    latest_timestamp = "1990-01-01T00:00:00Z"

    #最新TimeStampをdatetime型に変換
    nr_timestamp_dt = datetime.fromisoformat(latest_timestamp.replace("Z", "+00:00"))
   
    # SpeedPlanのSimulation処理実施   
    # Time to End of Year算出（年末 - 現在）
    year_end = datetime(dt_now.year, 12, 31, 23, 59, 59)
    time_to_end_of_year = calculate_function.calc_time_diff(dt_now, year_end)

    # 航海時間を算出
    sailing_rate = float(speed_plan[0]["salling_rate"]["S"])
    sailing_time = time_to_end_of_year * sailing_rate / 100
    port_time    = time_to_end_of_year - sailing_time
    print(f"sailing_time:{(sailing_time)} port_time:{(port_time)}")

    # Ballast、Ladenそれぞれの航海距離を算出
    displacement_rate    = float(speed_plan[0]["dispracement_rate"]["S"])
    ballast_sailing_time = sailing_time * displacement_rate / 100
    laden_sailing_time   = sailing_time - ballast_sailing_time

    # 必要項目を取得
    eu_rate          = float(speed_plan[0]["eu_rate"]["S"])
    ballast_logspeed = float(speed_plan[0]["log_speed_ballast"]["S"])
    laden_logspeed   = float(speed_plan[0]["log_speed_laden"]["S"])

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
        laden_foc_per_hour = laden_foc_per_day / 24
        # FOC算出
        ballast_foc = ballast_foc_per_hour * ballast_sailing_time
        laden_foc = laden_foc_per_hour * laden_sailing_time
        # Leg内総FOCを算出
        simulation_salling_foc = (ballast_foc + laden_foc) * eu_rate / 100
        simulation_port_foc    = auxiliary_equipment / 24 * port_time * eu_rate / 100
        simulation_leg_foc     = simulation_salling_foc + simulation_port_foc
        print(f"simulation_leg_foc:{(simulation_leg_foc)}")
        
        # 燃料別消費量を算出する
        fuel_list = Util.convertFuelOileStringToList(speed_plan[0]["fuel"]["S"]) 

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

        # co2排出量（EU Rate考慮済）を算出する
        simulation_co2 = calculate_function.calc_co2(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)
        
        # 消費エネルギー量を算出する
        simulation_leg_energy = calculate_function.calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_info_list)

        simulation_fuel_list = {
            "simulation_lng_ods"    : simulation_leg_lng_ods,
            "simulation_lng_oms"    : simulation_leg_lng_oms,
            "simulation_lng_oss"    : simulation_leg_lng_oss,
            "simulation_hfo"        : simulation_leg_hfo,
            "simulation_lfo"        : simulation_leg_lfo,
            "simulation_mdo"        : simulation_leg_mdo,
            "simulation_mgo"        : simulation_leg_mgo,
            "simulation_lpg_p"      : simulation_leg_lpg_p,
            "simulation_lpg_b"      : simulation_leg_lpg_b,
            "simulation_nh3_ng"     : simulation_leg_nh3_ng,
            "simulation_nh3_ef"     : simulation_leg_nh3_ef,
            "simulation_methanol_ng": simulation_leg_methanol_ng,
            "simulation_h2_ng"      : simulation_leg_h2_ng,
            "simulation_co2"        : simulation_co2,
            "simulation_energy"     : simulation_leg_energy
        }
        print(f"simulation_fuel_list:{(simulation_fuel_list)}")

    return simulation_fuel_list
