
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

def make_voyage_plans_data(imo, year, thisyear_year_total_list, voyage_plan, res_foc_formulas, fuel_oil_type_list):

    # 変数の設定
    distance  = 0
    lng       = 0
    hfo       = 0
    lfo       = 0
    mdo       = 0
    mgo       = 0
    foc       = 0
    cb        = 0
    cb_cost   = 0
    leg_rate                   = 0
    leg_total_time             = 0
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

    simulation_foc_per_day  = 0
    str_last_year           = ""
    eoy_foc                 = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    str_now = dt_now.strftime('%Y-%m-%dT%H:%M')

    # legの開始・終了時刻を取得する
    str_departure_time = voyage_plan["departure_time"]["S"]     # "2024-12-10 12:30"
    str_arrival_time   = voyage_plan["arrival_time"]["S"]       # "2024-12-19 17:30"

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
        print(f"departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは完全に先時刻")
        return_departure_time = str_departure_time
        return_arrival_time   = str_arrival_time
        return_leg_total_time = leg_total_time
        leg_rate              = 1

    elif str_now <= str_arrival_time:
        print(f"departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは現在進行中")
        # 表示する範囲の時間を算出し、leg全体に対する割合を求める。
        dt_time_from  = Util.to_datetime(str_now)
        dt_time_to    = Util.to_datetime(str_arrival_time)
        leg_part_time = Util.calc_time_diff(dt_time_from, dt_time_to)
        leg_rate              = float(leg_part_time / leg_total_time)

    else:
        print(f"departure_time: {(str_departure_time)}, arrival_time: {(str_arrival_time)} → このlegは完結済")
        # 以降の処理は行わず、次のlegを確認

    # 各項目を取得
    operator               = voyage_plan["operator"]["S"]
    displacement           = float(voyage_plan["dispracement"]["S"])
    voyage_distance        = float(voyage_plan["distance"]["S"])
    eu_rate                = int(voyage_plan["eu_rate"]["S"])

    # log_speedを算出
    leg_log_speed = voyage_distance / leg_total_time

    if res_foc_formulas:

        # Ballast、Ladenどちらか判断して、FOCを算出
        if displacement == "Ballast":
            # Ballast用の計算パラメータを取得し、1日当たりのFOCを算出
            calc_balast_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
            ballast_alpha = calc_balast_param_list[0]
            ballast_a = calc_balast_param_list[1]
            ballast_c = calc_balast_param_list[2]
            simulation_foc_per_day = ballast_alpha * leg_log_speed ** ballast_a + ballast_c
        else:
            # Laden用の計算パラメータを取得し、1日当たりのFOCを算出
            calc_laden_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
            laden_alpha = calc_laden_param_list[0]
            laden_a = calc_laden_param_list[1]
            laden_c = calc_laden_param_list[2]
            simulation_foc_per_day = laden_alpha * leg_log_speed ** laden_a + laden_c

        # 1時間あたりのFOC算出
        simulation_foc_per_hour = simulation_foc_per_day / 24
        # Leg内総FOCを算出
        simulation_leg_foc = simulation_foc_per_hour * leg_total_time * leg_rate
        eoy_foc += simulation_leg_foc

        # 燃料別消費量を算出する
        fuel_list = ast.literal_eval(voyage_plan["fuel"]["S"]) 

        for fuel in fuel_list:
            fuel_info_list = ast.literal_eval(fuel)
            fuel_type = fuel_info_list[0]
            fuel_rate = fuel_info_list[1]

            if  fuel_type == "LNG(Otto Diesel Speed)":
                simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lng_ods
            elif  fuel_type == "LNG(Otto Medium Speed)":
                simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lng_oms
            elif  fuel_type == "LNG(Otto Slow Speed)":
                simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lng_oss
            elif fuel_type == "HFO":
                simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_hfo
            elif fuel_type == "LFO":
                simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lfo
            elif fuel_type == "MDO":
                simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_mdo
            elif fuel_type == "MGO":
                simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_mgo
            elif fuel_type == "LPG(Propane)":
                simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lpg_p
            elif fuel_type == "LPG(Butane)":
                simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lpg_b
            elif fuel_type == "NH3(Natural gas)":
                simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_nh3_ng
            elif fuel_type == "NH3(e-fuel)":
                simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_nh3_ef
            elif fuel_type == "Methanol(Natural gas)":
                simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_methanol_ng
            elif fuel_type == "H2(Natural gas)":
                simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_h2_ng

        # シミュレーションvoyageを合わせたCBを算出する
        # 実測データの中から同じオペレーターのレコードを探して、各消費量を足し合わせる。
        for this_year_rec in thisyear_year_total_list:
            if this_year_rec["operator"] == operator:
                distance  = float(this_year_rec["distance"]["S"])
                lng       = float(this_year_rec["total_lng"]["S"])
                hfo       = float(this_year_rec["total_hfo"]["S"])
                lfo       = float(this_year_rec["total_lfo"]["S"])
                mdo       = float(this_year_rec["total_mdo"]["S"])
                mgo       = float(this_year_rec["total_mgo"]["S"])
                foc       = float(this_year_rec["total_foc"]["S"])
                cb        = float(this_year_rec["cb"]["S"])

                # CBからエネルギー消費量を逆算する
                GHG_Max    = calculate_function.calc_GHG_Max(year)
                GHG_Actual = calculate_function.calc_GHG_Actual(lng, 0, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
                ytd_energy = cb / (GHG_Max - GHG_Actual)

                # 実測データを足し合わせる前に、シミュレーションlegのエネルギー消費量を算出する
                simulation_leg_energy  = calculate_function.calc_energy(eu_rate, lng, 0, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)

                # 実測データを足し合わせる
                simulation_leg_lng_ods += lng
                simulation_leg_hfo += hfo
                simulation_leg_lfo += lfo
                simulation_leg_mdo += mdo
                simulation_leg_mgo += mgo
                eoy_foc = simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo

        eoy_GHG    = calculate_function.calc_GHG_Actual(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
        eoy_energy = ytd_energy + simulation_leg_energy
        eoy_cb     = calculate_function.calc_cb(year, eoy_energy, eoy_GHG)

        # CB Costの算出
        if eoy_cb >= 0:
            eoy_cb_cost = 0

        else:
            # ペナルティーファクターを調べる
            # 同一imoのyear-totalテーブルを取得（複数オペになったらどうする？）
            res_year_total_list    = select.get_year_total(imo)
            year_total_list_sorted = sorted(res_year_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

            # 今年を含め。、直近何年連続で罰金フラグが立っているかを確認する
            flag_count = 0
            for year in year_total_list_sorted:
                fine_flag = year["fine_flag"]["S"]
                if fine_flag == "1":
                    flag_count += 1
                else:
                    break

                # 去年分のyear-totalレコードを確認
                if year["year_and_ope"]["S"][0:4] == str(int(year) - 1):
                    last_year = float(year["banking"]["S"]) - float(year["borrowing"]["S"]) * 1.1
                    str_last_year = str(last_year)

            penalty_factor = 1 + (flag_count - 1) / 10
            eoy_cb_cost = abs(eoy_cb) * 2400 / (GHG_Actual * 41000) * penalty_factor

        # Voyage Planのシミュレーション用データ
        dataset = {
            "imo"            : imo,
            "operator"       : operator,
            "eoy_distance"   : distance + voyage_distance,
            "eoy_foc"        : str(round(eoy_foc, 0)),
            "eoy_cb"         : str(eoy_cb + last_year),
            "eoy_cb_cost"    : str(round(eoy_cb_cost, 0))
        }
        eoy_fuel_list = {
            "eoy_lng_ods"    : lng + simulation_leg_lng_ods,
            "eoy_lng_oms"    : simulation_leg_lng_oms,
            "eoy_lng_oss"    : simulation_leg_lng_oss,
            "eoy_hfo"        : hfo + simulation_leg_hfo,
            "eoy_lfo"        : lfo + simulation_leg_lfo,
            "eoy_mdo"        : mdo + simulation_leg_mdo,
            "eoy_mgo"        : mgo + simulation_leg_mgo,
            "eoy_lpg_p"      : simulation_leg_lpg_p,
            "eoy_lpg_b"      : simulation_leg_lpg_b,
            "eoy_nh3_ng"     : simulation_leg_nh3_ng,
            "eoy_nh3_ef"     : simulation_leg_nh3_ef,
            "eoy_mathanol_ng": simulation_leg_methanol_ng,
            "eoy_h2_ng"      : simulation_leg_h2_ng,
            "eoy_energy"     : eoy_energy
        }

    return dataset, eoy_fuel_list

def make_speed_plans_data(imo, year, thisyear_year_total_list, speed_plan, res_foc_formulas, fuel_oil_type_list):

    # 変数の設定
    distance  = 0
    lng       = 0
    hfo       = 0
    lfo       = 0
    mdo       = 0
    mgo       = 0
    foc       = 0
    cb        = 0
    cb_cost   = 0
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
    simulation_foc_per_day  = 0
    str_last_year           = ""

    # 合計用変数を設定する。
    eoy_distance = 0
    eoy_foc      = 0
    eoy_cb_cost  = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    str_now = dt_now.strftime('%Y-%m-%dT%H:%M')

    # シミュレーション用スピードプランから必要項目を取得
    operator        = speed_plan[0]["operator"]["S"]
    voyage_distance = float(speed_plan["distance"]["S"])

    eoy_distance += voyage_distance

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
    sailing_time = time_to_end_of_year * sailing_rate

    # Ballast、Ladenそれぞれの航海距離を算出
    displacement_rate    = float(speed_plan[0]["dispracement_rate"]["S"])
    ballast_sailing_time = sailing_time * displacement_rate
    laden_sailing_time   = sailing_time - ballast_sailing_time

    # 時間×速さで距離を算出
    ballast_logspeed = float(speed_plan[0]["log_speed_ballast"]["S"])
    laden_logspeed   = float(speed_plan[0]["log_speed_laden"]["S"])
    ballast_ditance  = ballast_sailing_time * ballast_logspeed
    laden_distance    = laden_sailing_time * laden_logspeed

    # BallastDisancen、LadenDistanceを加算
    total_ballast_laden_distance = ballast_ditance + laden_distance
    eoy_distance += total_ballast_laden_distance

    eu_rate                = int(speed_plan[0]["eu_rate"]["S"])

    if res_foc_formulas: 

        # Ballast用の計算パラメータを取得し、1日当たりのFOCを算出
        calc_balast_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
        ballast_alpha = calc_balast_param_list[0]
        ballast_a = calc_balast_param_list[1]
        ballast_c = calc_balast_param_list[2]
        ballast_foc_per_day = ballast_alpha * ballast_logspeed ** ballast_a + ballast_c
        # Laden用の計算パラメータを取得し、1日当たりのFOCを算出
        calc_laden_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
        laden_alpha = calc_laden_param_list[0]
        laden_a = calc_laden_param_list[1]
        laden_c = calc_laden_param_list[2]
        laden_foc_per_day = laden_alpha * laden_logspeed ** laden_a + laden_c

        # 1時間あたりのFOC算出
        ballast_foc_per_hour = ballast_foc_per_day / 24
        laden_foc_per_hour = laden_foc_per_day / 24
        # FOC算出
        ballast_foc = ballast_foc_per_hour * ballast_sailing_time
        laden_foc = laden_foc_per_hour * ballast_sailing_time
        # Leg内総FOCを算出
        simulation_leg_foc = ballast_foc + laden_foc
        
        # 燃料別消費量を算出する
        fuel_list = ast.literal_eval(speed_plan[0]["fuel"]["S"]) 

        for fuel in fuel_list:
            fuel_info_list = ast.literal_eval(fuel)
            fuel_type = fuel_info_list[0]
            fuel_rate = fuel_info_list[1]

            if  fuel_type == "LNG(Otto Diesel Speed)":
                simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lng_ods
            elif  fuel_type == "LNG(Otto Medium Speed)":
                simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lng_oms
            elif  fuel_type == "LNG(Otto Slow Speed)":
                simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lng_oss
            elif fuel_type == "HFO":
                simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_hfo
            elif fuel_type == "LFO":
                simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lfo
            elif fuel_type == "MDO":
                simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_mdo
            elif fuel_type == "MGO":
                simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_mgo
            elif fuel_type == "LPG(Propane)":
                simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lpg_p
            elif fuel_type == "LPG(Butane)":
                simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_lpg_b
            elif fuel_type == "NH3(Natural gas)":
                simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_nh3_ng
            elif fuel_type == "NH3(e-fuel)":
                simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_nh3_ef
            elif fuel_type == "Methanol(Natural gas)":
                simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_methanol_ng
            elif fuel_type == "H2(Natural gas)":
                simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100
                eoy_foc += simulation_leg_h2_ng

        # シミュレーションvoyageを合わせたCBを算出する
        # 実測データの中から同じオペレーターのレコードを探して、各消費量を足し合わせる。
        for this_year_rec in thisyear_year_total_list:
            if this_year_rec["operator"] == operator:
                distance  = float(this_year_rec["distance"]["S"])
                lng       = float(this_year_rec["total_lng"]["S"])
                hfo       = float(this_year_rec["total_hfo"]["S"])
                lfo       = float(this_year_rec["total_lfo"]["S"])
                mdo       = float(this_year_rec["total_mdo"]["S"])
                mgo       = float(this_year_rec["total_mgo"]["S"])
                eua       = float(this_year_rec["eua"]["S"])
                foc       = float(this_year_rec["total_foc"]["S"])
                cb        = float(this_year_rec["cb"]["S"])

                # CBからエネルギー消費量を逆算する
                GHG_Max    = calculate_function.calc_GHG_Max(year)
                GHG_Actual = calculate_function.calc_GHG_Actual(lng, 0, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
                ytd_energy = cb / (GHG_Max - GHG_Actual)

                # 実測データを足し合わせる前に、シミュレーションlegのエネルギー消費量を算出する
                simulation_leg_energy  = calculate_function.calc_energy(eu_rate, lng, 0, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)

                # 実測データを足し合わせる
                simulation_leg_lng_ods += lng
                simulation_leg_hfo += hfo
                simulation_leg_lfo += lfo
                simulation_leg_mdo += mdo
                simulation_leg_mgo += mgo
                eoy_foc = simulation_leg_lng_ods + simulation_leg_lng_oms + simulation_leg_lng_oss + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo

        eoy_GHG    = calculate_function.calc_GHG_Actual(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
        eoy_energy = ytd_energy + simulation_leg_energy
        eoy_cb     = calculate_function.calc_cb(year, eoy_energy, eoy_GHG)

        # CB Costの算出
        if eoy_cb >= 0:
            eoy_cb_cost = 0
        else:
            # ペナルティーファクターを調べる
            # 同一imoのyear-totalテーブルを取得（複数オペになったらどうする？）
            res_year_total_list    = select.get_year_total(imo)
            year_total_list_sorted = sorted(res_year_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

            # 今年を含め。、直近何年連続で罰金フラグが立っているかを確認する
            flag_count = 0
            for year in year_total_list_sorted:
                fine_flag = year["fine_flag"]["S"]
                if fine_flag == "1":
                    flag_count += 1
                else:
                    break

                # 去年分のyear-totalレコードを確認
                if year["year_and_ope"]["S"][0:4] == str(int(year) - 1):
                    last_year = float(year["banking"]["S"]) - float(year["borrowing"]["S"]) * 1.1

            penalty_factor = 1 + (flag_count - 1) / 10
            eoy_cb_cost = abs(eoy_cb) * 2400 / (GHG_Actual * 41000) * penalty_factor

        # Voyage Planのシミュレーション用データ
        dataset = {
            "imo"            : imo,
            "operator"       : operator,
            "eoy_distance"   : distance + voyage_distance,
            "eoy_eua"        : eua,
            "eoy_foc"        : str(round(eoy_foc, 0)),
            "eoy_cb"         : str(eoy_cb + last_year),
            "eoy_cb_cost"    : str(round(eoy_cb_cost, 0))
        }
        eoy_fuel_list = {
            "eoy_lng_ods"    : lng + simulation_leg_lng_ods,
            "eoy_lng_oms"    : simulation_leg_lng_oms,
            "eoy_lng_oss"    : simulation_leg_lng_oss,
            "eoy_hfo"        : hfo + simulation_leg_hfo,
            "eoy_lfo"        : lfo + simulation_leg_lfo,
            "eoy_mdo"        : mdo + simulation_leg_mdo,
            "eoy_mgo"        : mgo + simulation_leg_mgo,
            "eoy_lpg_p"      : simulation_leg_lpg_p,
            "eoy_lpg_b"      : simulation_leg_lpg_b,
            "eoy_nh3_ng"     : simulation_leg_nh3_ng,
            "eoy_nh3_ef"     : simulation_leg_nh3_ef,
            "eoy_mathanol_ng": simulation_leg_methanol_ng,
            "eoy_h2_ng"      : simulation_leg_h2_ng,
            "eoy_energy"     : eoy_energy
        }

    return dataset, eoy_fuel_list
