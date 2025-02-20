
from time import sleep
from botocore.errorfactory import ClientError
import boto3
import json
from datetime import datetime
import ast
import os

from dynamodb import select
from calculate import calculate_function
from vesselinfo import make_eoy_record

# 同一imo, 複数オペレーターのyear-totalデータを合計
def sum_year_total(imo, year, GHG_Max, ytd_distance, res_foc_formulas, fuel_oil_type_info_list):

    # 合計用変数
    ytd_lng    = 0
    ytd_hfo    = 0
    ytd_lfo    = 0
    ytd_mdo    = 0
    ytd_mgo    = 0
    ytd_co2    = 0
    ytd_eua    = 0
    ytd_energy = 0
    ytd_cb     = 0

    eoy_distance    = 0
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

    eoy_co2    = 0
    eoy_eua    = 0
    eoy_energy = 0
    eoy_cb     = 0


    # year-totalのレコードリストを取得する
    res_year_total_list = select.get_year_total_by_year(imo, year)

    # この年のEU-ETS対象割合を確認
    eu_ets_rate = 0
    if year == "2024":
        eu_ets_rate = 40
    elif year == "2025":
        eu_ets_rate = 70
    else:
        eu_ets_rate = 100

    # 該当imoでyear-totalが登録されている場合
    if res_year_total_list:

        # year-totalのリストでループ
        for year_total in res_year_total_list:

            lng = float(year_total["total_lng"]["S"])
            hfo = float(year_total["total_hfo"]["S"])
            lfo = float(year_total["total_lfo"]["S"])
            mdo = float(year_total["total_mdo"]["S"])
            mgo = float(year_total["total_mgo"]["S"])
            eua = float(year_total["eua"]["S"])
            cb  = float(year_total["cb"]["S"])

            # CBからco2排出量（EU Rate考慮済）を算出する
            co2 = eua / (eu_ets_rate / 100)

            # CBから使用したエネルギー量を逆算する
            energy     = calculate_function.calc_energy(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
            
            # Year to Dateの合計用変数に加算
            ytd_lng    += lng
            ytd_hfo    += hfo
            ytd_lfo    += lfo
            ytd_mdo    += mdo
            ytd_mgo    += mgo
            ytd_co2    += co2
            ytd_eua    += eua
            ytd_energy += energy

        # Year to DateのCBを算出する
        ytd_GHG_Actual = calculate_function.calc_GHG_Actual(0, ytd_lng, 0, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
        ytd_cb = calculate_function.calc_cb(ytd_energy, ytd_GHG_Actual, GHG_Max)

    # シミュレーション用リストを取得する
    res_simulation_voyage_list = select.get_simulation_voyage(imo, year)
    res_simulation_speed       = select.get_simulation_speed(imo, year)
    # print(f"res_simulation_voyage_list:{(res_simulation_voyage_list)}, res_simulation_speed:{(res_simulation_speed)}")

    # voyageかspeedのどちらを使うかフラグで管理する
    voyage_flag = 0
    speed_flag = 0
    if len(res_simulation_voyage_list) > 0 and res_simulation_voyage_list[0]["flag"]["S"] == "1":
        voyage_flag = 1
    elif len(res_simulation_speed) > 0 and res_simulation_speed[0]["flag"]["S"] == "1":
        speed_flag = 1

    # シミュレーション用リストから使用予定の燃料消費量を算出する
    # FOC Formulasが登録されている場合
    if res_foc_formulas and (voyage_flag == 1 or speed_flag == 1):
        print(f"res_simulation_voyage_list:{(res_simulation_voyage_list)}, res_simulation_speed:{(res_simulation_speed)}")
        simulation_fuel_list = []

        # voyage-planとspeed-planのどちらを使うか判断する
        if voyage_flag == 1:
            print("simulation plan: VOYAGE")
            simulation_fuel_list = make_eoy_record.make_voyage_plans_data(res_simulation_voyage_list, res_foc_formulas, fuel_oil_type_info_list)
            print(f"voyage_planのsimulation_fuel_list:{(simulation_fuel_list)}")
        elif speed_flag == 1:
            print("simulation plan: SPEED")
            simulation_fuel_list = make_eoy_record.make_speed_plans_data(res_simulation_speed, res_foc_formulas, fuel_oil_type_info_list)
        else:
            print(f"どちらのsimulation planも使わない")

        # シミュレーションデータ内での航行距離、各燃料の消費量とエネルギー量を合計する
        eoy_lng_ods     += simulation_fuel_list["simulation_lng_ods"]
        eoy_lng_oms     += (simulation_fuel_list["simulation_lng_oms"] + ytd_lng)
        eoy_lng_oss     += simulation_fuel_list["simulation_lng_oss"]
        eoy_hfo         += (simulation_fuel_list["simulation_hfo"] + ytd_hfo)
        eoy_lfo         += (simulation_fuel_list["simulation_lfo"] + ytd_lfo)
        eoy_mdo         += (simulation_fuel_list["simulation_mdo"] + ytd_mdo)
        eoy_mgo         += (simulation_fuel_list["simulation_mgo"] + ytd_mgo)
        eoy_lpg_p       += simulation_fuel_list["simulation_lpg_p"]
        eoy_lpg_b       += simulation_fuel_list["simulation_lpg_b"]
        eoy_nh3_ng      += simulation_fuel_list["simulation_nh3_ng"]
        eoy_nh3_ef      += simulation_fuel_list["simulation_nh3_ef"]
        eoy_methanol_ng += simulation_fuel_list["simulation_methanol_ng"]
        eoy_h2_ng       += simulation_fuel_list["simulation_h2_ng"]
        eoy_co2         += (simulation_fuel_list["simulation_co2"] + ytd_co2)
        eoy_energy      += (simulation_fuel_list["simulation_energy"] + ytd_energy)

        # End of YearのEUAを算出する
        eoy_eua = eoy_co2 * eu_ets_rate / 100

        # End of YearのCBを算出する
        eoy_GHG_Actual = calculate_function.calc_GHG_Actual(eoy_lng_ods, eoy_lng_oms, eoy_lng_oss, eoy_hfo, eoy_lfo, eoy_mdo, eoy_mgo, eoy_lpg_p, eoy_lpg_b, eoy_nh3_ng, eoy_nh3_ef, eoy_methanol_ng, eoy_h2_ng, fuel_oil_type_info_list)
        print(f"eoy_energy:{(eoy_energy)}, eoy_GHG_Actual:{(eoy_GHG_Actual)}, GHG_Max:{(GHG_Max)}")
        eoy_cb         = calculate_function.calc_cb(eoy_energy, eoy_GHG_Actual, GHG_Max)

    # シミュレーションデータが無い、または計算に必要な情報が不足している場合
    else:
        print("simulation plan: None")
        eoy_eua = ytd_eua
        eoy_cb  = ytd_cb

    data = {
        "ytd_eua": str(round(ytd_eua)),
        "eoy_eua": str(round(eoy_eua)),
        "ytd_cb" : str(round(ytd_cb * 10**(-6), 1)),
        "eoy_cb" : str(round(eoy_cb * 10**(-6), 1))
    }

    return data
