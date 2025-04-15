
import calendar
import math
import json
from datetime import datetime
import ast
import re

import auth
from dynamodb import select, insert

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# FuelOilTypeテーブルより、CII算出用の各燃料のEmissionFactorを取得する
FUELOILTYPE_hfo = select.get_fuel_oil_type("HFO")
FUELOILTYPE_lfo = select.get_fuel_oil_type("LFO")
FUELOILTYPE_mdo = select.get_fuel_oil_type("MDO")
FUELOILTYPE_mgo = select.get_fuel_oil_type("MGO")
FUELOILTYPE_lng_medium = select.get_fuel_oil_type("LNG(Otto Medium Speed)")
FUELOILTYPE_lng_slow = select.get_fuel_oil_type("LNG(Otto Slow Speed)")
FUELOILTYPE_lng_diesel = select.get_fuel_oil_type("LNG(Otto Diesel Speed)")
FUELOILTYPE_lpg_butane = select.get_fuel_oil_type("LPG(Butane)")
FUELOILTYPE_lpg_propane = select.get_fuel_oil_type("LPG(Propane)")
FUELOILTYPE_h2 = select.get_fuel_oil_type("H2(Natural gas)")
FUELOILTYPE_nh3_ng = select.get_fuel_oil_type("NH3(Natural gas)")
FUELOILTYPE_methanol = select.get_fuel_oil_type("Methanol(Natural gas)")
FUELOILTYPE_nh3_efuel = select.get_fuel_oil_type("NH3(e-fuel)")
__co2_factor_hfo = float(FUELOILTYPE_hfo[0]["emission_factor"]["S"])
__co2_factor_lfo = float(FUELOILTYPE_lfo[0]["emission_factor"]["S"])
__co2_factor_mdo = float(FUELOILTYPE_mdo[0]["emission_factor"]["S"])
__co2_factor_mgo = float(FUELOILTYPE_mgo[0]["emission_factor"]["S"])
__co2_factor_lng_medium = float(FUELOILTYPE_lng_medium[0]["emission_factor"]["S"])
__co2_factor_lng_slow = float(FUELOILTYPE_lng_slow[0]["emission_factor"]["S"])
__co2_factor_lng_diesel = float(FUELOILTYPE_lng_diesel[0]["emission_factor"]["S"])
__co2_factor_lpg_butane = float(FUELOILTYPE_lpg_butane[0]["emission_factor"]["S"])
__co2_factor_lpg_propane = float(FUELOILTYPE_lpg_propane[0]["emission_factor"]["S"])
__co2_factor_h2 = float(FUELOILTYPE_h2[0]["emission_factor"]["S"])
__co2_factor_nh3_ng = float(FUELOILTYPE_nh3_ng[0]["emission_factor"]["S"])
__co2_factor_methanol = float(FUELOILTYPE_methanol[0]["emission_factor"]["S"])
__co2_factor_nh3_efuel = float(FUELOILTYPE_nh3_efuel[0]["emission_factor"]["S"])

# datetime型のstartとendの時間差を返却。30分以上の場合は繰り上がり。
def calc_time_diff(start_time, end_time):

    return_val = 0
                        
    time_difference = end_time - start_time
    hours_difference = time_difference.total_seconds() / 3600

    # 30分以上で繰り上げ
    if hours_difference % 1 >= 0.5:
        return_val = math.ceil(hours_difference)  # 繰り上げ
    else:
        return_val = math.floor(hours_difference)  # 切り捨て
    
    return return_val

# 燃料別のCO2排出量を算出し、合算したものを返却
def calc_fuel_total_co2(fuel_list, leg_total_FOC):

    fuel_total_co2 = 0
        
    for fuel in fuel_list:
        
        fuel_info_list = fuel.split(',')
        fuel_name = fuel_info_list[0]
        fuel_rate = int(fuel_info_list[1])

        # 燃料別FOC算出
        tmp_fuel_foc =  leg_total_FOC * (fuel_rate / 100)

        # 燃料ごとの係数を掛けて、CO2排出量を算出⇒総CO2排出量（予測値）に加算
        if fuel_name == "HFO":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_hfo
        elif fuel_name == "LFO":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_lfo
        elif fuel_name == "MDO":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_mdo
        elif fuel_name == "MGO":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_mgo
        elif fuel_name == "LNG(Otto Medium Speed)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_lng_medium
        elif fuel_name == "LNG(Otto Slow Speed)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_lng_slow
        elif fuel_name == "LNG(Otto Diesel Speed)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_lng_diesel
        elif fuel_name == "LPG(Butane)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_lpg_butane
        elif fuel_name == "LPG(Propane)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_lpg_propane
        elif fuel_name == "H2(Natural gas)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_h2
        elif fuel_name == "NH3(Natural gas)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_nh3_ng
        elif fuel_name == "Methanol(Natural gas)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_methanol
        elif fuel_name == "NH3(e-fuel)":
            fuel_total_co2 += tmp_fuel_foc * __co2_factor_nh3_efuel
    
    return fuel_total_co2

# CII算出メソッド
def calc_cii(co2, distance, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER):
    
    dwt         = float(VESSELMASTER[0]["Deadweight"]["S"])
    gt          = float(VESSELMASTER[0]["Grosstongue"]["S"])   
    
    # リファレンスライン取得------------------------------------------
    weight = 0
    if cii_ref[0]["weight"]["S"] == "DWT":
        weight = dwt
    elif cii_ref[0]["weight"]["S"] == "GT":
        weight = gt
    elif cii_ref[0]["weight"]["S"] == "0":
        weight = 0
        
    less        = cii_ref[0]["less"]["S"]
    less_more   = cii_ref[0]["less_more"]["S"]
    more        = cii_ref[0]["more"]["S"]
    less_value  = float(cii_ref[0]["less_value"]["S"])
    more_value  = float(cii_ref[0]["more_value"]["S"])
    less_a      = float(cii_ref[0]["less_a"]["S"])
    less_c      = float(cii_ref[0]["less_c"]["S"])
    less_more_a = float(cii_ref[0]["less_more_a"]["S"])
    less_more_c = float(cii_ref[0]["less_more_c"]["S"])
    more_a      = float(cii_ref[0]["more_a"]["S"])
    more_c      = float(cii_ref[0]["more_c"]["S"])
       
    a_G2 = 0
    c_G2 = 0
    if less == "1" and less_more == "1" and more == "1":
        if weight < less_value:
            a_G2 = less_a
            c_G2 = less_c
        elif less_value <= weight and weight < more_value:
            a_G2 = less_more_a
            c_G2 = less_more_c
        elif more_value <= weight:
            a_G2 = more_a
            c_G2 = more_c
    elif less == "1" and less_more == "0" and more == "1":
        if weight < less_value:
            a_G2 = less_a
            c_G2 = less_c
        elif more_value <= weight:
            a_G2 = more_a
            c_G2 = more_c
    elif less == "0" and less_more == "0" and more == "0":
        a_G2 = less_a
        c_G2 = less_c
    
    # レーティング取得------------------------------------------
    weight_rating = 0
    weight_type = cii_rating[0]["weight_type"]["S"]
    if weight_type == "DWT":
        weight_rating = dwt
    elif weight_type == "GT":
        weight_rating = gt
    elif weight_type == "0":
        weight_rating = 0
    
    if weight_rating != 0:
        weight_value = float(cii_rating[0]["weight_value"]["S"])
        if weight_rating < weight_value:
            rating_1 = float(cii_rating[0]["less_d1"]["S"])
            rating_2 = float(cii_rating[0]["less_d2"]["S"])
            rating_3 = float(cii_rating[0]["less_d3"]["S"])
            rating_4 = float(cii_rating[0]["less_d4"]["S"])
        elif weight_value <= weight_rating:
            rating_1 = float(cii_rating[0]["more_d1"]["S"])
            rating_2 = float(cii_rating[0]["more_d2"]["S"])
            rating_3 = float(cii_rating[0]["more_d3"]["S"])
            rating_4 = float(cii_rating[0]["more_d4"]["S"])
    elif weight_rating == 0:
        rating_1 = float(cii_rating[0]["less_d1"]["S"])
        rating_2 = float(cii_rating[0]["less_d2"]["S"])
        rating_3 = float(cii_rating[0]["less_d3"]["S"])
        rating_4 = float(cii_rating[0]["less_d4"]["S"])

    CII_Rating = [rating_1, rating_2, rating_3, rating_4]
           
    # 削減率セット
    reduction_rate = float(cii_reduction_rate[0]["reduction_rate"]["S"])
    
    # CII計算
    CII_Attained        = (co2 * pow(10, 6)) / (dwt * distance)                 # Attained CII(G1)
    CII_Reference = a_G2 * pow(dwt, (-1 * c_G2))                          # CII ref. （G2）
    CII_Required        = CII_Reference * ((100 - reduction_rate) / 100)  # Required CII （G3, 2023）
    CII_Calculated      = CII_Attained / CII_Required                          # Attained CII / Required CII
    
    # CII計算値からCIIスコアを算出
    CII_Score = ""
    if CII_Calculated < rating_1:
        CII_Score = "A"
    elif rating_1 <= CII_Calculated and CII_Calculated < rating_2:
        CII_Score = "B"
    elif rating_2 <= CII_Calculated and CII_Calculated < rating_3:
        CII_Score = "C"
    elif rating_3 <= CII_Calculated and CII_Calculated < rating_4:
        CII_Score = "D"
    elif rating_4 < CII_Calculated:
        CII_Score = "E"
            
    return CII_Calculated, CII_Score, CII_Rating

# FOC Formulasを使用したFOC算出メソッド
def calc_foc_using_foc_formulas(foc_formulas, displacement, log_speed, total_time):

    # FOC算出時にBallast/Ladenどちらの式を使うかを判定
    if displacement == "Ballast":
        # Ballast用の計算パラメータを取得し、FOCを算出
        calc_param_list = ast.literal_eval(foc_formulas["me_ballast"]["S"])

    else:
        # 上記以外の場合（実質Laden）                       
        # Laden用の計算パラメータを取得し、FOCを算出
        calc_param_list = ast.literal_eval(foc_formulas["me_laden"]["S"])

    # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
    auxiliary_equipment = float(foc_formulas["auxiliary_equipment"]["S"])
    print(f"auxiliary_equipment: {(auxiliary_equipment)}")

    alpah = calc_param_list[0]
    a = calc_param_list[1]
    c = calc_param_list[2]

    # 1日あたりのFOC算出（**は指数）
    foc_per_day = alpah * log_speed ** a + c + auxiliary_equipment
    # 1時間あたりのFOC算出
    foc_per_hour = foc_per_day / 24
    # 総FOCを算出
    foc = foc_per_hour * total_time

    return foc

# datetimeをfloatに変換する。
def timestamp_datetime_to_float(timestamp):
    try:
        timestamp = round(datetime.timestamp(timestamp)*1000)
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""

# FuelListをリストに変換
def convertFuelOileStringToList(text):
    
    pattern = r'\([^()]*\([^()]*\)[^()]*\)|\([^()]*\)'
    matches = re.findall(pattern, text)

    # 前後の括弧を除去
    cleaned_matches = [match[1:-1] for match in matches]

    return cleaned_matches

# 最大桁の値以外を０に変換する（例：43⇒40、549⇒500）
def maxDigitOnly(n):
    # 数値を文字列に変換
    str_n = str(n)
    # 最初の桁を取得
    first_digit = str_n[0]
    # 残りを0で埋める
    result = first_digit + '0' * (len(str_n) - 1)
    # 数値型に変換して返す
    return int(result)

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    pathParameters = event['pathParameters']['proxy'].split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']

    # imo取得
    imo = queryStringParameters['imo']
    print(f"imo{type(imo)}: {imo}")
    
    # 認可：IMO参照権限チェック
    authCheck = auth.imo_check(token, imo)
    if authCheck == 401 or authCheck == 500:
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },  
            'body': authCheck
        }

    # インプット取得
    plan = queryStringParameters['plan']
    user_id = queryStringParameters['user']

    # VesselMaster取得
    res_vesselmaster = select.get_vessel_master(imo)
    # FOCFormulas取得
    res_foc_formulas = select.get_foc_formulas(imo)

    # 処理実施時の年取得
    dt_now = datetime.now()
    str_now  = dt_now.strftime('%Y/%m/%d %H:%M')
    now_year = str(dt_now.year)
    datetime_now = datetime.strptime(str_now, "%Y/%m/%d %H:%M")

    # CII算出のため、各種マスタを取得
    cii_ref = select.get_cii_ref(res_vesselmaster[0]["VesselType"]["S"])
    cii_rating = select.get_cii_rating(res_vesselmaster[0]["VesselType"]["S"])
    cii_reduction_rate = select.get_cii_reduction_rate(now_year)

    res_simulation = None

    # SpeedPlanを取得、登録済みの場合、flagを0に更新
    res_simulation_speed = select.get_simulation_speed(imo)
    if res_simulation_speed:
        insert.upsert_simulation_speed(imo)

    # VoyagePlanのflagを"1"に更新
    res_simulation_voyage = select.get_simulation_voyage(imo, now_year)
    if res_simulation_voyage:
        for item in res_simulation_voyage:
            pkey_imo = item["imo"]["S"]
            skey_year_and_serial_number = item["year_and_serial_number"]["S"]
            insert.upsert_simulation_voyage(pkey_imo, skey_year_and_serial_number)

    # 最新のeco-cii-simulation-cond-voyage-plan取得
    res_simulation = select.get_simulation_voyage(imo, now_year)
    
    simulation_execution_flg = False

    if res_simulation:
        # Simulation処理を実施

        simulation_execution_flg = True
        
        # Average計算用変数を定義
        average_log_speed_total = 0
        average_displacement_total = 0
        average_data_count_log_speed = 0
        average_data_count_displaement = 0

        # VesselAlarm取得
        res_vesselalarm = select.get_vessel_alarm(imo, now_year)
        last_year_cii_value = float(res_vesselalarm[0]["LastYear_val"]["S"])

        dt_timestamp_year_from = datetime(year = int(now_year), month = 1, day = 1, hour = 0, minute = 0, second = 0, microsecond = 0)
        dt_timestamp_year_from_str = dt_timestamp_year_from.strftime('%Y-%m-%dT%H:%M:%SZ')
        dt_timestamp_year_to = datetime(year = int(now_year), month = 12, day = 31, hour = 23, minute = 59, second = 59, microsecond = 999999)
        dt_timestamp_year_to_str = dt_timestamp_year_to.strftime('%Y-%m-%dT%H:%M:%SZ')

        # NoonRreport取得
        res_nr_list =  select.get_noonreport(imo, dt_timestamp_year_from_str, dt_timestamp_year_to_str)

        # 月別集計結果の定義
        monthly_total = {
            "timestamp_float"   : 0,
            "distance"          : 0,
            "foc"               : 0,
            "co2_emission"      : 0,
            "cii_value"         : 0,
            "cii_score"         : "",
            "log_speed_total"   : 0,
            "count_log_speed"   : 0,
            "displacement_total": 0,
            "count_displacement": 0
        }
        # 1～12月までの12個をListにセット
        monthly_total_list = [monthly_total.copy() for _ in range(12)]

        # 最新NoonReportのタイムスタンプ（初期値は適当に古い日付を設定している）
        latest_timestamp = "1990-01-01T00:00:00Z"

        # NoonReportの取得件数分繰り返し
        for nr in res_nr_list:

            # timestampから月を特定
            nr_timestamp = nr["timestamp"]["S"]
            nr_timestamp_dt = datetime.fromisoformat(nr_timestamp.replace("Z", "+00:00"))
            nr_month = nr_timestamp_dt.month

            # Average計算用に加算
            if ('log_speed' in nr and nr["log_speed"]["S"] != ""):
                average_log_speed_total += float(nr["log_speed"]["S"])
                average_data_count_log_speed += 1
                monthly_total_list[nr_month - 1]["log_speed_total"] += float(nr["log_speed"]["S"])
                monthly_total_list[nr_month - 1]["count_log_speed"] += 1
            
            if ('displacement' in nr and nr["displacement"]["S"] != ""):
                average_displacement_total += float(nr["displacement"]["S"])    
                average_data_count_displaement += 1
                monthly_total_list[nr_month - 1]["displacement_total"] += float(nr["displacement"]["S"])
                monthly_total_list[nr_month - 1]["count_displacement"] += 1

            # 燃料ごとの消費量を取得
            # M/E BOG
            me_bog = float(nr["me_bog"]["S"]) if 'me_bog' in nr and nr["me_bog"]["S"] != "" else ""
            # D/G BOG
            dg_bog = float(nr["dg_bog"]["S"]) if 'dg_bog' in nr and nr["dg_bog"]["S"] != "" else ""
            # GCU BOG
            gcu_bog = float(nr["gcu_bog"]["S"]) if 'gcu_bog' in nr and nr["gcu_bog"]["S"] != "" else ""            
            # M/E HFO
            me_hfo = float(nr["me_hfo"]["S"]) if 'me_hfo' in nr and nr["me_hfo"]["S"] != "" else ""
            # D/G HFO
            dg_hfo = float(nr["dg_hfo"]["S"]) if 'dg_hfo' in nr and nr["dg_hfo"]["S"] != "" else ""
            # BOILER HFO
            boiler_hfo = float(nr["boiler_hfo"]["S"]) if 'boiler_hfo' in nr and nr["boiler_hfo"]["S"] != "" else ""
            # M/E LSFO
            me_lsfo = float(nr["me_lsfo"]["S"]) if 'me_lsfo' in nr and nr["me_lsfo"]["S"] != "" else ""
            # D/G LSFO（SPAS上の名称はge_foc）
            dg_lsfo = float(nr["ge_foc"]["S"]) if 'ge_foc' in nr and nr["ge_foc"]["S"] != "" else ""
            # BOILER LSFO（SPAS上の名称はboiler_foc）
            boiler_lsfo = float(nr["boiler_foc"]["S"]) if 'boiler_foc' in nr and nr["boiler_foc"]["S"] != "" else ""
            # M/E DO
            me_do = float(nr["me_do"]["S"]) if 'me_do' in nr and nr["me_do"]["S"] != "" else ""
            # D/G DO
            dg_do = float(nr["dg_do"]["S"]) if 'dg_do' in nr and nr["dg_do"]["S"] != "" else ""
            # BOILER DO
            boiler_do = float(nr["boiler_do"]["S"]) if 'boiler_do' in nr and nr["boiler_do"]["S"] != "" else ""
            # M/E LSGO
            me_lsgo = float(nr["me_lsgo"]["S"]) if 'me_lsgo' in nr and nr["me_lsgo"]["S"] != "" else ""
            # D/G LSGO
            dg_lsgo = float(nr["dg_lsgo"]["S"]) if 'dg_lsgo' in nr and nr["dg_lsgo"]["S"] != "" else ""
            # BOILER LSGO
            boiler_lsgo = float(nr["boiler_lsgo"]["S"]) if 'boiler_lsgo' in nr and nr["boiler_lsgo"]["S"] != "" else ""
            # IGG GO
            igg_go = float(nr["igg_go"]["S"]) if 'igg_go' in nr and nr["igg_go"]["S"] != "" else ""
            # IGG LSGO
            igg_lsgo = float(nr["igg_lsgo"]["S"]) if 'igg_lsgo' in nr and nr["igg_lsgo"]["S"] != "" else ""

            # 燃料ごとの合計消費量を算出
            # BOG
            total_bog = 0
            total_bog += me_bog if me_bog != "" else 0
            total_bog += dg_bog if dg_bog != "" else 0
            total_bog += gcu_bog if gcu_bog != "" else 0
            # HFO
            total_hfo = 0
            total_hfo += me_hfo if me_hfo != "" else 0
            total_hfo += dg_hfo if dg_hfo != "" else 0
            total_hfo += boiler_hfo if boiler_hfo != "" else 0
            # LFO
            total_lfo = 0
            total_lfo += me_lsfo if me_lsfo != "" else 0
            total_lfo += dg_lsfo if dg_lsfo != "" else 0
            total_lfo += boiler_lsfo if boiler_lsfo != "" else 0
            # DO
            total_do = 0
            total_do += me_do if me_do != "" else 0
            total_do += dg_do if dg_do != "" else 0
            total_do += boiler_do if boiler_do != "" else 0
            # GO
            total_go = 0
            total_go += me_lsgo if me_lsgo != "" else 0
            total_go += dg_lsgo if dg_lsgo != "" else 0
            total_go += boiler_lsgo if boiler_lsgo != "" else 0
            total_go += igg_go if igg_go != "" else 0
            total_go += igg_lsgo if igg_lsgo != "" else 0

            # total_foc算出 
            total_foc = total_bog + total_hfo + total_lfo + total_do + total_go
            # co2排出量算出
            co2 = total_bog * __co2_factor_lng_medium + total_hfo * __co2_factor_hfo + total_lfo * __co2_factor_lfo + total_do * __co2_factor_mdo + total_go * __co2_factor_mgo

            # 月別集計リストの該当月に加算
            monthly_total_list[nr_month - 1]["distance"] += float(nr["og_distance"]["S"]) if 'og_distance' in nr and nr["og_distance"]["S"] != "" else 0
            monthly_total_list[nr_month - 1]["foc"] += total_foc
            monthly_total_list[nr_month - 1]["co2_emission"] += co2

            # 最新TimeStampの更新
            if latest_timestamp < nr_timestamp:
                latest_timestamp = nr_timestamp
        
        # NoonReportの繰り返し処理終了--------------------------------------------------------------------------

        #最新TimeStampをdatetime型に変換
        latest_timestamp_dt = datetime.fromisoformat(latest_timestamp.replace("Z", ""))

        # Simulation処理
        # VoyagePlanのSimulation処理実施
        
        for simulation_leg in res_simulation:

            start_time = None
            end_time = None

            # DepartureTime取得
            departure_time_string = simulation_leg["departure_time"]["S"]
            departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")

            # ArrivalTime取得
            arrival_time_string = simulation_leg["arrival_time"]["S"]
            arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")

            # start_timeを決める。
            if departure_time >= datetime_now:
                start_time = departure_time
            elif arrival_time > datetime_now:
                start_time = datetime_now
            else:
                # 上記以外の場合、すでに日付が過ぎているLegになるため次の要素へスキップ
                continue

            # endTimeにArrivalTimeを設定
            end_time = arrival_time

            # DepartureTimeからArrivalTimeまでのTotalTimeを算出
            leg_total_time = calc_time_diff(departure_time, arrival_time)
            leg_actual_time = calc_time_diff(start_time, end_time)
            print(f"leg_total_time:{(leg_total_time)} start_time:{(start_time)} end_time:{(end_time)}")

            # 登録されたVoyageの期間で、処理時刻よりも未来の時間の割合、log_speedを算出する
            leg_time_rate = 0
            if leg_total_time != 0:
                leg_time_rate = leg_actual_time / leg_total_time
            print(f"leg_time_rate:{(leg_time_rate)}")

            log_speed = 0
            if leg_total_time != 0:
                log_speed = float(simulation_leg["distance"]["S"]) / leg_total_time

            # voyageのdistanceのうち、未来期間に航海する距離を求める。
            actual_distance = float(simulation_leg["distance"]["S"]) * leg_time_rate

            # 平均値算出用処理
            # TotalTimeをに日数に換算
            actual_day_count = math.ceil(leg_total_time / 24)
            print(f"leg_actual_time:{(leg_actual_time)} actual_day_count:{(actual_day_count)}")
            all_log_speed = log_speed * actual_day_count
            average_data_count_log_speed += actual_day_count
            average_log_speed_total += all_log_speed

            # Leg内総FOCを算出
            leg_total_FOC_voyage = calc_foc_using_foc_formulas(res_foc_formulas[0], simulation_leg["dispracement"]["S"], log_speed, leg_actual_time)

            #Fuel取得
            fuel_list = convertFuelOileStringToList(simulation_leg["fuel"]["S"])

            # 燃料別にCO2排出量を算出
            leg_total_co2_emission = calc_fuel_total_co2(fuel_list, leg_total_FOC_voyage)

            start_month = start_time.month
            end_month = end_time.month
            diff_month = end_month - start_month

            # ※例：start：10月、end：12月の場合、3回ループさせる。
            for i in range(diff_month + 1):
                
                # 該当月
                target_month = start_month + i

                caluculated_time = None

                if target_month == start_month:
                    # startTimeからその月の月末までの時間を算出
                        start_last_day = calendar.monthrange(start_time.year, start_time.month)[1]

                        if diff_month == 0:
                            # 1LEGが月を跨いでいない場合の考慮
                            start_last_day_dt = datetime(end_time.year, end_time.month, end_time.day, end_time.hour, end_time.minute, end_time.second)
                        else:
                            start_last_day_dt = datetime(start_time.year, start_month, start_last_day, 23, 59, 59)
                        
                        caluculated_time = calc_time_diff(start_time, start_last_day_dt)                       
                    
                elif start_month < target_month < end_month:
                    # target_monthの月末までの時間を算出
                    target_month_last_day = calendar.monthrange(start_time.year, target_month)[1]
                    target_month_last_day_dt = datetime(start_time.year, target_month, target_month_last_day, 23, 59, 59)
                    target_month_farst_day_dt = datetime(start_time.year, target_month, 1, 0, 0, 0)
                    caluculated_time = calc_time_diff(target_month_farst_day_dt, target_month_last_day_dt)

                else:
                    # endTimeの月の1日からendTimeまでの時間を算出
                    end_first_day_dt = datetime(end_time.year, end_time.month, 1, 0, 0, 0)
                    caluculated_time = calc_time_diff(end_first_day_dt, end_time)
                
                # TotalTimeと算出した時間の割合を算出
                calc_time_rate = 0
                if leg_total_time != 0:
                    calc_time_rate = caluculated_time / leg_total_time

                # 月別集計リストの該当月に加算
                tmp_distance_voyage = float(simulation_leg["distance"]["S"]) * calc_time_rate
                tmp_foc_voyage = leg_total_FOC_voyage * calc_time_rate
                tmp_co2_emission_voyage = leg_total_co2_emission * calc_time_rate
                tmp_log_speed_total = log_speed * math.ceil(caluculated_time / 24)
                print(f"target_month:{(target_month)} caluculated_time:{(caluculated_time)} actual_day_count:{(actual_day_count)}")
                monthly_total_list[target_month - 1]["distance"] += tmp_distance_voyage

                print(f"【before】monthly_total_list[{(target_month - 1)}][foc]:{(monthly_total_list[target_month - 1]["foc"])}")
                print(f"tmp_foc_voyage:{(tmp_foc_voyage)}")
                monthly_total_list[target_month - 1]["foc"] += tmp_foc_voyage
                print(f"【after】monthly_total_list[{(target_month - 1)}][foc]:{(monthly_total_list[target_month - 1]["foc"])}")

                monthly_total_list[target_month - 1]["co2_emission"] += tmp_co2_emission_voyage
                monthly_total_list[target_month - 1]["log_speed_total"] += tmp_log_speed_total
                monthly_total_list[target_month - 1]["count_log_speed"] += math.ceil(caluculated_time / 24)

        # 変数定義
        cii_score_transition_list = []
        simulation_result_cii_score = ""
        total_foc_for_result = 0
        total_distance_for_result = 0
        total_co2_emission_for_result = 0
        cii_ration = []
        max_foc = 0

        # 月別集計リスト分ループ（1月～12月分を繰り返し処理）
        for i in range(12):
            
            # CII算出
            tmp_eof_co2 = monthly_total_list[i]["co2_emission"]
            tmp_eof_distance = monthly_total_list[i]["distance"]
            
            tmp_monthly_cii_value = 0
            tmp_monthly_cii_Score = ""

            # distanceが0以外の場合に計算
            if (tmp_eof_distance != 0):
                tmp_monthly_cii_value, tmp_monthly_cii_Score, cii_ration = calc_cii(tmp_eof_co2, tmp_eof_distance, cii_ref, cii_rating, cii_reduction_rate , res_vesselmaster)

            monthly_total_list[i]["cii_value"] = tmp_monthly_cii_value
            monthly_total_list[i]["cii_score"] = tmp_monthly_cii_Score

            total_foc_for_result += monthly_total_list[i]["foc"]
            total_distance_for_result += monthly_total_list[i]["distance"]
            total_co2_emission_for_result += monthly_total_list[i]["co2_emission"]

            # i月時点のCII算出
            tmp_total_cii_value = 0
            tmp_total_cii_Score = ""

            # distanceが0以外の場合に計算
            if (total_distance_for_result != 0):
                tmp_total_cii_value, tmp_total_cii_Score, cii_ration = calc_cii(total_co2_emission_for_result, total_distance_for_result, cii_ref, cii_rating, cii_reduction_rate , res_vesselmaster)
            
            # 結果をCIIスコア推移リストに追加
            cii_score_transition_list.append(tmp_total_cii_value)

            # 最終的なSimulation結果のCIIScoreを保持
            if (i == 11):
                simulation_result_cii_score = tmp_total_cii_Score
            # y軸のFOCのAxisを決めるため、FOCの最大値を保持
            max_foc = monthly_total_list[i]["foc"] if max_foc < monthly_total_list[i]["foc"] else max_foc


            # x軸のための日時設定
            x_axis_value_dt = datetime(dt_now.year, i + 1, 1, 0, 0, 0)
            x_axis_value_float = timestamp_datetime_to_float(x_axis_value_dt)
            monthly_total_list[i]["timestamp_float"] = x_axis_value_float
        
        # 平均値を算出
        calc_average_log_speed = 0
        calc_average_displacement = 0
        if (average_data_count_log_speed != 0):
            calc_average_log_speed = average_log_speed_total / average_data_count_log_speed
        
        if (average_data_count_displaement != 0):
            calc_average_displacement = average_displacement_total / average_data_count_displaement

    else:
        # 上記以外の場合、Simulationは行ない
        print("Simulationなし")

    # 返却値を作成していく。
    # VesselMaster
    vessel_master = {
        "BuildDate"         : res_vesselmaster[0]["BuildDate"]["S"],
        "Deadweight"        : res_vesselmaster[0]["Deadweight"]["S"],
        "Grosstongue"       : res_vesselmaster[0]["Grosstongue"]["S"],
        "LastDockDate"      : res_vesselmaster[0]["LastDockDate"]["S"],
        "HullCleaningDate"  : res_vesselmaster[0]["HullCleaningDate"]["S"],
        "OilType"           : res_vesselmaster[0]["OilType"]["S"],
        "Owner"             : res_vesselmaster[0]["Owner"]["S"],
        "Size"              : res_vesselmaster[0]["Size"]["S"],
        "VesselName"        : res_vesselmaster[0]["VesselName"]["S"],
        "VesselType"        : res_vesselmaster[0]["VesselType"]["S"],
        "Ballast"           : res_vesselmaster[0]["Ballast"]["S"],
        "Laden"             : res_vesselmaster[0]["Laden"]["S"],
    }

    # FuelOilList
    fuel_oil_list = ["HFO", "LFO", "MDO", "MGO", "LNG(Otto Medium Speed)", "LNG(Otto Slow Speed)", "LNG(Otto Diesel Speed)", "LPG(Butane)", "LPG(Propane)", "H2(Natural gas)", "NH3(Natural gas)", "Methanol(Natural gas)", "NH3(e-fuel)"]

    # FOC FOrmulas
    foc_formulas = {
        "ME_Ballast": [],
        "ME_Laden":  [],
        "AuxiliryEquipment": "",
    }
    if res_foc_formulas:
        # FOC Formulasが取得出来ている場合は設定
        foc_formulas["ME_Ballast"] = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
        foc_formulas["ME_Laden"] = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
        foc_formulas["AuxiliryEquipment"] = ast.literal_eval(res_foc_formulas[0]["auxiliary_equipment"]["S"])

    # SimulationList
    simulation_infomation_voyage_list = []
    simulation_infomation_speed_list = []

    if res_simulation:
        # Simulationテーブルが取得出来た場合
        
        if plan == "Voyage":
            # Voyageの場合

            tmp_simulation_infomation_voyage_list = []

            for item in res_simulation:

                # Leg No 取得
                tmp_text = item["year_and_serial_number"]["S"]
                leg_no = int(tmp_text.split("E")[1])  # 'E' 以降を抽出

                # total time算出
                # DepartureTime取得
                departure_time_string = item["departure_time"]["S"]
                departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")

                # ArrivalTime取得
                arrival_time_string = item["arrival_time"]["S"]
                arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")

                # Leg航海時間算出
                total_time = calc_time_diff(departure_time, arrival_time)

                # start_timeを決める。
                if datetime_now <= departure_time:
                    start_time = departure_time
                elif datetime_now <= arrival_time:
                    start_time = datetime_now
                else:
                    # 上記以外の場合、すでに日付が過ぎているLegになるため次の要素へスキップ
                    continue

                # endTimeにArrivalTimeを設定
                end_time = arrival_time

                # DepartureTimeからArrivalTimeまでのTotalTimeを算出
                voyage_total_time = calc_time_diff(departure_time, arrival_time)
                total_time = calc_time_diff(start_time, end_time)
                # Leg内航海時間との割合を算出し、その分のDistanceを切り出して使用
                tmp_ratio = 0
                if voyage_total_time != 0:
                    tmp_ratio =  total_time / voyage_total_time
                calculated_distance = float(item["distance"]["S"]) * tmp_ratio
                print(f"calculated_distance:{(calculated_distance)} item[distance][S]:{(item["distance"]["S"])} tmp_ratio:{(tmp_ratio)}")

                # Log Speed算出
                log_speed = 0
                if total_time != 0:
                    log_speed = float(item["distance"]["S"]) / voyage_total_time

                # fuel
                output_fuel_list = []
                fuel_list = convertFuelOileStringToList(item["fuel"]["S"])

                for fuel in fuel_list:
                    print(f"fuel:{fuel}")
                    fuel_info_list = fuel.split(',')
                    print(f"fuel_info_list:{fuel_info_list}")

                    output_fuel = {
                        "fuel_type" : fuel_info_list[0],
                        "fuel_rate" : fuel_info_list[1],
                    }

                    output_fuel_list.append(output_fuel)


                # FOC算出（FOC Formulasが取得出来なかった場合は計算しない）
                if res_foc_formulas:
                    tmp_foc = calc_foc_using_foc_formulas(res_foc_formulas[0], item["dispracement"]["S"], log_speed, total_time)
                    foc = round(tmp_foc)
                else:
                    foc = "-"
                
                print(f"start_time{type(start_time)}:{(start_time)}")
                data = {
                    "leg_no"            : leg_no,
                    "departure_port"    : item["departure_port"]["S"],
                    "departure_time"    : start_time.strftime('%Y/%m/%d %H:%M'),
                    "arrival_port"      : item["arrival_port"]["S"],
                    "arrival_time"      : item["arrival_time"]["S"],
                    "total_time"        : str(total_time),
                    "distance"          : str(round(calculated_distance)),
                    "fuel"              : output_fuel_list,
                    "dispracement"      : item["dispracement"]["S"],
                    "log_speed"         : str(round(log_speed, 1)),
                    "foc"               : str(foc),
                }

                tmp_simulation_infomation_voyage_list.append(data)
            
            # Leg Noでソート
            tmp_simulation_infomation_voyage_list_sorted = sorted(tmp_simulation_infomation_voyage_list, key=lambda x: x['leg_no'])

            # 通番を設定する
            num = 0
            for voyage_data in tmp_simulation_infomation_voyage_list_sorted:
                num += 1
                data = {
                    "leg_no"            : str(num),
                    "departure_port"    : voyage_data["departure_port"],
                    "departure_time"    : voyage_data["departure_time"],
                    "arrival_port"      : voyage_data["arrival_port"],
                    "arrival_time"      : voyage_data["arrival_time"],
                    "total_time"        : voyage_data["total_time"],
                    "distance"          : voyage_data["distance"],
                    "fuel"              : voyage_data["fuel"],
                    "dispracement"      : voyage_data["dispracement"],
                    "log_speed"         : voyage_data["log_speed"],
                    "foc"               : voyage_data["foc"],
                }
                simulation_infomation_voyage_list.append(data)

    # グラフエリアに表示する情報
    total_ciiscorelist_yeartodate = []
    total_ciiscorelist_simulation = []
    monthly_ciiscorelist_yeartodate = []
    monthly_ciiscorelist_simulation = []
    foclist_yeartodate = []
    foclist_simulation = []
    ciiscore_lastyear = ""
    voyage_infomation_total = None
    cii_rating_list = []
    foc_y_axsis = 0
    voyage_infomation_unit = []

    # Simulationが実施された場合のみ設定する
    if simulation_execution_flg:

        # 当月を取得
        now_month = dt_now.month
        
        # 月別集計リスト分ループ（1月～12月分を繰り返し処理）
        for i in range(12):
            
            # 月別CIIScore、FOC
            if i + 1 < now_month:
                # 当月以前の場合、実績リストにのみ設定
                monthly_ciiscorelist_yeartodate.append([float(monthly_total_list[i]["timestamp_float"]), float(monthly_total_list[i]["cii_value"])])
                foclist_yeartodate.append([float(monthly_total_list[i]["timestamp_float"]), float(monthly_total_list[i]["foc"])])
            elif i + 1 >= now_month:
                # 当月以降の場合、Simulationリストにのみ設定
                monthly_ciiscorelist_simulation.append([float(monthly_total_list[i]["timestamp_float"]), float(monthly_total_list[i]["cii_value"])])
                foclist_simulation.append([float(monthly_total_list[i]["timestamp_float"]), float(monthly_total_list[i]["foc"])])

            # TotalCIIScore
            if i + 2 < now_month:
                # 前月以前の場合、実績リストにのみ設定
                total_ciiscorelist_yeartodate.append([monthly_total_list[i]["timestamp_float"], cii_score_transition_list[i]])
            elif i + 1 >= now_month:
                # 当月以降の場合、Simulationリストにのみ設定
                total_ciiscorelist_simulation.append([monthly_total_list[i]["timestamp_float"], cii_score_transition_list[i]])
            else:
                # 前月の場合、実績、Simulationの両方に設定
                total_ciiscorelist_yeartodate.append([monthly_total_list[i]["timestamp_float"], cii_score_transition_list[i]])
                total_ciiscorelist_simulation.append([monthly_total_list[i]["timestamp_float"], cii_score_transition_list[i]])

            # 各レグをクリックした時のSimulation Result用Listに追加
            avg_log_speed    = 0
            avg_displacement = 0
            if monthly_total_list[i]["count_log_speed"] != 0:
                avg_log_speed = monthly_total_list[i]["log_speed_total"] / monthly_total_list[i]["count_log_speed"]
            if monthly_total_list[i]["count_displacement"] != 0:
                avg_displacement = monthly_total_list[i]["displacement_total"] / monthly_total_list[i]["count_displacement"]

            voyage_infomation_data = {
                "CiiScore"           : monthly_total_list[i]["cii_score"],
                "total_foc"          : round(monthly_total_list[i]["foc"], 1),
                "total_co2_emissions": round(monthly_total_list[i]["co2_emission"]),
                "total_distance"     : round(monthly_total_list[i]["distance"]),
                "avg_log_speed"      : round(avg_log_speed, 2),
                "avg_displacement"   : round(avg_displacement),
            }
            voyage_infomation_unit.append(voyage_infomation_data)

        ciiscore_lastyear = last_year_cii_value
        cii_rating_list = cii_ration
        foc_y_axsis = max_foc

        voyage_infomation_total = {
            "CiiScore": simulation_result_cii_score ,
            "total_foc": round(float(total_foc_for_result), 1),
            "total_co2_emissions": round(float(total_co2_emission_for_result)),
            "total_distance": round(float(total_distance_for_result)),
            "avg_log_speed": round(float(calc_average_log_speed), 2),
            "avg_displacement": round(float(calc_average_displacement))
        }
    
    datas = {
        "VESSELMASTER"                      : vessel_master,
        "FuelOilList"                       : fuel_oil_list,
        "FOCFormulas"                       : foc_formulas, 
        "SimulationInformationVoyageList"   : simulation_infomation_voyage_list,
        "SimulationInformationLogSpeedList" : simulation_infomation_speed_list,
        "TotalCiiScoreList_YeartoDate"      : total_ciiscorelist_yeartodate,
        "TotalCiiScoreList_Simulation"      : total_ciiscorelist_simulation,
        "MonthlyCiiScoreList_YeartoDate"    : monthly_ciiscorelist_yeartodate,
        "MonthlyCiiScoreList_Simulation"    : monthly_ciiscorelist_simulation,
        "FOCList_YeartoDate"                : foclist_yeartodate,
        "FOCList_Simulation"                : foclist_simulation,
        "CiiScore_LastYear"                 : ciiscore_lastyear,
        "VoyageInformationTotal"            : voyage_infomation_total,
        "VoyageInformationUnit"             : voyage_infomation_unit,
        "CII_RATING"                        : cii_ration,
        "FOC_YAXIS"                         :{"max": round(max_foc, 0) , "tickInterval":maxDigitOnly(round(max_foc / 4)) }
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
