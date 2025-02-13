
import math
import json
from datetime import datetime
import ast
import re

from dynamodb import select

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
            
    return CII_Calculated, CII_Score

# FuelListをリストに変換
def convertFuelOileStringToList(text):
    
    pattern = r'\([^()]*\([^()]*\)[^()]*\)|\([^()]*\)'
    matches = re.findall(pattern, text)

    # 前後の括弧を除去
    cleaned_matches = [match[1:-1] for match in matches]

    return cleaned_matches

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    # イベントからクエリ取得-------------------------------------------------------
    pathParameters = event['pathParameters']
    pathParameters = pathParameters['proxy']
    pathParameters = pathParameters.split("/")
    queryStringParameters = event['queryStringParameters']
    
    # クエリからインプット情報取得-------------------------------------------------------
    user_id = queryStringParameters['user']
    select_year = queryStringParameters['year']
    
    # ユーザ付随情報取得-------------------------------------------------------
    res_user = select.get_user(user_id)
    group_id      = res_user[0]["group_id"]["S"]
    company_id    = res_user[0]["company_id"]["S"]

    # 初回利用ログイン時 or ログイン時初期表示 or 画面遷移を判定---------------------------------------------
    if "init" in queryStringParameters:
        gid = ast.literal_eval(group_id)[0]
    else:
        gid = queryStringParameters['group']        

    imo_list = []
    
    # Imoリストを取得-------------------------------------------------------
    if gid == "Favorite":
        # favoriteの場合、Favoriteテーブルからimoリストを取得
        res_favorite = select.get_favorite(user_id)
        imo_list = ast.literal_eval(res_favorite[0]["imo_list"]["S"])       
                    
    else:
        # 上記以外の場合（基本はALL（=admin）のはず）、Groupテーブル取得し、GIDに該当するimoリストを特定する。
        res_group = select.get_group(company_id)

        for res_group_item in res_group:
            if res_group_item['group_id']['S'] == gid:
                imo_list = ast.literal_eval(res_group_item["imo_list"]["S"])        

    
    imo_list = list(set(imo_list))

    print(imo_list)

    # 処理実施時の年取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    
    # Imoリスト数分繰り返し
    data_list = []
    group_imo_list = []
    for imo in imo_list:

        # VesselMaster取得
        res_vesselmaster = select.get_vessel_master(imo)
        res_vesselalarm = select.get_vessel_alarm(imo, select_year)
        
        if select_year == now_year:
            # 選択yearが当年の場合、Simulationを実施
            
            # eco-cii-simulation-cond-voyage-plan取得
            res_simulation_voyage = select.get_simulation_voyage(imo, now_year)

            # eco-cii-simulation-cond-speed-plan取得
            res_simulation_speed = select.get_simulation_speed(imo, now_year)

            # SimulationのためにFOC Formulas取得
            res_foc_formulas = select.get_foc_formulas(imo)

            # シミュレーションの合計値保持のための変数定義
            all_distance_simulation = 0 # 総Distance（予測値）
            all_foc_simulation = 0      # 総FOC（予測値）
            all_co2_simulation = 0      # 総CO2排出量（予測値）

            if res_simulation_voyage and res_simulation_voyage[0]["flag"]["S"] == "1":
                # VoyagePlanが取得できたかつflagが1の場合

                # VoyagePlanの取得件数分ループ
                for i in range(len(res_simulation_voyage)):

                    # Leg航海時間（DepartureTime - ArrivalTime）
                    leg_sailing_time = 0

                    # 計算用変数
                    calculated_sailing_time = 0
                    calculated_distance = 0
                    
                    # DepartureTime取得
                    departure_time_string = res_simulation_voyage[i]["departure_time"]["S"]
                    departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")

                    # ArrivalTime取得
                    arrival_time_string = res_simulation_voyage[i]["arrival_time"]["S"]
                    arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")

                    # Leg航海時間算出
                    leg_sailing_time = calc_time_diff(departure_time, arrival_time)
                   
                    # Legの範囲をそのまま使えるか判定
                    if departure_time >= dt_now:

                        # 航海時間、Distanceをそのまま使用
                        calculated_sailing_time = leg_sailing_time
                        calculated_distance = float(res_simulation_voyage[i]["distance"]["S"])
                        
                    elif arrival_time >= dt_now:

                        # 現在時間からArrivalTimeまでの時間算出
                        calculated_sailing_time = calc_time_diff(dt_now, arrival_time)
                        # Leg内航海時間との割合を算出し、その分のDistanceを切り出して使用
                        tmp_ratio =  calculated_sailing_time / leg_sailing_time
                        calculated_distance = float(res_simulation_voyage[i]["distance"]["S"]) * tmp_ratio

                    else:
                        # 上記以外の場合、処理不要のため次の要素へ
                        continue

                    # 総Distance（予測値）に計算用Distanceを加算
                    all_distance_simulation += calculated_distance

                    # LogSpeed算出
                    log_speed = calculated_distance / calculated_sailing_time

                    # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
                    auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])
                    print(f"auxiliary_equipment: {(auxiliary_equipment)}")

                    # FOC算出時にBallast/Ladenどちらの式を使うかを判定
                    if res_simulation_voyage[i]["dispracement"]["S"] == "Ballast":
                        # Ballast用の計算パラメータを取得し、FOCを算出
                        calc_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])

                    else:
                        # 上記以外の場合（実質Laden）                       
                        # Laden用の計算パラメータを取得し、FOCを算出
                        calc_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])

                    alpah = calc_param_list[0]
                    a = calc_param_list[1]
                    c = calc_param_list[2]

                    # 1日あたりのFOC算出（**は指数）
                    foc_per_day = alpah * log_speed ** a + c + auxiliary_equipment
                    # 1時間あたりのFOC算出
                    foc_per_hour = foc_per_day / 24
                    # Leg内総FOCを算出
                    leg_total_FOC_voyage = foc_per_hour * calculated_sailing_time

                    # 総FOC（予測値）に加算
                    all_foc_simulation += leg_total_FOC_voyage

                    #Fuel取得
                    fuel_list = convertFuelOileStringToList(res_simulation_voyage[i]["fuel"]["S"])

                    # 燃料別にCO2排出量を算出し、予測値に加算
                    all_co2_simulation += calc_fuel_total_co2(fuel_list, leg_total_FOC_voyage)
                    
            elif res_simulation_speed and res_simulation_speed[0]["flag"]["S"] == "1" and res_simulation_speed[0]["year"]["S"] == now_year:
                # SpeedPlanが取得できたかつflagが1の場合
                
                # Time to End of Year算出（年末 - 現在）
                year_end = datetime(dt_now.year, 12, 31, 23, 59, 59)
                time_to_end_of_year = calc_time_diff(dt_now, year_end)

                # 航海時間を算出
                sailing_rate = float(res_simulation_speed[0]["salling_rate"]["S"])
                sailing_time = time_to_end_of_year * (sailing_rate / 100)

                # Ballast、Ladenそれぞれの航海距離を算出
                displacement_rate = float(res_simulation_speed[0]["dispracement_rate"]["S"])
                ballast_sailing_time = sailing_time * (displacement_rate / 100)
                laden_sailing_time = sailing_time - ballast_sailing_time

                # 時間×速さで距離を算出
                ballst_logspeed = float(res_simulation_speed[0]["log_speed_ballast"]["S"])
                laden_logspeed = float(res_simulation_speed[0]["log_speed_laden"]["S"])
                ballast_ditance = ballast_sailing_time * ballst_logspeed
                laden_ditance = laden_sailing_time * laden_logspeed

                # 総Distance（予測値）に加算
                all_distance_simulation = ballast_ditance + laden_ditance

                # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
                auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])
                print(f"auxiliary_equipment: {(auxiliary_equipment)}")

                # Ballast用の計算パラメータを取得し、1日当たりのFOCを算出
                calc_balast_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
                balast_alpha = calc_balast_param_list[0]
                balast_a = calc_balast_param_list[1]
                balast_c = calc_balast_param_list[2]
                balast_foc_per_day = balast_alpha * ballst_logspeed ** balast_a + balast_c + auxiliary_equipment
                # Laden用の計算パラメータを取得し、1日当たりのFOCを算出
                calc_laden_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
                laden_alpha = calc_laden_param_list[0]
                laden_a = calc_laden_param_list[1]
                laden_c = calc_laden_param_list[2]
                laden_foc_per_day = laden_alpha * laden_logspeed ** laden_a + laden_c + auxiliary_equipment

                # 1時間あたりのFOC算出
                ballast_foc_per_hour = balast_foc_per_day / 24
                laden_foc_per_hour = laden_foc_per_day / 24
                # FOC算出
                ballast_foc = ballast_foc_per_hour * ballast_sailing_time
                laden_foc = laden_foc_per_hour * laden_sailing_time
                # Leg内総FOCを算出
                leg_total_FOC_speed = ballast_foc + laden_foc

                # 総FOC（予測値）に加算
                all_foc_simulation += leg_total_FOC_speed

                #Fuel取得
                fuel_list = convertFuelOileStringToList(res_simulation_speed[0]["fuel"]["S"])

                # 燃料別にCO2排出量を算出し、予測値に加算
                all_co2_simulation += calc_fuel_total_co2(fuel_list, leg_total_FOC_speed)
                
            # 当年分のdistance、CO2排出量に予測値を加算
            tmp_ytd_distance = float(res_vesselalarm[0]["Januarytonow_distance"]["S"])
            tmp_ytd_foc = float(res_vesselalarm[0]["Januarytonow_foc"]["S"])
            tmp_ytd_co2 = float(res_vesselalarm[0]["Januarytonow_co2"]["S"])
            tmp_eoy_distance = tmp_ytd_distance + all_distance_simulation
            tmp_eoy_foc = tmp_ytd_foc + all_foc_simulation
            tmp_eoy_co2 = tmp_ytd_co2 + all_co2_simulation

            # CII算出のため、各種マスタを取得
            cii_ref = select.get_cii_ref(res_vesselmaster[0]["VesselType"]["S"])
            cii_rating = select.get_cii_rating(res_vesselmaster[0]["VesselType"]["S"])
            cii_reduction_rate = select.get_cii_reduction_rate(str(dt_now.year))

            # End of YearのCII算出
            tmp_eoy_cii_value = 0
            tmp_eoy_cii_Score = ""

            # distanceが0以外の場合に計算
            if (tmp_eoy_distance != 0):
                tmp_eoy_cii_value, tmp_eoy_cii_Score = calc_cii(tmp_eoy_co2, tmp_eoy_distance, cii_ref, cii_rating, cii_reduction_rate , res_vesselmaster)

            print(f"imo: {imo}, simulation cii value:{tmp_eoy_cii_value}, simulation cii value:{tmp_eoy_cii_Score}")
        
        
        if select_year == now_year:
            # 選択年が当年の場合、YearToDate、EndOfYearに値を設定

            # 返却用データセットに値を設定
            imo                 = res_vesselalarm[0]["imo"]["S"]
            VesselName          = res_vesselmaster[0]["VesselName"]["S"]
            l4w_distance        = round(float(res_vesselalarm[0]["oneMonth_distance"]["S"]))
            l4w_foc             = round(float(res_vesselalarm[0]["oneMonth_foc"]["S"]), 1)
            l4w_cii_score       = res_vesselalarm[0]["oneMonth"]["S"]
            l4w_from            = res_vesselalarm[0]["oneMonth_from"]["S"]
            l4w_to              = res_vesselalarm[0]["oneMonth_to"]["S"]
            ytd_distance        = round(float(res_vesselalarm[0]["Januarytonow_distance"]["S"]))
            ytd_foc             = round(float(res_vesselalarm[0]["Januarytonow_foc"]["S"]), 1)
            ytd_cii_score       = res_vesselalarm[0]["Januarytonow"]["S"]
            ytd_from            = res_vesselalarm[0]["Januarytonow_from"]["S"]
            ytd_to              = res_vesselalarm[0]["Januarytonow_to"]["S"]
            eoy_distance        = round(tmp_eoy_distance)
            eoy_foc             = round(tmp_eoy_foc, 1)
            eoy_cii_score       = tmp_eoy_cii_Score
            eoy_from            = ""
            eoy_to              = ""
            ly_distance         = round(float(res_vesselalarm[0]["LastYear_distance"]["S"]))
            ly_foc              = round(float(res_vesselalarm[0]["LastYear_foc"]["S"]), 1)
            ly_cii_score        = res_vesselalarm[0]["LastYear"]["S"]
            ly_from             = res_vesselalarm[0]["LastYear_from"]["S"]
            ly_to               = res_vesselalarm[0]["LastYear_to"]["S"]
        
        else:
            # 選択年が前年以前の場合、YearToDateには値設定せず、EndOfYearに「Januarytonow」の値を設定

            # 返却用データセットに値を設定
            imo                 = imo
            VesselName          = res_vesselmaster[0]["VesselName"]["S"]
            l4w_distance        = round(float(res_vesselalarm[0]["oneMonth_distance"]["S"])) if res_vesselalarm else 0
            l4w_foc             = round(float(res_vesselalarm[0]["oneMonth_foc"]["S"]), 1) if res_vesselalarm else 0
            l4w_cii_score       = res_vesselalarm[0]["oneMonth"]["S"] if res_vesselalarm else ""
            l4w_from            = res_vesselalarm[0]["oneMonth_from"]["S"] if res_vesselalarm else ""
            l4w_to              = res_vesselalarm[0]["oneMonth_to"]["S"] if res_vesselalarm else ""
            ytd_distance        = 0
            ytd_foc             = 0
            ytd_cii_score       = ""
            ytd_from            = ""
            ytd_to              = ""
            eoy_distance        = round(float(res_vesselalarm[0]["Januarytonow_distance"]["S"])) if res_vesselalarm else 0
            eoy_foc             = round(float(res_vesselalarm[0]["Januarytonow_foc"]["S"]), 1) if res_vesselalarm else 0
            eoy_cii_score       = res_vesselalarm[0]["Januarytonow"]["S"] if res_vesselalarm else ""
            eoy_from            = res_vesselalarm[0]["Januarytonow_from"]["S"] if res_vesselalarm else ""
            eoy_to              = res_vesselalarm[0]["Januarytonow_to"]["S"] if res_vesselalarm else ""
            ly_distance         = round(float(res_vesselalarm[0]["LastYear_distance"]["S"])) if res_vesselalarm else 0
            ly_foc              = round(float(res_vesselalarm[0]["LastYear_foc"]["S"]), 1) if res_vesselalarm else 0
            ly_cii_score        = res_vesselalarm[0]["LastYear"]["S"] if res_vesselalarm else ""
            ly_from             = res_vesselalarm[0]["LastYear_from"]["S"] if res_vesselalarm else ""
            ly_to               = res_vesselalarm[0]["LastYear_to"]["S"] if res_vesselalarm else ""

        data = {
            "imo"               : imo,
            "VesselName"        : VesselName,
            "l4w_distance"      : l4w_distance,
            "l4w_foc"           : l4w_foc,
            "l4w_cii_score"     : l4w_cii_score,
            "l4w_from"          : l4w_from,
            "l4w_to"            : l4w_to,
            "ytd_distance"      : ytd_distance,
            "ytd_foc"           : ytd_foc,
            "ytd_cii_score"     : ytd_cii_score,
            "ytd_from"          : ytd_from,
            "ytd_to"            : ytd_to,
            "eoy_distance"      : eoy_distance,
            "eoy_foc"           : eoy_foc,
            "eoy_cii_score"     : eoy_cii_score,
            "eoy_from"          : eoy_from,
            "eoy_to"            : eoy_to,
            "ly_distance"       : ly_distance,
            "ly_foc"            : ly_foc,
            "ly_cii_score"      : ly_cii_score,
            "ly_from"           : ly_from,
            "ly_to"             : ly_to,
        }

        data_list.append(data)
        group_imo_list.append({"imoNo":imo,"VesselName": VesselName})
         
    # ソート実行-------------------------------------------------------
    new_imo_list = sorted(group_imo_list, key=lambda x: x['VesselName'])
    
    new_data_list = sorted(data_list, key=lambda x: x['VesselName'])
       
    group_list = ast.literal_eval(group_id)
    new_group_list = group_list
    
    group_id = group_list[0]
    
    # 選択肢の先頭にFavoriteを追加
    new_group_list.insert(0, "Favorite")
    datas = {
        "datas":
            {
                "user_id":user_id,
                "group_id":group_id,
                "company_id":company_id,
                "gid":gid,
                "gidList":new_group_list,
                "imoList":new_imo_list,
                "rows": new_data_list, 
            }
    }
    datas = json.dumps(datas)
    print(f"datas{type(datas)}: {datas}")
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
