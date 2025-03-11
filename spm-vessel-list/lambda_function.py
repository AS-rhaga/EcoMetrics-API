
from time import sleep
from botocore.errorfactory import ClientError
import boto3
import json
from datetime import datetime
import ast
import os

from dynamodb import select, upsert
from vesselinfo import vessel_total
from Util import Util
from calculate import calculate_function

def make_fuel_oil_type_info_list():

    # Ecoで使用する燃料の情報リスト
    fuel_oil_info_list = {
        "HFO_info_list": [],
        "LFO_info_list": [],
        "MDO_info_list": [],
        "MGO_info_list": [],
        "LNG_OMS_info_list": [],
        "LNG_OSS_info_list": [],
        "LNG_ODS_info_list": [],
        "LPG_Butane_info_list": [],
        "LPG_Puropane_info_list": [],
        "H2_Ng_info_list"       : [],
        "NH3_Ng_info_list"      : [],
        "Methanol_Ng_info_list" : [],
        "NH3_eFuel_info_list"   : []
    }

    # 燃料情報リストを取得し、データセットを作成する
    fuel_oil_name_list = ["HFO", "LFO", "MDO", "MGO", "LNG(Otto Medium Speed)", "LNG(Otto Slow Speed)", "LNG(Otto Diesel Speed)", "LPG(Butane)", "LPG(Propane)", "H2(Natural gas)", "NH3(Natural gas)", "Methanol(Natural gas)", "NH3(e-fuel)"]
    fuel_oil_type_info_list = []

    for fuel_oil_name in fuel_oil_name_list:
        fuel_oil_type_info_list.append(select.get_fuel_oil_type(fuel_oil_name)[0])

    for fuel_oil_type_info in fuel_oil_type_info_list:
        name = fuel_oil_type_info["fuel_oil_type"]["S"]

        # それぞれの
        if name == "HFO":
            fuel_oil_info_list["HFO_info_list"] = fuel_oil_type_info
        elif name == "LFO":
            fuel_oil_info_list["LFO_info_list"] = fuel_oil_type_info
        elif name == "MDO":
            fuel_oil_info_list["MDO_info_list"] = fuel_oil_type_info
        elif name == "MGO":
            fuel_oil_info_list["MGO_info_list"] = fuel_oil_type_info
        elif name == "LNG(Otto Medium Speed)":        
            fuel_oil_info_list["LNG_OMS_info_list"] = fuel_oil_type_info
        elif name == "LNG(Otto Slow Speed)":
            fuel_oil_info_list["LNG_OSS_info_list"] = fuel_oil_type_info
        elif name == "LNG(Otto Diesel Speed)":
            fuel_oil_info_list["LNG_ODS_info_list"] = fuel_oil_type_info
        elif name == "LPG(Butane)":
            fuel_oil_info_list["LPG_Butane_info_list"] = fuel_oil_type_info
        elif name == "LPG(Propane)":
            fuel_oil_info_list["LPG_Puropane_info_list"] = fuel_oil_type_info
        elif name == "H2(Natural gas)":
            fuel_oil_info_list["H2_Ng_info_list"] = fuel_oil_type_info
        elif name == "NH3(Natural gas)":
            fuel_oil_info_list["NH3_Ng_info_list"] = fuel_oil_type_info
        elif name == "Methanol(Natural gas)":
            fuel_oil_info_list["Methanol_Ng_info_list"] = fuel_oil_type_info
        elif name == "NH3(e-fuel)":
            fuel_oil_info_list["NH3_eFuel_info_list"] = fuel_oil_type_info

    return fuel_oil_info_list

# 燃料別のCO2排出量を算出し、合算したものを返却
def calc_fuel_total_co2(fuel_list, leg_total_FOC, fuel_oil_info_list):

    fuel_total_co2 = 0
        
    for fuel in fuel_list:
        fuel_info_list = fuel.split(',')
        fuel_name = fuel_info_list[0]
        fuel_rate = int(fuel_info_list[1])

        # 燃料別FOC算出
        tmp_fuel_foc =  leg_total_FOC * (fuel_rate / 100)

        # 燃料ごとの係数を掛けて、CO2排出量を算出⇒総CO2排出量（予測値）に加算
        if fuel_name == "HFO":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["HFO_info_list"]["emission_factor"]["S"])
        elif fuel_name == "LFO":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["LFO_info_list"]["emission_factor"]["S"])
        elif fuel_name == "MGO":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["MDO_info_list"]["emission_factor"]["S"])
        elif fuel_name == "MGO":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["MGO_info_list"]["emission_factor"]["S"])
        elif fuel_name == "LNG(Otto Medium Speed)":        
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["LNG_OMS_info_list"]["emission_factor"]["S"])
        elif fuel_name == "LNG(Otto Slow Speed)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["LNG_OSS_info_list"]["emission_factor"]["S"])
        elif fuel_name == "LNG(Otto Diesel Speed)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["LNG_ODS_info_list"]["emission_factor"]["S"])
        elif fuel_name == "LPG(Butane)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["LPG_Butane_info_list"]["emission_factor"]["S"])
        elif fuel_name == "LPG(Propane)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["LPG_Puropane_info_list"]["emission_factor"]["S"])
        elif fuel_name == "H2(Natural gas)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["H2_Ng_info_list"]["emission_factor"]["S"])
        elif fuel_name == "NH3(Natural gas)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["NH3_Ng_info_list"]["emission_factor"]["S"])
        elif fuel_name == "Methanol(Natural gas)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["Methanol_Ng_info_list"]["emission_factor"]["S"])
        elif fuel_name == "NH3(e-fuel)":
            fuel_total_co2 += tmp_fuel_foc * float(fuel_oil_info_list["NH3_eFuel_info_list"]["emission_factor"]["S"])
    
    return fuel_total_co2


def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    # イベントからクエリ取得-------------------------------------------------------
    pathParameters = event['pathParameters']
    pathParameters = pathParameters['proxy']
    pathParameters = pathParameters.split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']
    
    # クエリからユーザID取得-------------------------------------------------------
    user_id = queryStringParameters['user']
    
    # ユーザ付随情報取得-------------------------------------------------------
    res_user      = select.get_user(user_id)
    group_id      = res_user[0]["group_id"]["S"]
    company_id    = res_user[0]["company_id"]["S"]
    last_disp_gid = res_user[0]["last_disp_gid"]["S"]

    # 初回利用ログイン時 or ログイン時初期表示 or 画面遷移を判定---------------------------------------------
    if "init" in queryStringParameters:
        if len(last_disp_gid) > 0 :
            gid = last_disp_gid
        else:
            gid = ast.literal_eval(group_id)[0]
    else:
        gid = queryStringParameters['GID']

    # お気に入りリスト取得-------------------------------------------------------
    res_favorite = select.get_favorite(user_id)
    favorite_imo_list = ast.literal_eval(res_favorite[0]["imo_list"]["S"])
    
    # Favoriteボタンが押下された時の処理-------------------------------------------------------
    if "favorite" in queryStringParameters:
        
        # クエリからお気に入り選択されたImoの情報を取得-------------------------------------------------------
        favorite_query = queryStringParameters['favorite']
        imo_query = queryStringParameters['imo']
        
        # 選択されたImoがお気に入りリストにない場合のみデータ登録-------------------------------------------------------
        if len(favorite_imo_list) == 0: 
            favorite_imo_list.append(imo_query)
        else:
            if imo_query not in favorite_imo_list and favorite_query == "default":
                favorite_imo_list.append(imo_query)
            elif imo_query in favorite_imo_list and favorite_query == "checked":
                favorite_imo_list.remove(imo_query)
        
        dataSet = {
            "user_id"       : user_id,
            "company_id"    : company_id,
            "imo_list"      : json.dumps(favorite_imo_list),
        }
        message = upsert.upsert_favorite(dataSet)
        
    # Userリストを更新-----------------------------------------------------
    dataSetUser = {
        "user_id"       : user_id,
        "company_id"    : company_id,
        "group_id"      : group_id,
        "last_disp_gid" : gid,
    }
    message = upsert.upsert_user(dataSetUser)

    imo_list = []
    favorite_check = ""
    
    # Imoリストを取得-------------------------------------------------------
    res_group = select.get_group(company_id)
    if gid != "Favorite":
        
        for res_group_item in res_group:
            if res_group_item['group_id']['S'] == gid:
                imo_list = ast.literal_eval(res_group_item["imo_list"]["S"])
                
        # for res_group_item in res_group:
        #     if res_group_item['group_id']['S'] in group_list:
        #         imoList = ast.literal_eval(res_group_item["imo_list"]["S"])
        #         for imo_item in imoList:
        #             imo_list.append(imo_item)
        
        
        # res = get_group(company_id)
        # for res_item in res:
        #     group_list.append(res_item["group_id"]["S"])
        #     if res_item["group_id"]["S"] == gid:
        #         imoList = ast.literal_eval(res_item["imo_list"]["S"])
        #         for imo_item in imoList:
        #             imo_list.append(imo_item)
                    
    else:
        favorite_check = "ok"
        imo_list = favorite_imo_list
        
        
        # favorite_check = "ok"
        # res = get_group(company_id)
        # for res_item in res:
        #     group_list.append(res_item["group_id"]["S"])
        #     imoList = ast.literal_eval(res_item["imo_list"]["S"])
        #     for imo_item in imoList:
        #         imo_list.append(imo_item)
    
    imo_list = list(set(imo_list))

    # 現在の西暦4桁を取得
    dt_now = datetime.now()
    dt_now_str = dt_now.strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # この年のGHG強度上限を算出
    GHG_Max = calculate_function.calc_GHG_Max(year_now)

    # Ecoで使用する燃料の情報リスト
    fuel_oil_type_info_list = make_fuel_oil_type_info_list()
    
    # Imoリストを画面表示用に整形
    data_list = []
    group_imo_list = []
    for imo in imo_list:
        res = select.get_vessel_alarm(imo, year_now)
        res_vesselmaster = select.get_vessel_master(imo)
        res_foc_formulas = select.get_foc_formulas(imo)
        
        # 既存
        favorite            = "default"
        imo                 = res[0]["imo"]["S"]
        VesselName          = res_vesselmaster[0]["VesselName"]["S"]
        LatestUpdate        = res[0]["LatestUpdate"]["S"]
        oneMonth_count      = res[0]["oneMonth_count"]["S"]
        oneMonth            = res[0]["oneMonth"]["S"]
        oneMonth_from       = res[0]["oneMonth_from"]["S"]
        oneMonth_to         = res[0]["oneMonth_to"]["S"]
        ytd_distance        = float(res[0]["Januarytonow_distance"]["S"])
        Januarytonow        = res[0]["Januarytonow"]["S"]
        Januarytonow_from   = res[0]["Januarytonow_from"]["S"]
        Januarytonow_to     = res[0]["Januarytonow_to"]["S"]
        LastYear            = res[0]["LastYear"]["S"]
        LastYear_from       = res[0]["LastYear_from"]["S"]
        LastYear_to         = res[0]["LastYear_to"]["S"]
        cp                  = float(res[0]["cp"]["S"])
        cp_from             = res[0]["cp_from"]["S"]    
        cp_to               = res[0]["cp_to"]["S"]

        # オペレーター毎に分かれているyear-total、シミュレーション用リストのデータをimo単位になるよう合計する
        year_total_data     = vessel_total.sum_year_total(imo, year_now, GHG_Max, ytd_distance, res_foc_formulas, fuel_oil_type_info_list)

        # 新規追加
        End_of_Year_from    = year_now + "-01-01T00:00:00Z"
        End_of_Year_to      = year_now + "-12-31T23:59:59Z"
        EUA_Year_to_Date    = year_total_data["ytd_eua"]
        EUA_End_of_Year     = year_total_data["eoy_eua"]
        CB_Year_to_Date     = year_total_data["ytd_cb"]
        CB_End_of_Year      = year_total_data["eoy_cb"]

        # CII Simulation用データを確認
        # eco-cii-simulation-cond-voyage-plan取得
        res_simulation_voyage = select.get_simulation_voyage_cii(imo, year_now)

        # eco-cii-simulation-cond-speed-plan取得
        res_simulation_speed = select.get_simulation_speed_cii(imo, year_now)

        # print(f"imo:{imo}, res_simulation_voyage:{res_simulation_voyage}, res_simulation_speed:{res_simulation_speed}")

        # シミュレーションの合計値保持のための変数定義
        all_distance_simulation = 0 # 総Distance（予測値）
        all_co2_simulation = 0      # 総CO2排出量（予測値）

        if res_simulation_voyage and res_simulation_voyage[0]["flag"]["S"] == "1":
            # VoyagePlanが取得できたかつflagが1の場合

            print(f"imo:{imo} Voyage Plan Simulation Start.")

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

                # print(f"imo:{imo}, departure_time:{departure_time}, arrival_time:{arrival_time}")

                # Leg航海時間算出
                leg_sailing_time = calculate_function.calc_time_diff(departure_time, arrival_time)
                
                # Legの範囲をそのまま使えるか判定
                if departure_time >= dt_now:

                    # 航海時間、Distanceをそのまま使用
                    calculated_sailing_time = leg_sailing_time
                    calculated_distance = float(res_simulation_voyage[i]["distance"]["S"])
                    
                elif arrival_time >= dt_now:
                    
                    # 現在時間からArrivalTimeまでの時間算出
                    calculated_sailing_time = calculate_function.calc_time_diff(dt_now, arrival_time)
                    # Leg内航海時間との割合を算出し、その分のDistanceを切り出して使用
                    tmp_ratio =  calculated_sailing_time / leg_sailing_time
                    calculated_distance = float(res_simulation_voyage[i]["distance"]["S"]) * tmp_ratio

                    print(f"imo:{imo}, leg_sailing_time:{leg_sailing_time}, calculated_sailing_time:{calculated_sailing_time}, tmp_ratio:{tmp_ratio}, tmpcalculated_distance:{calculated_distance},")

                else:
                    # 上記以外の場合、処理不要のため次の要素へ
                    continue

                # 総Distance（予測値）に計算用Distanceを加算
                all_distance_simulation += calculated_distance

                # LogSpeed算出
                log_speed = calculated_distance / calculated_sailing_time

                print(f"imo:{imo}, log_speed:{log_speed}")

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

                print(f"imo:{imo}, alpah:{alpah}, a:{a}, c:{c}, ")

                # 1日あたりのFOC算出（**は指数）
                foc_per_day = alpah * log_speed ** a + c + auxiliary_equipment
                # 1時間あたりのFOC算出
                foc_per_hour = foc_per_day / 24
                # Leg内総FOCを算出
                leg_total_FOC_voyage = foc_per_hour * calculated_sailing_time

                print(f"imo:{imo}, leg_total_FOC_voyage:{leg_total_FOC_voyage}, foc_per_hour:{foc_per_hour}, foc_per_day:{foc_per_day}")

                #Fuel取得
                fuel_list = Util.convertFuelOileStringToList(res_simulation_voyage[i]["fuel"]["S"])

                # 燃料別にCO2排出量を算出し、予測値に加算
                all_co2_simulation += calc_fuel_total_co2(fuel_list, leg_total_FOC_voyage, fuel_oil_type_info_list)

                print(f"imo:{imo}, all_co2_simulation:{all_co2_simulation}")
                
        elif res_simulation_speed and res_simulation_speed[0]["flag"]["S"] == "1" and res_simulation_speed[0]["year"]["S"] == year_now:
            # SpeedPlanが取得できたかつflagが1の場合
            
            print(f"imo:{imo} Speed Plan Simulation Start.")

            # Time to End of Year算出（年末 - 現在）
            year_end = datetime(int(year_now), 12, 31, 23, 59, 59)
            time_to_end_of_year = calculate_function.calc_time_diff(dt_now, year_end)

            # 航海時間を算出
            sailing_rate = float(res_simulation_speed[0]["salling_rate"]["S"])
            sailing_time = time_to_end_of_year * (sailing_rate / 100)
            port_time    = time_to_end_of_year - sailing_time
            print(f"imo:{(imo)} sailing_time:{(sailing_time)} port_time:{(port_time)}")

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
            # print(f"auxiliary_equipment: {(auxiliary_equipment)}")

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
            ballast_foc = ballast_foc_per_hour *ballast_sailing_time
            laden_foc = laden_foc_per_hour *ballast_sailing_time
            port_total_foc = auxiliary_equipment / 24 * port_time
            # Leg内総FOCを算出
            leg_total_FOC_speed = ballast_foc + laden_foc + port_total_foc

            #Fuel取得
            fuel_list = Util.convertFuelOileStringToList(res_simulation_speed[0]["fuel"]["S"])

            # 燃料別にCO2排出量を算出し、予測値に加算
            all_co2_simulation += calc_fuel_total_co2(fuel_list, leg_total_FOC_speed, fuel_oil_type_info_list)
            
        # 当年分のdistance、CO2排出量に予測値を加算
        tmp_ytd_distance = float(res[0]["Januarytonow_distance"]["S"])
        tmp_ytd_co2 = float(res[0]["Januarytonow_co2"]["S"])
        tmp_eof_distance = tmp_ytd_distance + all_distance_simulation
        tmp_eof_co2 = tmp_ytd_co2 + all_co2_simulation

        # 当年分のdistance、CO2排出量に予測値を加算
        tmp_ytd_co2 = float(res[0]["Januarytonow_co2"]["S"])
        tmp_eof_co2 = tmp_ytd_co2 + all_co2_simulation

        # CII算出のため、各種マスタを取得
        cii_ref = select.get_cii_ref(res_vesselmaster[0]["VesselType"]["S"])
        cii_rating = select.get_cii_rating(res_vesselmaster[0]["VesselType"]["S"])
        cii_reduction_rate = select.get_cii_reduction_rate(str(year_now))

        # End of YearのCII算出
        print(f"imo:{imo}, tmp_eof_co2:{(tmp_eof_co2)}, tmp_eof_distance:{(tmp_eof_distance)}, cii_ref, cii_rating:{(cii_rating)}, cii_reduction_rate:{(cii_reduction_rate)}, res_vesselmaster:{(res_vesselmaster)}")
        tmp_eof_cii_value, CII_End_of_Year = calculate_function.calc_cii(tmp_eof_co2, tmp_eof_distance, cii_ref, cii_rating, cii_reduction_rate, res_vesselmaster)
            
        data = {
            "imo"               : imo,
            "favorite"          : favorite, 
            "VesselName"        : VesselName, 
            "LatestUpdate"      : LatestUpdate, 
            "oneMonth"          : oneMonth,
            "oneMonth_from"     : oneMonth_from,
            "oneMonth_to"       : oneMonth_to,
            "oneMonth_count"    : oneMonth_count,
            "Januarytonow"      : Januarytonow, 
            "Januarytonow_from" : Januarytonow_from, 
            "Januarytonow_to"   : Januarytonow_to, 
            "LastYear"          : LastYear, 
            "LastYear_from"     : LastYear_from, 
            "LastYear_to"       : LastYear_to, 
            "cp"                : cp, 
            "cp_from"           : cp_from, 
            "cp_to"             : cp_to,
            "CIIEndofYear"      : CII_End_of_Year,
            "EndofYear_from"    : End_of_Year_from,
            "EndofYear_to"      : End_of_Year_to,
            "EUAYeartoDate"     : EUA_Year_to_Date,
            "EUAEndofYear"      : EUA_End_of_Year,
            "CBYeartoDate"      : CB_Year_to_Date,
            "CBEndofYear"       : CB_End_of_Year
        }
        
        # お気に入り登録されているImoだけにお気に入り表示するための判定-------------------------------------------------------
        if favorite_check =="ok":
            if imo in favorite_imo_list:
                data["favorite"] = "checked"
                data_list.append(data)
                group_imo_list.append({"imoNo":imo,"VesselName": VesselName})
        else:
            if imo in favorite_imo_list:
                data["favorite"] = "checked"
            data_list.append(data)
            group_imo_list.append({"imoNo":imo,"VesselName": VesselName})
    
    
    # ソート実行-------------------------------------------------------
    new_group_imo_list = sorted(group_imo_list, key=lambda x: x['VesselName'])
    
    new_data_list = sorted(data_list, key=lambda x: x['VesselName'])
       
    group_list = ast.literal_eval(group_id)
    # new_group_list = sorted(group_list)
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
                "imoList":new_group_imo_list,
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
