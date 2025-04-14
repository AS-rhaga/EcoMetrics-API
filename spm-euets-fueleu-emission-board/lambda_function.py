
import math
import json
from datetime import datetime
import ast
import re

# import pandas as pd

import auth
from dynamodb import select
from Util import Util
from EmissionBoard import EmissionBoard
from Leg import leg
from Voyage import voyage
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 消費燃料の割合リストを作成する。
def make_fuel_list(display_leg):

    fuel_list = []

    lng = float(display_leg["total_lng"]["S"])
    hfo = float(display_leg["total_hfo"]["S"])
    lfo = float(display_leg["total_lfo"]["S"])
    mdo = float(display_leg["total_mdo"]["S"])
    mgo = float(display_leg["total_mgo"]["S"])
    foc = float(display_leg["total_foc"]["S"])

    if foc > 0:
        if lng > 0:
            lng_rate = str(round(lng / foc * 100))
            fuel_list.append(["LNG", lng_rate])
        if hfo > 0:
            hfo_rate = str(round(hfo / foc * 100))
            fuel_list.append(["HFO", hfo_rate])
        if lfo > 0:
            lfo_rate = str(round(lfo / foc * 100))
            fuel_list.append(["LFO", lfo_rate])
        if mdo > 0:
            mdo_rate = str(round(mdo / foc * 100))
            fuel_list.append(["MDO", mdo_rate])
        if mgo > 0:
            mgo_rate = str(round(mgo / foc * 100))
            fuel_list.append(["MGO", mgo_rate])
    
    return fuel_list

# 燃料消費で実際に排出されたco2(MT)
def calc_co2_Actual(lng, hfo, lfo, mdo, mgo, fuel_oil_type_list):

    sum_co2 = 0

    if lng > 0:
        lng_emission_factor =  float(fuel_oil_type_list["LNG_info_list"]["emission_factor"]["S"])
        sum_co2 += lng * lng_emission_factor
    if hfo > 0:
        hfo_emission_factor =  float(fuel_oil_type_list["HFO_info_list"]["emission_factor"]["S"])
        sum_co2 += hfo * hfo_emission_factor
    if lfo > 0:
        lfo_emission_factor =  float(fuel_oil_type_list["LFO_info_list"]["emission_factor"]["S"])
        sum_co2 += lfo * lfo_emission_factor
    if mdo > 0:
        mdo_emission_factor =  float(fuel_oil_type_list["MDO_info_list"]["emission_factor"]["S"])
        sum_co2 += mdo * mdo_emission_factor
    if mgo > 0:
        mgo_emission_factor =  float(fuel_oil_type_list["MGO_info_list"]["emission_factor"]["S"])
        sum_co2 += mgo * mgo_emission_factor

    return sum_co2

#実際のGHG強度を算出するメソッド
def calc_GHG_Actual(lng, hfo, lfo, mdo, mgo, fuel_oil_type_list):
    sum_ghg = 0
    sum_foc = 0

    if lng > 0:
        lng_ghg_intensity =  float(fuel_oil_type_list["LNG_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lng * lng_ghg_intensity
        sum_foc += lng
    if hfo > 0:
        hfo_ghg_intensity =  float(fuel_oil_type_list["HFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += hfo * hfo_ghg_intensity
        sum_foc += hfo
    if lfo > 0:
        lfo_ghg_intensity =  float(fuel_oil_type_list["LFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lfo * lfo_ghg_intensity
        sum_foc += lfo
    if mdo > 0:
        mdo_ghg_intensity =  float(fuel_oil_type_list["MDO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += mdo * mdo_ghg_intensity
        sum_foc += mdo
    if mgo > 0:
        mgo_ghg_intensity =  float(fuel_oil_type_list["MGO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += mgo * mgo_ghg_intensity
        sum_foc += mgo

    GHG_Actual = 0
    if sum_foc != 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_energy(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    energy_lng = 0
    energy_hfo = 0
    energy_lfo = 0
    energy_mdo = 0
    energy_mgo = 0

    if total_lng > 0:
        lng_lcv =  float(fuel_oil_info_list["LNG_OMS_info_list"]["lcv"]["S"])
        energy_lng += total_lng * lng_lcv
    if total_hfo > 0:
        hfo_lcv =  float(fuel_oil_info_list["HFO_info_list"]["lcv"]["S"])
        energy_hfo += total_hfo * hfo_lcv
    if total_lfo > 0:
        lfo_lcv =  float(fuel_oil_info_list["LFO_info_list"]["lcv"]["S"])
        energy_lfo += total_lfo * lfo_lcv
    if total_mdo > 0:
        mdo_lcv =  float(fuel_oil_info_list["MDO_info_list"]["lcv"]["S"])
        energy_mdo += total_mdo * mdo_lcv
    if total_mgo > 0:
        mgo_lcv =  float(fuel_oil_info_list["MGO_info_list"]["lcv"]["S"])
        energy_mgo += total_mgo * mgo_lcv

    energy = (energy_lng + energy_hfo + energy_lfo + energy_mdo + energy_mgo)
    return energy

def calc_GHG_Max(year):
    year = int(year)
    if year <= 2024:
        target_rate = 0
    elif year <= 2029:
        target_rate = 2
    elif year <= 2034:
        target_rate = 6
    elif year <= 2039:
        target_rate = 14.5
    elif year <= 2044:
        target_rate = 31
    elif year <= 2049:
        target_rate = 62
    else:
        target_rate = 80

    GHG_Max = round(float(91.16 * (100 - float(target_rate)) / 100), 2)
    print(f"GHG_Max{type(GHG_Max)}: {GHG_Max}")
    return GHG_Max

# CBとCB Costを返す
def calc_cb(year_timestamp, energy, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb      = 0
    cb_cost = 0

    # ゼロ割防止のため、燃料消費量がゼロでない場合のみ計算する
    if total_lng + total_hfo + total_lfo + total_mdo + total_mgo > 0:

        GHG_Actual = calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, fuel_oil_info_list)
        cb = (GHG_Max - GHG_Actual) * energy
        print(f"cb{type(cb)}: {cb}")

        if cb < 0:
            cb_cost = abs(cb) / GHG_Actual * 24000 / 410000

    return cb, cb_cost

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

def choise_period_noonreport(res_np, time_from, time_to):

    nr_list = []
    
    for i in range(len(res_np)):
        record     = res_np[i]
        local_date = record['local_date']['S']

        # local_timeを変換
        # datetimeオブジェクトに変換
        dt = datetime.strptime(local_date, "%Y/%m/%d %H:%M")
        # フォーマットを適用して再度文字列化
        formatted_date = dt.strftime("%Y/%m/%d %H:%M")
        local_date =  formatted_date

        if time_from <= local_date and local_date < time_to:
            nr_list.append(record)

    return nr_list

# 最大桁の値以外を０に変換する（例：43⇒40、549⇒500、0.345→0.3）
def maxDigitOnly(n):
    # 数値を文字列に変換
    print(f"maxDigitOnly input:{(n)}")
    str_n = str(n)
    # ゼロ以外の最初の桁を取得
    count  = 0
    degits = ""
    for i in range(len(str_n)):
        count += 1
        degits = degits + str_n[i]
        if str_n[i] != "0" and str_n[i] != ".":
            break
    # 整数部分がある場合、整数部分の残りを0で埋める
    if n >= 1:
        result = degits + '0' * (len(str(round(n))) - count)
    else:
        result = degits
    print(f"maxDigitOnly result:{(result)}")
    # 数値型に変換して返す
    return float(result)

def lambda_handler(event, context):

    print(f"event{type(event)}: {event}")
    
    pathParameters = event['pathParameters']['proxy'].split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']

    # imo取得
    imo = pathParameters[0]
    print(f"imo{type(imo)}: {imo}")

    # パラメーター取得
    user_id        = queryStringParameters['user_id']
    Timestamp_from = queryStringParameters['Timestamp_from']
    Timestamp_to   = queryStringParameters['Timestamp_to']
    unit           = queryStringParameters["Unit"]

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

    res_noonreport_list    = []

    # 返却用データセット
    # 新規作成分
    EUAList                = []
    CBList                 = []
    VoyageInformationTotal = []
    VoyageInformationList  = []

    # 検索範囲の西暦を取得
    year_from = Timestamp_from[0:4]
    year_to   = Timestamp_to[0:4]

    # leg, voyage管理リスト
    display_leg_list = []
    recalc_leg_list  = []
    display_voyage_list = []
    recalc_voyage_list  = []

    # 指定期間の年の、必要テーブルを取得
    res_leg_list    = []
    res_voyage_list = []

    # EmissionBoardに渡すリスト
    unit_timestamp_list = []

    # Ecoで使用する燃料の情報リスト
    fuel_oil_info_list = {
        "LNG_info_list": [],
        "HFO_info_list": [],
        "LFO_info_list": [],
        "MDO_info_list": [],
        "MGO_info_list": []
    }

    # Fuel-Oil-Typeリストを取得する
    fuel_oil_name_list = ["HFO", "LFO", "MDO", "MGO", "LNG(Otto Medium Speed)", "LNG(Otto Slow Speed)", "LNG(Otto Diesel Speed)", "LPG(Butane)", "LPG(Propane)", "H2(Natural gas)", "NH3(Natural gas)", "Methanol(Natural gas)", "NH3(e-fuel)"]
    fuel_oil_type_info_list = []
    for fuel_oil_name in fuel_oil_name_list:
        fuel_oil_type_info_list.append(select.get_fuel_oil_type(fuel_oil_name))
    
    # print(f"fuel_oil_type_info_list[{type(fuel_oil_type_info_list)}]: {fuel_oil_type_info_list}")

    for i in range(len(fuel_oil_type_info_list)):
        name = fuel_oil_type_info_list[i][0]["fuel_oil_type"]["S"]
        if  name == "LNG(Otto Medium Speed)":
            fuel_oil_info_list["LNG_info_list"] = fuel_oil_type_info_list[i][0]
        elif name == "HFO":
            fuel_oil_info_list["HFO_info_list"] = fuel_oil_type_info_list[i][0]
        elif name == "LFO":
            fuel_oil_info_list["LFO_info_list"] = fuel_oil_type_info_list[i][0]
        elif name == "MDO":
            fuel_oil_info_list["MDO_info_list"] = fuel_oil_type_info_list[i][0]
        elif name == "MGO":
            fuel_oil_info_list["MGO_info_list"] = fuel_oil_type_info_list[i][0]

    # 検索期間のNoonReportを取得する。
    response = select.get_noonreport(imo, Timestamp_from, Timestamp_to)
    print(f"期間内のNoonReport数:{(len(response))}")

    # user情報取得
    res_user = select.get_user(user_id)
    # group情報取得
    res_group = select.get_group_one_record(res_user[0]["company_id"]["S"], "admin")

    # EUA単価を取得
    eua_price = int(res_group[0]["eua_price"]["S"]) if "eua_price" in res_group[0] else 0

    # Y軸設定用の変数定義
    max_eua = 0
    min_cb = 0
    max_cb = 0

    # Leg単位で表示する場合
    if unit == "Leg":

        res_leg_list = []
        from_to_leg_cb = 0
        from_to_leg_cb_cost = 0

        # 検索範囲が年を跨ぐ場合、何年分のデータが必要かを確認
        year_range = int(year_to) - int(year_from)
        
        for i in range(year_range + 1):
            leg_total_list = select.get_leg_total(imo, str(int(year_from) + i))

            for i in range(len(leg_total_list)):
                res_leg_list.append(leg_total_list[i])

        # EU Rateがゼロのレコードを除いたリストを作成する
        res_leg = []
        for leg_info in res_leg_list:
            leg_eu_rate = leg_info["eu_rate"]["S"]
            if leg_eu_rate != "0":
                res_leg.append(leg_info)
        print(f"len(res_leg): {len(res_leg)}")
        res_leg_list = res_leg
        
        # 必要な年数分のLegリストでループ
        for res in res_leg_list:

            # DepartureTime取得
            departure_time_string = res["departure_time"]["S"]
            departure_time_dt = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            departure_time = departure_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # ArrivalTime取得
            arrival_time_string = res["arrival_time"]["S"]
            arrival_time_dt = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
            arrival_time = arrival_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # 指定期間内で完結している
            if Timestamp_from <= departure_time and arrival_time <= Timestamp_to:
                display_leg_list.append(res)
                print("このlegは指定期間内で完結している")
            
            # 指定期間内に含まれている
            elif departure_time < Timestamp_from and Timestamp_from < arrival_time and arrival_time <= Timestamp_to:
                # departure_timeにTimestamp_fromを設定
                # 文字列をdatetimeオブジェクトに変換
                departure_datetime_obj = datetime.strptime(Timestamp_from, "%Y-%m-%dT%H:%M:%SZ")
                # datetimeオブジェクトを新しい形式の文字列に変換
                departure_formatted_date_str = departure_datetime_obj.strftime("%Y/%m/%d %H:%M")
                # resに設定
                res["departure_time"]["S"] = departure_formatted_date_str
                recalc_leg_list.append(res)
                print("このlegは途中から最後まで含まれる")
                
            elif Timestamp_from <= departure_time and departure_time < Timestamp_to and Timestamp_to < arrival_time:
                # arrival_timeにTimestamp_toを設定
                # 文字列をdatetimeオブジェクトに変換
                arrival_datetime_obj = datetime.strptime(Timestamp_to, "%Y-%m-%dT%H:%M:%SZ")
                # datetimeオブジェクトを新しい形式の文字列に変換
                arrival_formatted_date_str = arrival_datetime_obj.strftime("%Y/%m/%d %H:%M")
                # resに設定
                res["arrival_time"]["S"] = arrival_formatted_date_str
                recalc_leg_list.append(res)
                print("このlegは最初から途中まで含まれる")
            
            # departure、arrivalの間にfrom、toが収まっている
            elif departure_time < Timestamp_from and Timestamp_to < arrival_time:
                # departure_timeにTimestamp_from、arrival_timeにTimestamp_toを設定# 元の文字列
                # 文字列をdatetimeオブジェクトに変換（ISO形式の解析）
                departure_datetime_obj = datetime.strptime(Timestamp_from, "%Y-%m-%dT%H:%M:%SZ")
                arrival_datetime_obj = datetime.strptime(Timestamp_to, "%Y-%m-%dT%H:%M:%SZ")
                # datetimeオブジェクトを新しい形式の文字列に変換
                departure_formatted_date_str = departure_datetime_obj.strftime("%Y/%m/%d %H:%M")
                arrival_formatted_date_str = arrival_datetime_obj.strftime("%Y/%m/%d %H:%M")
                # resに設定
                res["departure_time"]["S"] = departure_formatted_date_str
                res["arrival_time"]["S"] = arrival_formatted_date_str

                recalc_leg_list.append(res)
                print("このlegは指定期間内を含んでいる")
            
            # 指定期間外
            else:
                None

        # 部分的に含まれるLegリストについて、整形する
        for recalc_res in recalc_leg_list:
            # DepartureTime取得
            departure_time_string = recalc_res["departure_time"]["S"]
            departure_time_dt = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            departure_time = departure_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # ArrivalTime取得
            arrival_time_string = recalc_res["arrival_time"]["S"]
            arrival_time_dt = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
            arrival_time = arrival_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # res_noonreport_list = select.get_noonreport(imo, departure_time, arrival_time)
            res_noonreport_list = choise_period_noonreport(response, departure_time_string, arrival_time_string)
            record_data = leg.make_leg_data(imo, departure_time, arrival_time, res_noonreport_list, fuel_oil_info_list, recalc_res)
            if len(record_data) > 0:
                resord_data_ = {
                        "imo"           : {"S": record_data["imo"]},
                        "leg_no"        : {"S": record_data["leg_no"]},
                        "departure_port": {"S": record_data["departure_port"]},
                        "departure_time": {"S": departure_time_string},
                        "arrival_port"  : {"S": record_data["arrival_port"]},
                        "arrival_time"  : {"S": arrival_time_string},
                        "eu_rate"       : {"S": record_data["eu_rate"]},
                        "displacement"  : {"S": record_data["displacement"]},
                        "leg_type"      : {"S": record_data["leg_type"]},
                        "distance"      : {"S": record_data["distance"]},
                        "total_lng"     : {"S": record_data["total_lng"]},
                        "total_hfo"     : {"S": record_data["total_hfo"]},
                        "total_lfo"     : {"S": record_data["total_lfo"]},
                        "total_mdo"     : {"S": record_data["total_mdo"]},
                        "total_mgo"     : {"S": record_data["total_mgo"]},
                        "total_foc"     : {"S": record_data["total_foc"]},
                        "eta_local_date": {"S": record_data["eta_local_date"]},
                        "eua"           : {"S": record_data["eua"]},
                        "cb"            : {"S": record_data["cb"]}
                    }
                display_leg_list.append(resord_data_)

        # 航海開始時刻でソートする
        display_leg_list_sorted = sorted(display_leg_list, key=lambda x:x["departure_time"]["S"])

        # 合計用変数を設定
        leg_count      = 0
        total_foc      = 0
        eu_actual_total_foc = 0
        total_GHG      = 0
        total_distance = 0
        total_eua      = 0
        total_cb       = 0
        total_cb_cost  = 0
        from_to_leg_total_cb = 0
        from_to_leg_total_cb_cost = 0 

        # CB折れ線グラフ用
        from_to_leg_lng = 0
        from_to_leg_hfo = 0
        from_to_leg_lfo = 0
        from_to_leg_mdo = 0
        from_to_leg_mgo = 0

        # 数値保持用
        # keep_year = display_leg_list_sorted[0]["leg_no"]["S"][0:4]
        keep_year = display_leg_list_sorted[0].get("leg_no", {}).get("S", "")[:4] if display_leg_list_sorted else ""
        keep_total_cb      = 0
        keep_total_cb_cost = 0

        # 画面表示用に整形
        for display_leg in display_leg_list_sorted:

            # leg_noから西暦4桁を取得
            leg_year = display_leg["leg_no"]["S"][0:4]
            
            # 各legの燃料消費量
            tmp_total_lng = float(display_leg["total_lng"]["S"])
            tmp_total_hfo = float(display_leg["total_hfo"]["S"])
            tmp_total_lfo = float(display_leg["total_lfo"]["S"])
            tmp_total_mdo = float(display_leg["total_mdo"]["S"])
            tmp_total_mgo = float(display_leg["total_mgo"]["S"])

            # GHG強度算出
            tmpGHGActual = calc_GHG_Actual(tmp_total_lng, tmp_total_hfo, tmp_total_lfo, tmp_total_mdo, tmp_total_mgo, fuel_oil_info_list)
            
            # EU関連対象の燃料消費で実際に排出されたco2(MT)
            tmp_co2_Actual = calc_co2_Actual(tmp_total_lng, tmp_total_hfo, tmp_total_lfo, tmp_total_mdo, tmp_total_mgo, fuel_oil_info_list)

            # CBコスト取得
            tmp_cb = float(display_leg["cb"]["S"])

            if tmp_cb >= 0:
                tmp_total_cb_cost = 0
            else:
                res_year_total_list    = select.get_year_total(imo)
                year_total_list_sorted = sorted(res_year_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

                flag_count = 0

                for year in year_total_list_sorted:
                            fine_flag = year["fine_flag"]["S"]
                            if fine_flag == "1":
                                flag_count += 1
                            else:
                                break
                                
                penalty_factor = 1 + (flag_count / 10)
                tmp_total_cb_cost  = abs(tmp_cb) * penalty_factor * 2400 / (tmpGHGActual * 41000)

            # 各種合計値に加算
            total_foc      += float(display_leg["total_foc"]["S"])
            eu_actual_total_foc += float(display_leg["total_foc"]["S"]) / (int(display_leg["eu_rate"]["S"]) / 100)
            total_GHG      += tmp_co2_Actual
            total_distance += float(display_leg["distance"]["S"]) if display_leg["distance"]["S"] != "" else 0
            total_eua      += float(display_leg["eua"]["S"])
            total_cb       += tmp_cb
            total_cb_cost  += tmp_total_cb_cost

            # レグの開始時刻の年をkeep_yearと比較し、from_to_leg（GHG強度上限削減率が同じ期間）の燃料消費量に加算する
            if leg_year == keep_year:
                from_to_leg_lng += tmp_total_lng
                from_to_leg_hfo += tmp_total_hfo
                from_to_leg_lfo += tmp_total_lfo
                from_to_leg_mdo += tmp_total_mdo
                from_to_leg_mgo += tmp_total_mgo

            else:
                # 2025年～2050年の間、5年ごとにGHG強度上限の削減率が変わる
                if int(leg_year) % 5 == 0 and int(leg_year) <= 2050:
                    keep_total_cb      = from_to_leg_total_cb
                    keep_total_cb_cost = from_to_leg_total_cb_cost
                    print(f"leg_count:{(leg_count)} keep_total_cb:{(keep_total_cb)} keep_total_cb_cost:{(keep_total_cb_cost)}")
                    from_to_leg_lng = tmp_total_lng
                    from_to_leg_hfo = tmp_total_hfo
                    from_to_leg_lfo = tmp_total_lfo
                    from_to_leg_mdo = tmp_total_mdo
                    from_to_leg_mgo = tmp_total_mgo

                # 新しい年もGHG強度上限の削減率が変わらない
                else:
                    from_to_leg_lng += tmp_total_lng
                    from_to_leg_hfo += tmp_total_hfo
                    from_to_leg_lfo += tmp_total_lfo
                    from_to_leg_mdo += tmp_total_mdo
                    from_to_leg_mgo += tmp_total_mgo
            
            keep_year = leg_year 

            # 通番を設定する
            leg_count      += 1

            # 消費燃料リストを作成
            fuel_list = make_fuel_list(display_leg)

            # TotalTIme算出
            # DepartureTime取得
            departure_time_string = display_leg["departure_time"]["S"]
            departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            # ArrivalTime取得
            arrival_time_string = display_leg["arrival_time"]["S"]
            arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
            # DepartureTimeからArrivalTimeまでのTotalTimeを算出
            tmp_total_time = calc_time_diff(departure_time, arrival_time)
            # EU範囲内の実績FOC算出
            eu_actual_foc = float(display_leg["total_foc"]["S"]) / (int(display_leg["eu_rate"]["S"]) / 100)

            #画面返却用データを作成
            datarow = {
                "leg_no"     : leg_count,
                "state"         : display_leg["leg_type"]["S"],
                "operator"      : "",
                "departure_port": display_leg["departure_port"]["S"],
                "departure_time": display_leg["departure_time"]["S"],
                "arrival_port"  : display_leg["arrival_port"]["S"],
                "arrival_time"  : display_leg["arrival_time"]["S"],
                "total_time"    : tmp_total_time,
                "eu_rate"       : display_leg["eu_rate"]["S"],
                "displacement"  : str(round(float(display_leg["displacement"]["S"]))) if display_leg["displacement"]["S"] != "" else "",
                "distance"      : str(round(float(display_leg["distance"]["S"]))) if display_leg["distance"]["S"] != "" else "",
                "fuel"          : fuel_list,
                "foc"           : str(round(eu_actual_foc, 1)),
                "eua"           : str(round(float(display_leg["eua"]["S"]))),
                "cb"            : str(round(float(display_leg["cb"]["S"]) / 1000000, 1))
            }
            VoyageInformationList.append(datarow)
            
            # EUA, CBのグラフ用データを作成
            eua_data = [leg_count, float(display_leg["eua"]["S"])]
            EUAList.append(eua_data)

            print(f"【from_to_leg】lng:{(from_to_leg_lng)}, hfo:{(from_to_leg_hfo)}, lfo:{(from_to_leg_lfo)}, mdo:{(from_to_leg_mdo)}, mgo:{(from_to_leg_mgo)}")
            from_to_leg_energy = calc_energy(from_to_leg_lng, from_to_leg_hfo, from_to_leg_lfo, from_to_leg_mdo, from_to_leg_mgo, fuel_oil_info_list)
            from_to_leg_cb, from_to_leg_cb_cost = calc_cb(int(leg_year), from_to_leg_energy, from_to_leg_lng, from_to_leg_hfo, from_to_leg_lfo, from_to_leg_mdo, from_to_leg_mgo, fuel_oil_info_list)
            from_to_leg_total_cb      = from_to_leg_cb      + keep_total_cb
            from_to_leg_total_cb_cost = from_to_leg_cb_cost + keep_total_cb_cost
            cb_data  = [leg_count, from_to_leg_total_cb / 1000000]
            CBList.append(cb_data)

            # Y軸設定用にMax値、Min値を取得
            max_eua = float(display_leg["eua"]["S"]) if max_eua < float(display_leg["eua"]["S"]) else max_eua
            max_cb = from_to_leg_total_cb if max_cb < from_to_leg_total_cb else max_cb
            min_cb = from_to_leg_total_cb if min_cb > from_to_leg_total_cb else min_cb

            # 各LEGの開始時刻（timestamp）のリストを作成する。
            # timestamp_departure_time = pd.Timestamp(display_leg["departure_time"]["S"])
            departure_time_string = display_leg["departure_time"]["S"]
            departure_time_dt = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            departure_time = departure_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            timestamp_departure_time = Util.timestamp_calc_datetime(departure_time)
            unit_timestamp_list.append(timestamp_departure_time)

            # 最後の要素の場合のみ、arrival_timeを追加
            if display_leg == display_leg_list_sorted[-1]:
                arrival_time_string = display_leg["arrival_time"]["S"]
                arrival_time_dt = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
                arrival_time = arrival_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                timestamp_arrival_time = Util.timestamp_calc_datetime(arrival_time)
                unit_timestamp_list.append(timestamp_arrival_time)
        
        VoyageInformationTotal = {
            "foc"     : round(eu_actual_total_foc),
            "ghg"     : round(total_GHG),
            "distance": round(total_distance),
            "eua"     : round(total_eua),
            "eua_cost": round(total_eua) * eua_price,
            "cb"      : round(from_to_leg_total_cb / 1000000, 1),
            "cb_cost" : round(from_to_leg_total_cb_cost)
        }


    # Voyage単位で表示する場合
    else:
        res_voyage_list = []
        from_to_voyage_cb      = 0
        from_to_voyage_cb_cost = 0

        # 検索範囲が年を跨ぐ場合、何年分のデータが必要かを確認
        year_range = int(year_to) - int(year_from)
        
        for i in range(year_range + 1):
            voyage_total_list = select.get_voyage_total(imo, str(int(year_from) + i))

            for i in range(len(voyage_total_list)):
                res_voyage_list.append(voyage_total_list[i])

        # 必要な年数分のVoyageリストでループ
        for res in res_voyage_list:

            # DepartureTime取得
            departure_time_string = res["departure_time"]["S"]
            departure_time_dt = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            departure_time = departure_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # ArrivalTime取得
            arrival_time_string = res["arrival_time"]["S"]
            arrival_time_dt = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
            arrival_time = arrival_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # 指定期間内で完結している
            if Timestamp_from <= departure_time and arrival_time <= Timestamp_to:
                display_voyage_list.append(res)
                print(f"voyage_no:{(res["voyage_no"]["S"])}は期間内で完結")
            
            # 指定期間内に含まれている
            elif departure_time < Timestamp_from and Timestamp_from < arrival_time and arrival_time <= Timestamp_to:
                # departure_timeにTimestamp_fromを設定
                # 文字列をdatetimeオブジェクトに変換
                departure_datetime_obj = datetime.strptime(Timestamp_from, "%Y-%m-%dT%H:%M:%SZ")
                # datetimeオブジェクトを新しい形式の文字列に変換
                departure_formatted_date_str = departure_datetime_obj.strftime("%Y/%m/%d %H:%M")
                # resに設定
                res["departure_time"]["S"] = departure_formatted_date_str
                recalc_voyage_list.append(res)
                print(f"voyage_no:{(res["voyage_no"]["S"])}は途中から最後まで")
                
            elif Timestamp_from <= departure_time and departure_time < Timestamp_to and Timestamp_to < arrival_time:
                # arrival_timeにTimestamp_toを設定
                # 文字列をdatetimeオブジェクトに変換
                arrival_datetime_obj = datetime.strptime(Timestamp_to, "%Y-%m-%dT%H:%M:%SZ")
                # datetimeオブジェクトを新しい形式の文字列に変換
                arrival_formatted_date_str = arrival_datetime_obj.strftime("%Y/%m/%d %H:%M")
                # resに設定
                res["arrival_time"]["S"] = arrival_formatted_date_str
                recalc_voyage_list.append(res)
                print(f"voyage_no:{(res["voyage_no"]["S"])}は最初から途中まで")

            # departure、arrivalの間にfrom、toが収まっている
            elif departure_time < Timestamp_from and Timestamp_to < arrival_time:
                # departure_timeにTimestamp_from、arrival_timeにTimestamp_toを設定# 元の文字列
                # 文字列をdatetimeオブジェクトに変換（ISO形式の解析）
                departure_datetime_obj = datetime.strptime(Timestamp_from, "%Y-%m-%dT%H:%M:%SZ")
                arrival_datetime_obj = datetime.strptime(Timestamp_to, "%Y-%m-%dT%H:%M:%SZ")
                # datetimeオブジェクトを新しい形式の文字列に変換
                departure_formatted_date_str = departure_datetime_obj.strftime("%Y/%m/%d %H:%M")
                arrival_formatted_date_str = arrival_datetime_obj.strftime("%Y/%m/%d %H:%M")
                # resに設定
                res["departure_time"]["S"] = departure_formatted_date_str
                res["arrival_time"]["S"] = arrival_formatted_date_str
                recalc_voyage_list.append(res)
                print(f"voyage_no:{(res["voyage_no"]["S"])}は検索期間を含む")
            
            # 指定期間外
            else:
                None
            
        # 部分的に含まれるvoyageリストについて、整形する
        for recalc_res in recalc_voyage_list:
            # DepartureTime取得
            departure_time_string = recalc_res["departure_time"]["S"]
            departure_time_dt = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            departure_time = departure_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # ArrivalTime取得
            arrival_time_string = recalc_res["arrival_time"]["S"]
            arrival_time_dt = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
            arrival_time = arrival_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            res_noonreport_list = choise_period_noonreport(response, departure_time_string, arrival_time_string)
            print(f"期間内のNoonReportレコード数:{(len(res_noonreport_list))}")
            record_data = voyage.make_voyage_data(imo, year_from, year_to, res_noonreport_list, fuel_oil_info_list, recalc_res)
            if len(record_data) > 0:
                resord_data_ = {
                    "imo"           : {"S": record_data["imo"]},
                    "voyage_no"     : {"S": record_data["voyage_no"]},
                    "operator"      : {"S": record_data["operator"]},
                    "departure_port": {"S": record_data["departure_port"]},
                    "departure_time": {"S": record_data["departure_time"]},
                    "arrival_port"  : {"S": record_data["arrival_port"]},
                    "arrival_time"  : {"S": record_data["arrival_time"]},
                    "dispracement"  : {"S": record_data["displacement"]},
                    "distance"      : {"S": record_data["distance"]},
                    "total_lng"     : {"S": record_data["total_lng"]},
                    "total_hfo"     : {"S": record_data["total_hfo"]},
                    "total_lfo"     : {"S": record_data["total_lfo"]},
                    "total_mdo"     : {"S": record_data["total_mdo"]},
                    "total_mgo"     : {"S": record_data["total_mgo"]},
                    "total_foc"     : {"S": record_data["total_foc"]},
                    "eu_actual_foc" : {"S": record_data["eu_actual_foc"]},
                    "eua"           : {"S": record_data["eua"]},
                    "cb"            : {"S": record_data["cb"]},
                    "cb_cost"       : {"S": record_data["cb_cost"]}
                }
                display_voyage_list.append(resord_data_)

        # 航海開始時刻でソートする
        display_voyage_list_sorted = sorted(display_voyage_list, key=lambda x:x["departure_time"]["S"])

        # 合計用変数を設定
        voyage_count   = 0
        total_foc      = 0
        eu_actual_total_foc = 0
        total_GHG      = 0
        total_distance = 0
        total_eua      = 0
        total_cb       = 0
        total_cb_cost  = 0
        from_to_voyage_total_cb = 0
        from_to_voyage_total_cb_cost = 0

        # CB折れ線グラフ用
        from_to_voyage_lng = 0
        from_to_voyage_hfo = 0
        from_to_voyage_lfo = 0
        from_to_voyage_mdo = 0
        from_to_voyage_mgo = 0

        # 数値保持用
        # keep_year = display_voyage_list_sorted[0]["voyage_no"]["S"][0:4]
        keep_year = display_voyage_list_sorted[0].get("voyage_no", {}).get("S", "")[:4] if display_voyage_list_sorted else ""

        keep_total_cb      = 0
        keep_total_cb_cost = 0

        print(f"display_voyage_list_sorted:{(display_voyage_list_sorted)}")
        # 画面表示用に整形
        for display_voyage in display_voyage_list_sorted:

            # voyage_noから西暦4桁を取得
            voyage_year = display_voyage["voyage_no"]["S"][0:4]
            # GHG強度算出
            tmp_total_lng = float(display_voyage["total_lng"]["S"])
            tmp_total_hfo = float(display_voyage["total_hfo"]["S"])
            tmp_total_lfo = float(display_voyage["total_lfo"]["S"])
            tmp_total_mdo = float(display_voyage["total_mdo"]["S"])
            tmp_total_mgo = float(display_voyage["total_mgo"]["S"])

            tmpGHGActual = calc_GHG_Actual(tmp_total_lng, tmp_total_hfo, tmp_total_lfo, tmp_total_mdo, tmp_total_mgo, fuel_oil_info_list)

            # EU関連対象の燃料消費で実際に排出されたco2(MT)
            tmp_co2_Actual = calc_co2_Actual(tmp_total_lng, tmp_total_hfo, tmp_total_lfo, tmp_total_mdo, tmp_total_mgo, fuel_oil_info_list)

            # CBコスト算出
            tmp_cb = float(display_voyage["cb"]["S"])

            if tmp_cb >= 0:
                tmp_total_cb_cost = 0
            else:
                res_year_total_list    = select.get_year_total(imo)
                year_total_list_sorted = sorted(res_year_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

                flag_count = 0

                for year in year_total_list_sorted:
                            fine_flag = year["fine_flag"]["S"]
                            if fine_flag == "1":
                                flag_count += 1
                            else:
                                break
                                
                penalty_factor = 1 + (flag_count / 10)
                tmp_total_cb_cost  = abs(tmp_cb) * penalty_factor * 2400 / (tmpGHGActual * 41000)

                print(f"tmp_total_cb_cost:{tmp_total_cb_cost}")

            # 各種合計値に加算
            total_foc      += float(display_voyage["total_foc"]["S"])
            eu_actual_total_foc += float(display_voyage["eu_actual_foc"]["S"])
            total_GHG      += tmp_co2_Actual
            total_distance += float(display_voyage["distance"]["S"]) if display_voyage["distance"]["S"] != "" else 0
            total_eua      += float(display_voyage["eua"]["S"])
            total_cb       += tmp_cb
            total_cb_cost  += tmp_total_cb_cost

            # レグの開始時刻の年をkeep_yearと比較し、from_to_voyage（GHG強度上限削減率が同じ期間）の燃料消費量に加算する            
            if voyage_year == keep_year:
                from_to_voyage_lng += tmp_total_lng
                from_to_voyage_hfo += tmp_total_hfo
                from_to_voyage_lfo += tmp_total_lfo
                from_to_voyage_mdo += tmp_total_mdo
                from_to_voyage_mgo += tmp_total_mgo
            else:
                # 2025年～2050年の間、5年ごとにGHG強度上限の削減率が変わる
                print(f"voyage_count:{(voyage_count)} voyage_year:{(voyage_year)}")
                if int(voyage_year) % 5 == 0 and int(voyage_year) <= 2050:
                    keep_total_cb      = from_to_voyage_total_cb
                    keep_total_cb_cost = from_to_voyage_total_cb_cost
                    print(f"voyage_count:{(voyage_count)} keep_total_cb:{(keep_total_cb)} keep_total_cb_cost:{(keep_total_cb_cost)}")
                    from_to_voyage_lng = tmp_total_lng
                    from_to_voyage_hfo = tmp_total_hfo
                    from_to_voyage_lfo = tmp_total_lfo
                    from_to_voyage_mdo = tmp_total_mdo
                    from_to_voyage_mgo = tmp_total_mgo
                # 新しい年もGHG強度上限の削減率が変わらない
                else:
                    from_to_voyage_lng += tmp_total_lng
                    from_to_voyage_hfo += tmp_total_hfo
                    from_to_voyage_lfo += tmp_total_lfo
                    from_to_voyage_mdo += tmp_total_mdo
                    from_to_voyage_mgo += tmp_total_mgo

            keep_year = voyage_year

            # 通番を設定する
            voyage_count      += 1

            # 消費燃料リストを作成
            fuel_list = make_fuel_list(display_voyage)

            # TotalTIme算出
            # DepartureTime取得
            departure_time_string = display_voyage["departure_time"]["S"]
            departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            # ArrivalTime取得
            arrival_time_string = display_voyage["arrival_time"]["S"]
            arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
            # DepartureTimeからArrivalTimeまでのTotalTimeを算出
            tmp_total_time = calc_time_diff(departure_time, arrival_time)

            #画面返却用データを作成
            datarow = {
                "serial_no"     : voyage_count,
                "state"         : "",
                "operator"      : display_voyage["operator"]["S"],
                "departure_port": display_voyage["departure_port"]["S"],
                "departure_time": display_voyage["departure_time"]["S"],
                "arrival_port"  : display_voyage["arrival_port"]["S"],
                "arrival_time"  : display_voyage["arrival_time"]["S"],
                "total_time"    : tmp_total_time,
                "displacement"  : str(round(float(display_voyage["dispracement"]["S"]))) if display_voyage["dispracement"]["S"] != "" else "",
                "distance"      : str(round(float(display_voyage["distance"]["S"]))) if display_voyage["distance"]["S"] != "" else "",
                "fuel"          : fuel_list,
                "foc"           : str(round(float(display_voyage["eu_actual_foc"]["S"]), 1)),
                "eua"           : str(round(float(display_voyage["eua"]["S"]))),
                "cb"            : str(round(float(display_voyage["cb"]["S"]) / 1000000, 1))
            }
            VoyageInformationList.append(datarow)
            
            # EUA, CBのグラフ用データを作成
            eua_data = [voyage_count, float(display_voyage["eua"]["S"])]
            EUAList.append(eua_data)

            print(f"【from_to_voyage】lng:{(from_to_voyage_lng)}, hfo:{(from_to_voyage_hfo)}, lfo:{(from_to_voyage_lfo)}, mdo:{(from_to_voyage_mdo)}, mgo:{(from_to_voyage_mgo)}")
            from_to_voyage_energy = calc_energy(from_to_voyage_lng, from_to_voyage_hfo, from_to_voyage_lfo, from_to_voyage_mdo, from_to_voyage_mgo, fuel_oil_info_list)
            from_to_voyage_cb, from_to_voyage_cb_cost = calc_cb(int(voyage_year), from_to_voyage_energy, from_to_voyage_lng, from_to_voyage_hfo, from_to_voyage_lfo, from_to_voyage_mdo, from_to_voyage_mgo, fuel_oil_info_list)
            from_to_voyage_total_cb      = from_to_voyage_cb      + keep_total_cb
            from_to_voyage_total_cb_cost = from_to_voyage_cb_cost + keep_total_cb_cost
            cb_data  = [voyage_count, from_to_voyage_total_cb / 1000000]
            CBList.append(cb_data)

            # Y軸設定用にMax値、Min値を取得
            max_eua = float(display_voyage["eua"]["S"]) if max_eua < float(display_voyage["eua"]["S"]) else max_eua
            max_cb = from_to_voyage_total_cb if max_cb < from_to_voyage_total_cb else max_cb
            min_cb = from_to_voyage_total_cb if min_cb > from_to_voyage_total_cb else min_cb

            # 各VOYAGEの開始時刻（timestamp）のリストを作成する。
            # timestamp_departure_time = pd.Timestamp(display_voyage["departure_time"]["S"])
            departure_time_string = display_voyage["departure_time"]["S"]
            departure_time_dt = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")
            departure_time = departure_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            timestamp_departure_time = Util.timestamp_calc_datetime(departure_time)
            unit_timestamp_list.append(timestamp_departure_time)

            # 最後の要素の場合のみ、arrival_timeを追加
            if display_voyage == display_voyage_list_sorted[-1]:
                arrival_time_string = display_voyage["arrival_time"]["S"]
                arrival_time_dt = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")
                arrival_time = arrival_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                timestamp_arrival_time = Util.timestamp_calc_datetime(arrival_time)
                unit_timestamp_list.append(timestamp_arrival_time)

        
        VoyageInformationTotal = {
            "foc"     : round(eu_actual_total_foc),
            "ghg"     : round(total_GHG),
            "distance": round(total_distance),
            "eua"     : round(total_eua),
            "eua_cost": round(total_eua) * eua_price,
            "cb"      : round(from_to_voyage_total_cb / 1000000, 1),
            "cb_cost" : round(from_to_voyage_total_cb_cost)
        }

    # その他のグラフデータ取得（CII EmissionBoardから流用）
    print(f"unit_timestamp_list:{unit_timestamp_list}")
    data = EmissionBoard.util_EmissionBoard_main(imo, Timestamp_from, Timestamp_to, response, unit_timestamp_list, unit)

    # CB_YAXISの桁数を調整する
    max_cb = max_cb / 1000000
    min_cb = min_cb / 1000000
    # max/minについて、プラスマイナス場合分け
    if max_cb > 0 and min_cb > 0:
        sub_max_cb = max_cb
        sub_min_cb = 0
    elif max_cb < 0 and min_cb < 0:
        sub_max_cb = 0
        sub_min_cb = min_cb
    else:
        sub_max_cb = max_cb
        sub_min_cb = min_cb

    # CB範囲(subInterval)桁数によってtickIntervalを設定する
    subInterval  = abs(sub_max_cb - sub_min_cb)
    tickInterval = maxDigitOnly(subInterval / 2)

    datas = {
        "VESSELMASTER"                     : data["VESSELMASTER"],
        "LOGSpeedRate"                     : data["LOGSpeedRate"],
        "LOGSpeedRateUnit"                 : data["LOGSpeedRateUnit"],
        "LOGSpeedRate_Accumulation"        : data["LOGSpeedRate_Accumulation"],
        "LOGSpeedRateUnit_Accumulation"    : data["LOGSpeedRateUnit_Accumulation"],
        "MELoadRate"                       : data["MELoadRate"],
        "MELoadRateUnit"                   : data["MELoadRateUnit"],
        "MELoadRate_Accumulation"          : data["MELoadRate_Accumulation"],
        "MELoadRateUnit_Accumulation"      : data["MELoadRateUnit_Accumulation"],
        "Displacement_Categories"          : data["Displacement_Categories"],
        "DisplacementRate"                 : data["DisplacementRate"],
        "DisplacementRateUnit"             : data["DisplacementRateUnit"],
        "DisplacementRate_Accumulation"    : data["DisplacementRate_Accumulation"],
        "DisplacementRateUnit_Accumulation": data["DisplacementRateUnit_Accumulation"],
        "BeaufortRate"                     : data["BeaufortRate"],
        "BeaufortRateUnit"                 : data["BeaufortRateUnit"],
        "BeaufortRate_Accumulation"        : data["BeaufortRate_Accumulation"],
        "BeaufortRateUnit_Accumulation"    : data["BeaufortRateUnit_Accumulation"],
        "Average"                          : data["Average"],
        "AverageUnit"                      : data["AverageUnit"],
        "EUAList"                          : EUAList,
        "CBList"                           : CBList,
        "VoyageInformationTotal"           : VoyageInformationTotal,
        "VoyageInformationList"            : VoyageInformationList,
        "EUA_YAXIS"                        :{"max": round(max_eua, 0) , "tickInterval": math.ceil(round(max_eua / 2, 0) / 100) * 100 },
        "CB_YAXIS"                         :{"max": max_cb, "min": min_cb, "tickInterval": tickInterval}
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