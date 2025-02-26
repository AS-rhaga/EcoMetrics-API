
import json
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

from dynamodb import select, insert
from Util import Util


# 事前に登録したスピコンカーブを取得
def create_registered_spcon_curve(speed_consumption_curve):
    # print(f"speed_consumption_curve: {speed_consumption_curve})")
    
    res_sp_list = []
    for i in range(len(speed_consumption_curve)):
        sp_dict = {
            "alpha" : float(speed_consumption_curve[i]["alpha"]["S"]),
            "a"     : float(speed_consumption_curve[i]["a"]["S"]),
            "C"     : float(speed_consumption_curve[i]["C"]["S"]),
        }
        res_sp_list.append(sp_dict)
    # print(f"res_sp_list: {res_sp_list})")
    
    cp_curve = {
        "alpha"   : res_sp_list[0]["alpha"],
        "a"       : res_sp_list[0]["a"],
        "C"       : res_sp_list[0]["C"],
    }
    rf_curve = {
        "alpha"   : res_sp_list[1]["alpha"],
        "a"       : res_sp_list[1]["a"],
        "C"       : res_sp_list[1]["C"],
    }
    
    return cp_curve, rf_curve


# CII算出メソッド
def calc_cii(co2, distance, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER, fromDate, toDate):
    
    imo         = VESSELMASTER[0]["imo"]["S"]
    VesselName  = VESSELMASTER[0]["VesselName"]["S"]
    VesselType  = VESSELMASTER[0]["VesselType"]["S"]
    OilType     = VESSELMASTER[0]["OilType"]["S"]
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
        
        
    # print(f"imo: {imo}, VesselName: {VesselName}, VesselType: {VesselType}, dwt: {dwt}, gt: {gt}, weight: {weight}")
        
        
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
    
    # print(f"less: {less}, less_value: {less_value}, less_more: {less_more}, more: {more}, more_value: {more_value}")
    # print(f"less_a: {less_a}, less_c: {less_c}, less_more_a: {less_more_a}, less_more_c: {less_more_c}, more_a: {more_a}, more_c: {more_c}")
    
    
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
    
    # print(f"a_G2: {a_G2}, c_G2: {c_G2}")
    
    
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
    
    # print(f"rating_1: {rating_1}, rating_2: {rating_2}, rating_3: {rating_3}, rating_4: {rating_4}")
        
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
        
    
    IMO         = imo
    NAME        = VesselName
    TYPE        = VesselType
    DWT         = dwt
    GT          = gt
    DISTANCE    = distance
    OIL         = OilType
    CO2         = co2
    a_G2        = a_G2
    c_G2        = c_G2
    REDUCTION   = reduction_rate
    CIIATTAINED = CII_Attained
    CIIREF      = CII_Reference
    CIIREQ      = CII_Required
    CIICALC     = CII_Calculated
    CIISCORE    = CII_Score
    FROM        = fromDate.strftime('%Y/%m/%d %H:%M:%S.%f')
    TO          = toDate.strftime('%Y/%m/%d %H:%M:%S.%f')
    
    VALUE = [
        IMO,
        FROM,
        TO,
        NAME,
        TYPE,
        DWT,
        GT,
        DISTANCE,
        OIL,
        CO2,
        a_G2,
        c_G2,
        REDUCTION,
        CIIATTAINED,
        CIIREF,
        CIIREQ,
        CIICALC,
        CIISCORE,
    ]
    
    return CII_Calculated, CII_Score


def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    # 2024年の日付オブジェクト作成（検索のスタートを2024年とする）
    dt_2024 = datetime(2024, 1, 1, 0, 0, 0)
    dt_2024_year = int(dt_2024.year)

    # 現在日付の日付オブジェクト作成
    dt_now = datetime.now()
    dt_now_year = int(dt_now.year)

    # IMO番号一覧取得
    res_vessel = select.get_vessel()

    imo_list = []

    for i in range(len(res_vessel)):

        imo = res_vessel[i]["imo"]["S"]

        imo_list.append(imo)

        # マスタ取得 
        VESSELMASTER =select.get_vesselmaster(imo)
        FUELOILTYPE = select.get_fuel_oil_type(VESSELMASTER[0]["OilType"]["S"])
        co2_factor = float(FUELOILTYPE[0]["emission_factor"]["S"])

        cii_ref = select.get_cii_ref(VESSELMASTER[0]["VesselType"]["S"])
        cii_rating = select.get_cii_rating(VESSELMASTER[0]["VesselType"]["S"])

        speed_consumption_curve = select.get_speed_consumption_curve(imo)
        cp_curve, rf_curve = create_registered_spcon_curve(speed_consumption_curve)

        # 現在の年から2024年までループ（現在が2025年の場合、2025、2024で2回ループ）
        for year in range(dt_now_year - 1, dt_2024_year - 1, -1):

            # 処理対象年の年始、年末日付を取得
            dt_January_from = datetime(year = year, month = 1, day = 1)
            dt_January_from_str = dt_January_from.strftime('%Y-%m-%dT%H:%M:%SZ')
            print(f"imo:{imo}, dt_January_from_str{type(dt_January_from_str)}: {dt_January_from_str}")
            dt_January_to = datetime(year = year, month = 12, day = 31, hour = 23, minute = 59, second = 59, microsecond = 999999)
            dt_January_to_str = dt_January_to.strftime('%Y-%m-%dT%H:%M:%SZ')
            print(f"imo:{imo}, dt_January_to_str{type(dt_January_to_str)}: {dt_January_to_str}")

            # 処理対象年の年末日付の1カ月前の日付を取得
            dt_oneMonth = dt_January_to + relativedelta(months=-1)
            dt_oneMonth_str = dt_oneMonth.strftime('%Y-%m-%dT%H:%M:%SZ')

            # 処理対象年前年の年始、年末日付を取得
            dt_last_year_from = datetime(year = year - 1, month = 1, day = 1)
            dt_last_year_from_str = dt_last_year_from.strftime('%Y-%m-%dT%H:%M:%SZ')
            print(f"imo:{imo}, dt_last_year_from_str{type(dt_last_year_from_str)}: {dt_last_year_from_str}")
            dt_last_year_to = datetime(year = year - 1, month = 12, day = 31, hour = 23, minute = 59, second = 59, microsecond = 999999)
            dt_last_year_to_str = dt_last_year_to.strftime('%Y-%m-%dT%H:%M:%SZ')
            print(f"imo:{imo}, dt_last_year_to_str{type(dt_last_year_to_str)}: {dt_last_year_to_str}")

            # cii_reduction取得
            cii_reduction_rate_oneMonth = select.get_cii_reduction_rate(str(dt_oneMonth.year))
            cii_reduction_rate_Januarytonow = select.get_cii_reduction_rate(str(dt_January_to.year))
            cii_reduction_rate_LastYear = select.get_cii_reduction_rate(str(dt_last_year_to.year))

            # NoonReportデータ取得 from: dt_last_year_from_str, to: dt_January_to_str
            res_np = select.get_noonreport(imo, dt_last_year_from_str, dt_January_to_str)

            # スピコンアラート算出用
            cp_count = 0
            rf_count = 0           
            
            # Januarytonow
            Januarytonow_foc_value = 0
            Januarytonow_cii_value = 0
            Januarytonow_co2_value = 0
            Januarytonow_distance_value = 0
            Januarytonow_cii_Score = ""
            
            # LastYear
            LastYear_foc_value = 0
            LastYear_cii_value = 0
            LastYear_co2_value = 0
            LastYear_distance_value = 0
            LastYear_cii_Score = ""

            # oneMonth
            oneMonth_foc_value = 0
            oneMonth_co2_value = 0
            oneMonth_distance_value = 0
            oneMonth_cii_value = 0
            oneMonth_cii_Score = ""
            oneMonth_count = 0

            dt_latest_update = datetime.now()
            
            # NoonReportの取得件数分ループ
            for res in res_np:
                
                utc_date = res["timestamp"]["S"] if 'timestamp' in res and res["timestamp"]["S"] != "" else ""
                timstamp = Util.timestamp_Z_datetime(utc_date)
                dt_latest_update = timstamp

                # OG Distance
                og_distance = float(res["og_distance"]["S"]) if 'og_distance' in res and res["og_distance"]["S"] != "" else ""
                
                # Log Speed
                log_speed = float(res["log_speed"]["S"]) if 'log_speed' in res and res["log_speed"]["S"] != "" else ""

                # me_foc
                me_foc = float(res["me_foc"]["S"]) if 'me_foc' in res and res["me_foc"]["S"] != "" else ""
                # displacement
                displacement = float(res["displacement"]["S"]) if 'displacement' in res and res["displacement"]["S"] != "" else ""
                # me_load
                me_load = float(res["me_load"]["S"]) if 'me_load' in res and res["me_load"]["S"] != "" else ""
                
                # CO2,foc
                total_foc = ""
                co2 = ""
                if 'total_foc' in res and res["total_foc"]["S"] != "":
                    total_foc = float(res["total_foc"]["S"])
                    co2 = total_foc * co2_factor
                
                # Last Month
                if dt_oneMonth <= timstamp and timstamp <= dt_now:
                    # print(f"dt_oneMonth-dt_now: {dt_oneMonth}-{dt_now}, oneMonth_distance_value: {oneMonth_distance_value}, oneMonth_co2_value: {oneMonth_co2_value}")
                    oneMonth_distance_value += og_distance if og_distance != "" else 0
                    oneMonth_foc_value += total_foc if total_foc != "" else 0
                    oneMonth_co2_value += co2 if co2 != "" else 0
                    
                    # 5~23knotの場合だけをフィルタする
                    if log_speed != "" and me_foc != "" and total_foc != "" and displacement != "" and me_load != "" and 5 <= log_speed and log_speed <= 23:
                        oneMonth_count += 1
                        foc_cp = cp_curve["alpha"] * pow(log_speed, cp_curve["a"]) + cp_curve["C"]
                        cp_count += 1 if foc_cp < me_foc else 0
                        
                        foc_rf = rf_curve["alpha"] * pow(log_speed, rf_curve["a"]) + rf_curve["C"]
                        rf_count += 1 if foc_rf < me_foc else 0
                
                # This Year
                if dt_January_from <= timstamp and timstamp <= dt_January_to:
                    Januarytonow_distance_value += og_distance if og_distance != "" else 0
                    Januarytonow_foc_value += total_foc if total_foc != "" else 0
                    Januarytonow_co2_value += co2 if co2 != "" else 0

                # Last Year
                if dt_last_year_from <= timstamp and timstamp <= dt_last_year_to:
                    LastYear_distance_value += og_distance if og_distance != "" else 0
                    LastYear_foc_value += total_foc if total_foc != "" else 0
                    LastYear_co2_value += co2 if co2 != "" else 0

            # CII算出
            if oneMonth_distance_value != 0 and oneMonth_co2_value != 0:
                oneMonth_cii_value, oneMonth_cii_Score = calc_cii(oneMonth_co2_value, oneMonth_distance_value, cii_ref, cii_rating, cii_reduction_rate_oneMonth , VESSELMASTER, dt_oneMonth, dt_now)
            if Januarytonow_distance_value != 0 and Januarytonow_co2_value != 0:
                Januarytonow_cii_value, Januarytonow_cii_Score = calc_cii(Januarytonow_co2_value, Januarytonow_distance_value, cii_ref, cii_rating, cii_reduction_rate_Januarytonow, VESSELMASTER, dt_January_from, dt_January_to)
            if LastYear_distance_value != 0 and LastYear_co2_value != 0:
                LastYear_cii_value, LastYear_cii_Score = calc_cii(LastYear_co2_value, LastYear_distance_value, cii_ref, cii_rating, cii_reduction_rate_LastYear, VESSELMASTER, dt_last_year_from, dt_last_year_to)
        
            latest_year = str(dt_latest_update.year)
            latest_month = str(dt_latest_update.month) if len(str(dt_latest_update.month)) > 1 else "0" + str(dt_latest_update.month)
            latest_day = str(dt_latest_update.day) if len(str(dt_latest_update.day)) > 1 else "0" + str(dt_latest_update.day)
            latest_update = f"{latest_year}/{latest_month}/{latest_day}"

            # 登録用データセット作成
            dataSet = {
                "imo"                    : imo,
                "year"                   : str(year),
                "cp"                     : str(cp_count),
                "cp_from"                : dt_oneMonth_str,
                "cp_to"                  : dt_January_to_str,
                "favorite"               : "checked",
                "Januarytonow_val"       : str(Januarytonow_cii_value),
                "Januarytonow"           : Januarytonow_cii_Score,
                "Januarytonow_from"      : dt_January_from_str,
                "Januarytonow_to"        : dt_January_to_str,
                "Januarytonow_distance"  : str(Januarytonow_distance_value),
                "Januarytonow_foc"       : str(Januarytonow_foc_value),
                "Januarytonow_co2"       : str(Januarytonow_co2_value),
                "LastYear_val"           : str(LastYear_cii_value),
                "LastYear"               : LastYear_cii_Score,
                "LastYear_from"          : dt_last_year_from_str,
                "LastYear_to"            : dt_last_year_to_str,
                "LastYear_distance"      : str(LastYear_distance_value),
                "LastYear_foc"           : str(LastYear_foc_value),
                "LastYear_co2"           : str(LastYear_co2_value),
                "LatestUpdate"           : latest_update,
                "oneMonth_val"           : str(oneMonth_cii_value),
                "oneMonth"               : oneMonth_cii_Score,
                "oneMonth_from"          : dt_oneMonth_str,
                "oneMonth_to"            : dt_January_to_str,
                "oneMonth_distance"      : str(oneMonth_distance_value),
                "oneMonth_foc"           : str(oneMonth_foc_value),
                "oneMonth_co2"           : str(oneMonth_co2_value),
                "oneMonth_count"         : str(oneMonth_count),
                "rf"                     : str(rf_count),
                "rf_from"                : dt_oneMonth_str,
                "rf_to"                  : dt_January_to_str,
                "VesselName"             : VESSELMASTER[0]["VesselName"]["S"],
            }
            # print(f"dataSet[{type(dataSet)}]: {dataSet}")
            
            # DB登録
            insert.upsert(imo, dataSet)

        # print(f"The number of ships is {imo_count}. {imo_list}")
    datas = json.dumps(imo_list)
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }

    

    
