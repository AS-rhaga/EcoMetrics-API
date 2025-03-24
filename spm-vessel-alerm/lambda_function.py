
from time import sleep
from botocore.errorfactory import ClientError
import boto3
import ast
import json
from datetime import datetime, date, timedelta
import ast
import calendar
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
    cp_curve_alpha = cp_curve["alpha"]
    cp_curve_a = cp_curve["a"]
    cp_curve_C = cp_curve["C"]
    # print(f"CPCurve: FOC = {cp_curve_alpha}(1×V^{cp_curve_a}+{cp_curve_C})")
    
    rf_curve_alpha = rf_curve["alpha"]
    rf_curve_a = rf_curve["a"]
    rf_curve_C = rf_curve["C"]
    # print(f"RefferenceCurve: FOC = {rf_curve_alpha}(1×V^{rf_curve_a}+{rf_curve_C})")
    
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

    if imo == "9468310":
        print(f"VALUE: {VALUE}")
    
    return CII_Calculated, CII_Score


def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    # datetimeをformatを指定して文字列に変換
    dt_now = datetime.now() + timedelta(hours=0)
    print(f"dt_now{type(dt_now)}: {dt_now}")
    
    dt_now_str = dt_now.strftime('%Y-%m-%dT23:59:59Z')
    print(f"dt_now_str{type(dt_now_str)}: {dt_now_str}")
    
    dt_oneMonth = dt_now + timedelta(hours=0, weeks=-4)
    dt_oneMonth = dt_now + relativedelta(months=-1)
    dt_oneMonth_str = dt_oneMonth.strftime('%Y-%m-%dT00:00:00Z')
    print(f"dt_oneMonth_str{type(dt_oneMonth_str)}: {dt_oneMonth_str}")
    
    dt_now_year = int(dt_now.year)
    dt_January_from = datetime(year = dt_now_year, month = 1, day = 1)
    dt_January_from_str = dt_January_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"dt_January_from_str{type(dt_January_from_str)}: {dt_January_from_str}")
    dt_January_to = datetime(year = dt_now_year, month = 12, day = 31, hour = 23, minute = 59, second = 59, microsecond = 999999)
    dt_January_to_str = dt_January_to.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"dt_January_to_str{type(dt_January_to_str)}: {dt_January_to_str}")
    
    dt_now_year = int(dt_now.year)
    dt_last_year_from = datetime(year = dt_now_year-1, month = 1, day = 1)
    dt_last_year_from_str = dt_last_year_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"dt_last_year_from_str{type(dt_last_year_from_str)}: {dt_last_year_from_str}")
    dt_last_year_to = datetime(year = dt_now_year-1, month = 12, day = 31, hour = 23, minute = 59, second = 59, microsecond = 999999)
    dt_last_year_to_str = dt_last_year_to.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"dt_last_year_to_str{type(dt_last_year_to_str)}: {dt_last_year_to_str}")
    
    dt_latest_update = datetime.now()
    
    # IMO番号一覧取得
    res_vessel = select.get_vessel()

    # HFO、LFO、MDO、MGO、LNG(Otto Medium Speed)のemisson_factor取得
    FUELOILTYPE_HFO = select.get_fuel_oil_type("HFO")
    FUELOILTYPE_LFO = select.get_fuel_oil_type("LFO")
    FUELOILTYPE_MDO = select.get_fuel_oil_type("MDO")
    FUELOILTYPE_MGO = select.get_fuel_oil_type("MGO")
    FUELOILTYPE_LNG_MEDIUM = select.get_fuel_oil_type("LNG(Otto Medium Speed)")
    CO2FACTOR_HFO = float(FUELOILTYPE_HFO[0]["emission_factor"]["S"])
    CO2FACTOR_LFO = float(FUELOILTYPE_LFO[0]["emission_factor"]["S"])
    CO2FACTOR_MDO = float(FUELOILTYPE_MDO[0]["emission_factor"]["S"])
    CO2FACTOR_MGO = float(FUELOILTYPE_MGO[0]["emission_factor"]["S"])
    CO2FACTOR_LNG_MEDIUM = float(FUELOILTYPE_LNG_MEDIUM[0]["emission_factor"]["S"])

    imo_list = []
    imo_count = 0
    for i in range(len(res_vessel)):
        imo_count += 1
        imo = res_vessel[i]["imo"]["S"]
        VESSELMASTER = select.get_vesselmaster(imo)      
        
        imo_list.append(imo)
        
        # NoonReportデータ取得 from: dt_last_year_from_str, to: dt_now_str
        res_np = select.get_noonreport(imo, dt_last_year_from_str, dt_now_str)
        # print(f"res_np[{type(res_np)}]: {res_np}")
        
        # スピコンアラート算出用
        cp_count = 0
        rf_count = 0
        speed_consumption_curve = select.get_speed_consumption_curve(imo)
        cp_curve, rf_curve = create_registered_spcon_curve(speed_consumption_curve)
        
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
        
        np_count = 0
        for res in res_np:
            np_count += 1
            
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

            # beaufort
            beaufort = float(res["beaufort"]["S"]) if 'beaufort' in res and res["beaufort"]["S"] != "" else ""

            # 燃料ごとの消費量を取得
            # M/E BOG
            me_bog = float(res["me_bog"]["S"]) if 'me_bog' in res and res["me_bog"]["S"] != "" else ""
            # D/G BOG
            dg_bog = float(res["dg_bog"]["S"]) if 'dg_bog' in res and res["dg_bog"]["S"] != "" else ""
            # GCU BOG
            gcu_bog = float(res["gcu_bog"]["S"]) if 'gcu_bog' in res and res["gcu_bog"]["S"] != "" else ""            
            # M/E HFO
            me_hfo = float(res["me_hfo"]["S"]) if 'me_hfo' in res and res["me_hfo"]["S"] != "" else ""
            # D/G HFO
            dg_hfo = float(res["dg_hfo"]["S"]) if 'dg_hfo' in res and res["dg_hfo"]["S"] != "" else ""
            # BOILER HFO
            boiler_hfo = float(res["boiler_hfo"]["S"]) if 'boiler_hfo' in res and res["boiler_hfo"]["S"] != "" else ""
            # M/E LSFO
            me_lsfo = float(res["me_lsfo"]["S"]) if 'me_lsfo' in res and res["me_lsfo"]["S"] != "" else ""
            # D/G LSFO（SPAS上の名称はge_foc）
            dg_lsfo = float(res["ge_foc"]["S"]) if 'ge_foc' in res and res["ge_foc"]["S"] != "" else ""
            # BOILER LSFO（SPAS上の名称はboiler_foc）
            boiler_lsfo = float(res["boiler_foc"]["S"]) if 'boiler_foc' in res and res["boiler_foc"]["S"] != "" else ""
            # M/E DO
            me_do = float(res["me_do"]["S"]) if 'me_do' in res and res["me_do"]["S"] != "" else ""
            # D/G DO
            dg_do = float(res["dg_do"]["S"]) if 'dg_do' in res and res["dg_do"]["S"] != "" else ""
            # BOILER DO
            boiler_do = float(res["boiler_do"]["S"]) if 'boiler_do' in res and res["boiler_do"]["S"] != "" else ""
            # M/E LSGO
            me_lsgo = float(res["me_lsgo"]["S"]) if 'me_lsgo' in res and res["me_lsgo"]["S"] != "" else ""
            # D/G LSGO
            dg_lsgo = float(res["dg_lsgo"]["S"]) if 'dg_lsgo' in res and res["dg_lsgo"]["S"] != "" else ""
            # BOILER LSGO
            boiler_lsgo = float(res["boiler_lsgo"]["S"]) if 'boiler_lsgo' in res and res["boiler_lsgo"]["S"] != "" else ""
            # IGG GO
            igg_go = float(res["igg_go"]["S"]) if 'igg_go' in res and res["igg_go"]["S"] != "" else ""
            # IGG LSGO
            igg_lsgo = float(res["igg_lsgo"]["S"]) if 'igg_lsgo' in res and res["igg_lsgo"]["S"] != "" else ""

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
            co2 = total_bog * CO2FACTOR_LNG_MEDIUM + total_hfo * CO2FACTOR_HFO + total_lfo * CO2FACTOR_LFO + total_do * CO2FACTOR_MDO + total_go * CO2FACTOR_MGO
                          
            # print(f"timstamp[{timstamp}],dt_oneMonth[{dt_oneMonth}|{type(dt_oneMonth)}]-dt_now[{dt_now}|{type(dt_now)}]: total_foc: {total_foc}, co2: {co2}")
            # print(f"timstamp[{timstamp}],dt_January_from[{dt_January_from}|{type(dt_January_from)}]-dt_January_to[{dt_January_to}|{type(dt_January_to)}]: total_foc: {total_foc}, co2: {co2}")
            # print(f"timstamp[{timstamp}],dt_last_year_from[{dt_last_year_from}|{type(dt_last_year_from)}]-dt_last_year_to[{dt_last_year_to}|{type(dt_last_year_to)}]: total_foc: {total_foc}, co2: {co2}")
                    
            
            # Last Month
            if dt_oneMonth <= timstamp and timstamp <= dt_now:
                # print(f"dt_oneMonth-dt_now: {dt_oneMonth}-{dt_now}, oneMonth_distance_value: {oneMonth_distance_value}, oneMonth_co2_value: {oneMonth_co2_value}")
                oneMonth_distance_value += og_distance if og_distance != "" else 0
                oneMonth_foc_value += total_foc
                oneMonth_co2_value += co2
                
                # 5~23knotの場合だけをフィルタする
                if log_speed != "" and me_foc != "" and displacement != "" and me_load != "" and beaufort != "" and 5 <= log_speed:
                    oneMonth_count += 1
                    foc_cp = cp_curve["alpha"] * pow(log_speed, cp_curve["a"]) + cp_curve["C"]
                    cp_count += 1 if foc_cp < me_foc else 0
                    
                    foc_rf = rf_curve["alpha"] * pow(log_speed, rf_curve["a"]) + rf_curve["C"]
                    rf_count += 1 if foc_rf < me_foc else 0
            
            # This Year
            if dt_January_from <= timstamp and timstamp <= dt_January_to:
                Januarytonow_distance_value += og_distance if og_distance != "" else 0
                Januarytonow_foc_value += total_foc
                Januarytonow_co2_value += co2
            
            # Last Year
            if dt_last_year_from <= timstamp and timstamp <= dt_last_year_to:
                # print(f"Test: timstamp[{timstamp}],dt_last_year_from[{dt_last_year_from}]-dt_last_year_to[{dt_last_year_to}]: total_foc: {total_foc}, co2: {co2}")
                LastYear_distance_value += og_distance if og_distance != "" else 0
                LastYear_foc_value += total_foc
                LastYear_co2_value += co2
            
        # データベースから抽出したレコード数をチェック
        print(f"The number of records that contain data is {np_count}.")
        print(f"The number of records in 1 month is  {oneMonth_count}.")
        
        # CII----------------------------------------------------------
        # マスタデータ抽出
        cii_ref = select.get_cii_ref(VESSELMASTER[0]["VesselType"]["S"])
        cii_rating = select.get_cii_rating(VESSELMASTER[0]["VesselType"]["S"])
        cii_reduction_rate_oneMonth = select.get_cii_reduction_rate(str(dt_now.year))
        cii_reduction_rate_Januarytonow = select.get_cii_reduction_rate(str(dt_January_to.year))
        cii_reduction_rate_LastYear = select.get_cii_reduction_rate(str(dt_last_year_to.year))
        
        # CII算出
        if oneMonth_distance_value != 0 and oneMonth_co2_value != 0:
            oneMonth_cii_value, oneMonth_cii_Score = calc_cii(oneMonth_co2_value, oneMonth_distance_value, cii_ref, cii_rating, cii_reduction_rate_oneMonth , VESSELMASTER, dt_oneMonth, dt_now)
        if Januarytonow_distance_value != 0 and Januarytonow_co2_value != 0:
            Januarytonow_cii_value, Januarytonow_cii_Score = calc_cii(Januarytonow_co2_value, Januarytonow_distance_value, cii_ref, cii_rating, cii_reduction_rate_Januarytonow, VESSELMASTER, dt_January_from, dt_January_to)
        if LastYear_distance_value != 0 and LastYear_co2_value != 0:
            LastYear_cii_value, LastYear_cii_Score = calc_cii(LastYear_co2_value, LastYear_distance_value, cii_ref, cii_rating, cii_reduction_rate_LastYear, VESSELMASTER, dt_last_year_from, dt_last_year_to)
        # print(f"oneMonth_distance_value: {oneMonth_distance_value}, oneMonth_co2_value: {oneMonth_co2_value}, oneMonth_cii_value[{type(oneMonth_cii_value)}]: {oneMonth_cii_value}, oneMonth_cii_Score[{type(oneMonth_cii_Score)}]: {oneMonth_cii_Score}")
        # print(f"Januarytonow_co2_value: {Januarytonow_co2_value}, Januarytonow_distance_value: {Januarytonow_distance_value}, Januarytonow_cii_value[{type(Januarytonow_cii_value)}]: {Januarytonow_cii_value}, Januarytonow_cii_Score[{type(Januarytonow_cii_Score)}]: {Januarytonow_cii_Score}")
        # print(f"LastYear_distance_value: {LastYear_distance_value}, LastYear_co2_value: {LastYear_co2_value}, LastYear_cii_value[{type(LastYear_cii_value)}]: {LastYear_cii_value}, LastYear_cii_Score[{type(LastYear_cii_Score)}]: {LastYear_cii_Score}")
        
        # Spped Consumption alerm----------------------------------------------------------
        cp_check = "out" if cp_count > 1 else "ok"
        rf_check = "out" if rf_count > 5 else "ok"
        # print(f"cp_count: {cp_count}, rf_count: {rf_count}")
        
        year = str(dt_latest_update.year)
        month = str(dt_latest_update.month) if len(str(dt_latest_update.month)) > 1 else "0" + str(dt_latest_update.month)
        day = str(dt_latest_update.day) if len(str(dt_latest_update.day)) > 1 else "0" + str(dt_latest_update.day)
        latest_update = f"{year}/{month}/{day}"
        # print(f"latest_update: type{type(latest_update)}, timestamp: {latest_update}.")
        
        # imo = ""
        VesselName = VESSELMASTER[0]["VesselName"]["S"]
        favorite = "checked"
        LatestUpdate = latest_update
        # cp = cp_check
        cp = str(cp_count)
        cp_from = dt_oneMonth_str
        cp_to = dt_now_str
        # rf = rf_check
        rf = str(rf_count)
        rf_from = dt_oneMonth_str
        rf_to = dt_now_str
        Januarytonow_val        = str(Januarytonow_cii_value)
        Januarytonow            = Januarytonow_cii_Score
        Januarytonow_from       = dt_January_from_str
        Januarytonow_to         = dt_January_to_str
        Januarytonow_distance   = str(Januarytonow_distance_value)
        Januarytonow_foc        = str(Januarytonow_foc_value)
        Januarytonow_co2        = str(Januarytonow_co2_value)
        LastYear_val            = str(LastYear_cii_value)
        LastYear                = LastYear_cii_Score
        LastYear_from           = dt_last_year_from_str
        LastYear_to             = dt_last_year_to_str
        LastYear_distance       = str(LastYear_distance_value)
        LastYear_foc            = str(LastYear_foc_value)
        LastYear_co2            = str(LastYear_co2_value)

        oneMonth_val            = str(oneMonth_cii_value)
        oneMonth                = oneMonth_cii_Score
        oneMonth_from           = dt_oneMonth_str
        oneMonth_to             = dt_now_str
        oneMonth_distance       = str(oneMonth_distance_value)
        oneMonth_foc            = str(oneMonth_foc_value)
        oneMonth_co2            = str(oneMonth_co2_value)
        oneMonth_count          = str(oneMonth_count)
        year                    = str(dt_now.year)

        dataSet = {
            "imo"                    : imo,
            "year"                   : year,
            "cp"                     : cp,
            "cp_from"                : cp_from,
            "cp_to"                  : cp_to,
            "favorite"               : favorite,
            "Januarytonow_val"       : Januarytonow_val,
            "Januarytonow"           : Januarytonow,
            "Januarytonow_from"      : Januarytonow_from,
            "Januarytonow_to"        : Januarytonow_to,
            "Januarytonow_distance"  : Januarytonow_distance,
            "Januarytonow_foc"       : Januarytonow_foc,
            "Januarytonow_co2"       : Januarytonow_co2,
            "LastYear_val"           : LastYear_val,
            "LastYear"               : LastYear,
            "LastYear_from"          : LastYear_from,
            "LastYear_to"            : LastYear_to,
            "LastYear_distance"      : LastYear_distance,
            "LastYear_foc"           : LastYear_foc,
            "LastYear_co2"           : LastYear_co2,
            "LatestUpdate"           : LatestUpdate,
            "oneMonth_val"           : oneMonth_val,
            "oneMonth"               : oneMonth,
            "oneMonth_from"          : oneMonth_from,
            "oneMonth_to"            : oneMonth_to,
            "oneMonth_distance"      : oneMonth_distance,
            "oneMonth_foc"           : oneMonth_foc,
            "oneMonth_co2"           : oneMonth_co2,
            "oneMonth_count"         : oneMonth_count,
            "rf"                     : rf,
            "rf_from"                : rf_from,
            "rf_to"                  : rf_to,
            "VesselName"             : VesselName,
        }
        # print(f"dataSet[{type(dataSet)}]: {dataSet}")
        
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
    
