import json
import ast
import calendar
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta


from poseidon import dynamodb
from poseidon.Util.VesselMaster import VesselMaster
from poseidon.Util.VesselAlerm import VesselAlerm
from poseidon.Util.FuelOilType import FuelOilType
from poseidon.Util import Util


    
def calc_cii(imo, co2, distance, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER):
    # print(f"co2: {co2}, distance: {distance}, cii_ref: {cii_ref}, cii_rating: {cii_rating}, cii_reduction_rate: {cii_reduction_rate}, VESSELMASTER: {VESSELMASTER}")
    
    dwt = float(VESSELMASTER["Deadweight"])
    gt  = float(VESSELMASTER["Grosstongue"])
    
    
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
    CII_Reference_value = a_G2 * pow(dwt, (-1 * c_G2))                          # CII ref. （G2）
    CII_Reference       = CII_Reference_value * ((100 - reduction_rate) / 100)  # Required CII （G3, 2023）
    CII_Calculated      = CII_Attained / CII_Reference                          # Attained CII / Required CII
    
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
        
    # CII計算
    CII_Attained        = (co2 * pow(10, 6)) / (dwt * distance)                 # Attained CII(G1)
    CII_Reference_value = a_G2 * pow(dwt, (-1 * c_G2))                          # CII ref. （G2）
    CII_Reference       = CII_Reference_value * ((100 - reduction_rate) / 100)  # Required CII （G3, 2023）
    CII_Calculated      = CII_Attained / CII_Reference                          # Attained CII / Required CII

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
        
    
    print(f"less: {less}, less_value: {less_value}, less_more: {less_more}, more: {more}, more_value: {more_value}")
    print(f"less_a: {less_a}, less_c: {less_c}, less_more_a: {less_more_a}, less_more_c: {less_more_c}, more_a: {more_a}, more_c: {more_c}")
    print(f"dwt: {dwt}, gt: {gt}, weight: {weight}, distance: {distance}, co2: {co2}, a_G2: {a_G2}, c_G2: {c_G2}, reduction_rate: {reduction_rate}")
    print(f"CII_Calculated[{type(CII_Calculated)}]: {CII_Calculated}")
    print(f"CII_Score[{type(CII_Score)}]: {CII_Score}")
    print(f"CII_Rating[{type(CII_Rating)}]: {CII_Score}")
    
    return CII_Calculated, CII_Score, CII_Rating


def util_EmissionBoard_Unit(imo, Timestamp_from, Timestamp_to, response, Unit, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER, FUELOILTYPE, VESSELALERM):
    # print(f"response[{type(response)}]: {response}")
    # print(f"imo: {imo}, VesselName: {VESSELMASTER["VesselName"]}")
    
    RESPONSE = {}
    VALUE_LIST = []
    
    # DynamoDBから取得してきたレコードをリストに移管 ※利用項目精査必須
    emission_factor = float(FUELOILTYPE["emission_factor"])
    ballast = float(VESSELMASTER["Ballast"])
    laden = float(VESSELMASTER["Laden"])
    max_displacement = 0
    min_displacement = 9999999
    max_foc = 0
    for res in response:
        # local_date          = res["local_date"]["S"] if 'local_date' in res and res["local_date"]["S"] != "" else ""
        utc_date            = res["timestamp"]["S"] if 'timestamp' in res and res["timestamp"]["S"] != "" else ""
        # state               = res["state"]["S"] if 'state' in res and res["state"]["S"] != "" else ""
        # port_name           = res["port_name"]["S"] if 'port_name' in res and res["port_name"]["S"] != "" else ""
        # lat                 = float(res["lat"]["S"]) if 'lat' in res and res["lat"]["S"] != "" else ""
        # lng                 = float(res["lng"]["S"]) if 'lng' in res and res["lng"]["S"] != "" else ""
        # voyage_no           = res["voyage_no"]["S"] if 'voyage_no' in res and res["voyage_no"]["S"] != "" else ""
        # leg_no              = res["leg_no"]["S"] if 'leg_no' in res and res["leg_no"]["S"] != "" else ""
        # leg                 = res["leg"]["S"] if 'leg' in res and res["leg"]["S"] != "" else ""
        # co2_factor        = res["co2_factor"]["S"] if 'co2_factor' in res and res["co2_factor"]["S"] != "" else 2.7
        # course              = float(res["course"]["S"]) if 'course' in res and res["course"]["S"] != "" else ""
        beaufort            = float(res["beaufort"]["S"]) if 'beaufort' in res and res["beaufort"]["S"] != "" else ""
        # log_distance        = float(res["log_distance"]["S"]) if 'log_distance' in res and res["log_distance"]["S"] != "" else ""
        og_distance         = float(res["og_distance"]["S"]) if 'og_distance' in res and res["og_distance"]["S"] != "" else ""
        log_speed           = float(res["log_speed"]["S"]) if 'log_speed' in res and res["log_speed"]["S"] != "" else ""
        # og_speed            = float(res["og_speed"]["S"]) if 'og_speed' in res and res["og_speed"]["S"] != "" else ""
        me_rpm              = float(res["me_rpm"]["S"]) if 'me_rpm' in res and res["me_rpm"]["S"] != "" else ""
        # ge_foc              = float(res["ge_foc"]["S"]) if 'ge_foc' in res and res["ge_foc"]["S"] != "" else ""
        # boiler_foc          = float(res["boiler_foc"]["S"]) if 'boiler_foc' in res and res["boiler_foc"]["S"] != "" else ""
        me_foc              = float(res["me_foc"]["S"]) if 'me_foc' in res and res["me_foc"]["S"] != "" else ""
        # total_foc         = res["total_foc"]["S"] if 'total_foc' in res and res["total_foc"]["S"] != "" else ""
        me_load             = float(res["me_load"]["S"]) if 'me_load' in res and res["me_load"]["S"] != "" else ""
        # eta_local_date      = res["eta_local_date"]["S"] if 'eta_local_date' in res and res["eta_local_date"]["S"] != "" else ""
        # eta_utc_date        = res["eta_utc_date"]["S"] if 'eta_utc_date' in res and res["eta_utc_date"]["S"] != "" else ""
        # eta_destination     = res["eta_destination"]["S"] if 'eta_destination' in res and res["eta_destination"]["S"] != "" else ""
        # displacement        = float(res["displacement"]["S"] if res["displacement"]["S"] != "" else ""
        gt                  = float(res["gt"]["S"]) if 'gt' in res and res["gt"]["S"] != "" else ""
        dwt                 = float(res["dwt"]["S"]) if 'dwt' in res and res["dwt"]["S"] != "" else ""
        wind_speed          = float(res["wind_speed"]["S"]) if 'wind_speed' in res and res["wind_speed"]["S"] != "" else ""
        # wind_direction      = float(res["wind_direction"]["S"]) if 'wind_direction' in res and res["wind_direction"]["S"] != "" else ""
        # wave_period         = float(res["wave_period"]["S"]) if 'wave_period' in res and res["wave_period"]["S"] != "" else ""
        # wave_direction      = float(res["wave_direction"]["S"]) if 'wave_direction' in res and res["wave_direction"]["S"] != "" else ""
        # wave_height         = float(res["wave_height"]["S"]) if 'wave_height' in res and res["wave_height"]["S"] != "" else ""
        # swell_height        = float(res["swell_height"]["S"]) if 'swell_height' in res and res["swell_height"]["S"] != "" else ""
        # swell_period        = float(res["swell_period"]["S"]) if 'swell_period' in res and res["swell_period"]["S"] != "" else ""
        # swell_direction     = float(res["swell_direction"]["S"]) if 'swell_direction' in res and res["swell_direction"]["S"] != "" else ""
        # ablog_id            = res["ablog_id"]["S"] if 'ablog_id' in res and res["ablog_id"]["S"] != "" else ""
        
        
        
        # CO2,foc
        total_foc = ""
        co2 = ""
        if 'total_foc' in res and res["total_foc"]["S"] != "":
            total_foc = float(res["total_foc"]["S"])
            co2 = total_foc * emission_factor
        
        # displacement
        displacement = ""
        if 'displacement' in res and res["displacement"]["S"] != "":
            displacement = float(res["displacement"]["S"])
            max_displacement = displacement if max_displacement < displacement else max_displacement
            min_displacement = displacement if displacement < min_displacement else min_displacement
        
        VALUE = {}
        VALUE["timestamp"] = utc_date
        VALUE["foc"] = total_foc if total_foc != "" else None
        VALUE["wind_speed"] = wind_speed if wind_speed != "" else None
        VALUE["log_speed"] = log_speed if log_speed != "" else None
        VALUE["displacement"] = displacement if displacement != "" else None
        VALUE["beaufort"] = beaufort if beaufort != "" else None
        VALUE["co2"] = co2 if co2 != "" else None
        VALUE["distance"] = og_distance if og_distance != "" else None
        VALUE["me_foc"] = me_foc if me_foc != "" else None
        VALUE["me_rpm"] = me_rpm if me_rpm != "" else None
        VALUE["me_load"] = me_load if me_load != "" else None
        VALUE_LIST.append(VALUE)
        
    # データベースから抽出したレコード数をチェック
    print(f"Number of data included in VALUE_LIST: {len(VALUE_LIST)} counts, value: [{type(VALUE_LIST)}]: {VALUE_LIST}")
    
    
    print(f"[DEFAULT] Timestamp_from: {Timestamp_from}, Timestamp_to: {Timestamp_to}")
    
    # URLクエリにセットされたFromToタイムスタンプを取得
    Timestamp_from = Util.timestamp_calc_datetime(Timestamp_from)
    Timestamp_to = Util.timestamp_calc_datetime(Timestamp_to)
    print(f"Timestamp_from: type: {type(Timestamp_from)}, value: {Timestamp_from}")
    print(f"Timestamp_to: type: {type(Timestamp_to)}, value: {Timestamp_to}")
    
    
    Timestamp_from = Timestamp_from
    Timestamp_to = Timestamp_to
    
    Timestamp_from_All = Timestamp_from
    Timestamp_to_All = Timestamp_to
        
    unit_timestamp_list = []
    if Unit == "Monthly":
        # DataRange内の月を取得
        for index in range(120):
            if Timestamp_from < Timestamp_to:
                unit_timestamp_list.append(Timestamp_from)
                Timestamp_from = Timestamp_from + relativedelta(months=+1)
        
    elif Unit == "Weekly":
        for index in range(480):
            if Timestamp_from < Timestamp_to:
                unit_timestamp_list.append(Timestamp_from)
                Timestamp_from = Timestamp_from + timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=1)
        
        
    unit_timestamp_list.append(Timestamp_to)
    print(f"unit_timestamp_list: {unit_timestamp_list}")
    
    
    # print(f"集計処理 開始")
    # 集計処理 開始---------------------------------------------------------------------------
    
    # LOGSpeedRateUnit-----------------------------
    LOGSpeedRate_color = {
        "linearGradient": { "x1": 0, "x2": 0, "y1": 0, "y2": 1 },
        "stops": [
            [0, '#7E7E1A'],
            [1, '#B0B025']
        ]
    }
    LOGSpeedALL_one = 0
    LOGSpeedALL_two = 0
    LOGSpeedALL_three = 0
    LOGSpeedALL_four = 0
    LOGSpeedALL_five = 0
    LOGSpeedALL_six = 0
    LOGSpeedALL_seven = 0
    LOGSpeedALL_eight = 0
    LOGSpeedALL_nine = 0
    LOGSpeedALL_ten = 0
    LOGSpeedALL_eleven = 0
    LOGSpeedALL_twelve = 0
    LOGSpeedALL_thirteen = 0
    LOGSpeedALL_fourteen = 0
    LOGSpeedALL_fifteen = 0
    LOGSpeedALL_sixteen = 0
    LOGSpeedALL_seventeen = 0
    LOGSpeedALL_eighteen = 0
    LOGSpeedALL_nineteen = 0
    LOGSpeedALL_twenty = 0
    LOGSpeedALL_else = 0
    LOGSpeedALL_zero = 0
    
    # MELoadRateUnit-----------------------------
    MELoadRate_color = {
        "linearGradient": { "x1": 0, "x2": 0, "y1": 0, "y2": 1 },
        "stops": [
            [0, '#7e2da6'],
            [1, '#a641ce']
        ]
    }
    MELoadALL_one = 0
    MELoadALL_two = 0
    MELoadALL_three = 0
    MELoadALL_four = 0
    MELoadALL_five = 0
    MELoadALL_six = 0
    MELoadALL_seven = 0
    MELoadALL_eight = 0
    MELoadALL_nine = 0
    MELoadALL_ten = 0
    MELoadALL_eleven = 0
    MELoadALL_twelve = 0
    MELoadALL_thirteen = 0
    MELoadALL_fourteen = 0
    MELoadALL_fifteen = 0
    MELoadALL_sixteen = 0
    MELoadALL_seventeen = 0
    MELoadALL_eighteen = 0
    MELoadALL_nineteen = 0
    MELoadALL_twenty = 0
    MELoadALL_else = 0
    MELoadALL_zero = 0
    
    # DisplacementRateUnit-----------------------------
    Displacement_color_first = {
        "linearGradient": { "x1": 0, "y1": 0, "x2": 0, "y2": 1 },
        "stops": [
            [0, '#013A67'],
            [1, '#014880']
        ]
    }
    Displacement_color_second = {
        "linearGradient": { "x1": 0, "y1": 0, "x2": 0, "y2": 1 },
        "stops": [
            [0, '#3A74A3'],
            [1, '#32658D']
        ]
    }
    Displacement_Categories = []
    DisplacementALL_less_ballast = 0
    DisplacementALL_more_ballast_more_laden = 0
    DisplacementALL_zero = 0
    
    # BeaufortRateUnit-----------------------------
    BeaufortRate_color = {
        "linearGradient": { "x1": 0, "x2": 0, "y1": 0, "y2": 1 },
        "stops": [
            [0, '#1A7E6C'],
            [1, '#24B299']
        ]
    }
    BeaufortALL_one = 0
    BeaufortALL_two = 0
    BeaufortALL_three = 0
    BeaufortALL_four = 0
    BeaufortALL_five = 0
    BeaufortALL_six = 0
    BeaufortALL_seven = 0
    BeaufortALL_eight = 0
    BeaufortALL_nine = 0
    BeaufortALL_ten = 0
    BeaufortALL_eleven = 0
    BeaufortALL_twelve = 0
    BeaufortALL_thirteen = 0
    BeaufortALL_zero = 0

    # countAll
    countAll = 0
    countAll_wind_speed = 0
    countAll_me_load = 0
    countAll_me_rpm = 0
    countAll_log_speed = 0
    countAll_displacement = 0
    countAll_beaufort = 0
            
    # VoyageInformation
    CII_ScoreALL           = ""
    CII_valueALL           = 0
    Total_FOCALL           = 0
    ME_FOCALL              = 0
    Total_DistanceALL      = 0
    Total_CO2_EmissionsALL = 0
    Avg_Wind_SpeedALL      = 0
    Avg_LOG_SpeedALL       = 0
    Avg_LOADALL            = 0
    Avg_RPMALL             = 0
    Avg_DisplacementALL    = 0
    Avg_BeaufortALL        = 0
    
    # List
    FOCAll = []
    CIIAll = []
    CIIAllSCORE = []
    LOGSpeedRate = []
    LOGSpeedRate_Accumulation = []
    MELoadRate = []
    MELoadRate_Accumulation = []
    DisplacementRate = []
    DisplacementRate_Accumulation = []
    BeaufortRate = []
    BeaufortRate_Accumulation = []
    VoyageInformation = []
    
    # List: Aggregation Unit
    FOC = []
    CIIRATING = []
    CII = []
    CIISCORE = []
    LOGSpeedRateUnit = []
    LOGSpeedRateUnit_Accumulation = []
    MELoadRateUnit = []
    MELoadRateUnit_Accumulation = []
    DisplacementRateUnit = []
    DisplacementRateUnit_Accumulation = []
    BeaufortRateUnit = []
    BeaufortRateUnit_Accumulation = []
    VoyageInformationUnit = []
    
    
    # DataRangeに含まれる月数分ループ-----------------------------
    for index in range(len(unit_timestamp_list)):
        
        unit_timestamp_exist = True
        unit_timestamp_from = ""
        unit_timestamp_to = ""
        if index < len(unit_timestamp_list)-1:
            unit_timestamp_from = unit_timestamp_list[index]
            unit_timestamp_to   = unit_timestamp_list[index+1] + timedelta(seconds=0, milliseconds=0, microseconds=-1)
        elif index == len(unit_timestamp_list)-1:
            unit_timestamp_exist = False
        
        Timestamp_float = Util.timestamp_datetime_to_float(unit_timestamp_from)
        # print(f"unit_timestamp_from: {unit_timestamp_from}")
        # print(f"unit_timestamp_to: {unit_timestamp_to}")
        
        # 最終回は、開始日がunit_timestamp_toとイコールになるので処理不要。
        if unit_timestamp_exist == True:
            # 月別集計結果を初期化
            # LOGSpeedRateUnit-----------------------------
            LOGSpeed_one = 0
            LOGSpeed_two = 0
            LOGSpeed_three = 0
            LOGSpeed_four = 0
            LOGSpeed_five = 0
            LOGSpeed_six = 0
            LOGSpeed_seven = 0
            LOGSpeed_eight = 0
            LOGSpeed_nine = 0
            LOGSpeed_ten = 0
            LOGSpeed_eleven = 0
            LOGSpeed_twelve = 0
            LOGSpeed_thirteen = 0
            LOGSpeed_fourteen = 0
            LOGSpeed_fifteen = 0
            LOGSpeed_sixteen = 0
            LOGSpeed_seventeen = 0
            LOGSpeed_eighteen = 0
            LOGSpeed_nineteen = 0
            LOGSpeed_twenty = 0
            LOGSpeed_else = 0
            LOGSpeed_zero = 0
            
            # MELoadRateUnit-----------------------------
            MELoad_one = 0
            MELoad_two = 0
            MELoad_three = 0
            MELoad_four = 0
            MELoad_five = 0
            MELoad_six = 0
            MELoad_seven = 0
            MELoad_eight = 0
            MELoad_nine = 0
            MELoad_ten = 0
            MELoad_eleven = 0
            MELoad_twelve = 0
            MELoad_thirteen = 0
            MELoad_fourteen = 0
            MELoad_fifteen = 0
            MELoad_sixteen = 0
            MELoad_seventeen = 0
            MELoad_eighteen = 0
            MELoad_nineteen = 0
            MELoad_twenty = 0
            MELoad_else = 0
            MELoad_zero = 0

            #  DisplacementRateUnit-----------------------------
            Displacement_less_ballast = 0
            Displacement_more_ballast_more_laden = 0
            Displacement_zero = 0
            
            # BeaufortRateUnit-----------------------------
            Beaufort_one = 0
            Beaufort_two = 0
            Beaufort_three = 0
            Beaufort_four = 0
            Beaufort_five = 0
            Beaufort_six = 0
            Beaufort_seven = 0
            Beaufort_eight = 0
            Beaufort_nine = 0
            Beaufort_ten = 0
            Beaufort_eleven = 0
            Beaufort_twelve = 0
            Beaufort_thirteen = 0
            Beaufort_zero = 0
            
            # VoyageInformationUnit-----------------------------
            CII_Score               = ""
            CII_value               = 0
            Total_FOC               = 0 
            ME_FOC                  = 0
            Total_Distance          = 0 
            Total_CO2_Emissions     = 0 
            Avg_Wind_Speed          = 0
            Avg_LOG_Speed           = 0
            Avg_LOAD                = 0
            Avg_RPM                 = 0
            Avg_Displacement        = 0
            Avg_Beaufort            = 0
            count = 0
            count_wind_speed = 0
            count_me_load = 0
            count_me_rpm = 0
            count_log_speed = 0
            count_displacement = 0
            count_beaufort = 0
            
            
            
            for VALUE in VALUE_LIST:
                
                Timestamp = Util.timestamp_calc_datetime(VALUE["timestamp"])
                
                # 月別集計
                if unit_timestamp_from <= Timestamp and Timestamp < unit_timestamp_to:
                    
                    count += 1
                    countAll += 1
                    # count_Accumulation += 1
                    
                    # wind_speed-----------------------------
                    if VALUE["wind_speed"] != None:
                        count_wind_speed += 1
                        countAll_wind_speed += 1
                    
                    # me_load-----------------------------
                    if VALUE["me_load"] != None:
                        count_me_load += 1
                        countAll_me_load += 1
                        # count_me_load_Accumulation += 1
                        me_load_value = VALUE['me_load']
                        if me_load_value <=  5:
                            MELoad_one += 1
                            MELoadALL_one += 1
                        elif 5 < me_load_value and me_load_value <=  10:
                            MELoad_two += 1
                            MELoadALL_two += 1
                        elif 10 <= me_load_value and me_load_value <  15:
                            MELoad_three += 1
                            MELoadALL_three += 1
                        elif 15 <= me_load_value and me_load_value <  20:
                            MELoad_four += 1
                            MELoadALL_four += 1
                        elif 20 <= me_load_value and me_load_value <  25:
                            MELoad_five += 1
                            MELoadALL_five += 1
                        elif 25 <= me_load_value and me_load_value <  30:
                            MELoad_six += 1
                            MELoadALL_six += 1
                        elif 30 <= me_load_value and me_load_value <  35:
                            MELoad_seven += 1
                            MELoadALL_seven += 1
                        elif 35 <= me_load_value and me_load_value <  40:
                            MELoad_eight += 1
                            MELoadALL_eight += 1
                        elif 40 <= me_load_value and me_load_value <  45:
                            MELoad_nine += 1
                            MELoadALL_nine += 1
                        elif 45 <= me_load_value and me_load_value <  50:
                            MELoad_ten += 1
                            MELoadALL_ten += 1
                        elif 50 <= me_load_value and me_load_value <  55:
                            MELoad_eleven += 1
                            MELoadALL_eleven += 1
                        elif 55 <= me_load_value and me_load_value <  60:
                            MELoad_twelve += 1
                            MELoadALL_twelve += 1
                        elif 60 <= me_load_value and me_load_value <  65:
                            MELoad_thirteen += 1
                            MELoadALL_thirteen += 1
                        elif 65 <= me_load_value and me_load_value <  70:
                            MELoad_fourteen += 1
                            MELoadALL_fourteen += 1
                        elif 70 <= me_load_value and me_load_value <  75:
                            MELoad_fifteen += 1
                            MELoadALL_fifteen += 1
                        elif 75 <= me_load_value and me_load_value <  80:
                            MELoad_sixteen += 1
                            MELoadALL_sixteen += 1
                        elif 80 <= me_load_value and me_load_value <  85:
                            MELoad_seventeen += 1
                            MELoadALL_seventeen += 1
                        elif 85 <= me_load_value and me_load_value <  90:
                            MELoad_eighteen += 1
                            MELoadALL_eighteen += 1
                        elif 90 <= me_load_value and me_load_value <  95:
                            MELoad_nineteen += 1
                            MELoadALL_nineteen += 1
                        elif 95 <= me_load_value and me_load_value <  100:
                            MELoad_twenty += 1
                            MELoadALL_twenty += 1
                        elif 100 <= me_load_value:
                            MELoad_else += 1
                            MELoadALL_else += 1
                    else:
                        MELoad_zero += 1
                        MELoadALL_zero += 1

                    # me_rpm-----------------------------
                    if VALUE["me_rpm"] != None:
                        count_me_rpm += 1
                        countAll_me_rpm += 1
                        # count_me_rpm_Accumulation += 1
                    
                    # LOGSpeedRateUnit-----------------------------
                    if VALUE['log_speed'] != None:
                        count_log_speed += 1
                        countAll_log_speed += 1
                        # count_log_speed_Accumulation += 1
                        log_speed_value = VALUE['log_speed']
                        if log_speed_value <=  1:
                            LOGSpeed_one += 1
                            LOGSpeedALL_one += 1
                        elif 1 < log_speed_value and log_speed_value <=  2:
                            LOGSpeed_two += 1
                            LOGSpeedALL_two += 1
                        elif 2 <= log_speed_value and log_speed_value <  3:
                            LOGSpeed_three += 1
                            LOGSpeedALL_three += 1
                        elif 3 <= log_speed_value and log_speed_value <  4:
                            LOGSpeed_four += 1
                            LOGSpeedALL_four += 1
                        elif 4 <= log_speed_value and log_speed_value <  5:
                            LOGSpeed_five += 1
                            LOGSpeedALL_five += 1
                        elif 5 <= log_speed_value and log_speed_value <  6:
                            LOGSpeed_six += 1
                            LOGSpeedALL_six += 1
                        elif 6 <= log_speed_value and log_speed_value <  7:
                            LOGSpeed_seven += 1
                            LOGSpeedALL_seven += 1
                        elif 7 <= log_speed_value and log_speed_value <  8:
                            LOGSpeed_eight += 1
                            LOGSpeedALL_eight += 1
                        elif 8 <= log_speed_value and log_speed_value <  9:
                            LOGSpeed_nine += 1
                            LOGSpeedALL_nine += 1
                        elif 9 <= log_speed_value and log_speed_value <  10:
                            LOGSpeed_ten += 1
                            LOGSpeedALL_ten += 1
                        elif 10 <= log_speed_value and log_speed_value <  11:
                            LOGSpeed_eleven += 1
                            LOGSpeedALL_eleven += 1
                        elif 11 <= log_speed_value and log_speed_value <  12:
                            LOGSpeed_twelve += 1
                            LOGSpeedALL_twelve += 1
                        elif 12 <= log_speed_value and log_speed_value <  13:
                            LOGSpeed_thirteen += 1
                            LOGSpeedALL_thirteen += 1
                        elif 13 <= log_speed_value and log_speed_value <  14:
                            LOGSpeed_fourteen += 1
                            LOGSpeedALL_fourteen += 1
                        elif 14 <= log_speed_value and log_speed_value <  15:
                            LOGSpeed_fifteen += 1
                            LOGSpeedALL_fifteen += 1
                        elif 15 <= log_speed_value and log_speed_value <  16:
                            LOGSpeed_sixteen += 1
                            LOGSpeedALL_sixteen += 1
                        elif 16 <= log_speed_value and log_speed_value <  17:
                            LOGSpeed_seventeen += 1
                            LOGSpeedALL_seventeen += 1
                        elif 17 <= log_speed_value and log_speed_value <  18:
                            LOGSpeed_eighteen += 1
                            LOGSpeedALL_eighteen += 1
                        elif 18 <= log_speed_value and log_speed_value <  19:
                            LOGSpeed_nineteen += 1
                            LOGSpeedALL_nineteen += 1
                        elif 19 <= log_speed_value and log_speed_value <  20:
                            LOGSpeed_twenty += 1
                            LOGSpeedALL_twenty += 1
                        elif 20 <= log_speed_value:
                            LOGSpeed_else += 1
                            LOGSpeedALL_else += 1
                    else:
                        LOGSpeed_zero += 1
                        LOGSpeedALL_zero += 1
                    
                    # DisplacementRateUnit-----------------------------
                    Displacement_Categories = [str(ballast), str(laden)]
                    if VALUE['displacement'] != None:
                        count_displacement += 1
                        countAll_displacement += 1
                        # count_displacement_Accumulation += 1
                        displacement_value = VALUE['displacement']
                        if displacement_value <= ballast:
                            Displacement_less_ballast += 1
                            DisplacementALL_less_ballast += 1
                        elif ballast < displacement_value and displacement_value <= laden:
                            Displacement_more_ballast_more_laden += 1
                            DisplacementALL_more_ballast_more_laden += 1
                    else:
                        Displacement_zero += 1
                        DisplacementALL_zero += 1
                        
                    # BeaufortRateUnit-----------------------------
                    if VALUE['beaufort'] != None:
                        count_beaufort += 1
                        countAll_beaufort += 1
                        # count_beaufort_Accumulation += 1
                        beaufort_value = int(VALUE['beaufort'])
                        if beaufort_value <=  0:
                            BeaufortALL_one += 1
                            Beaufort_one += 1
                        elif 0 < beaufort_value and beaufort_value <=  1:
                            Beaufort_two += 1
                            BeaufortALL_two += 1
                        elif 1 < beaufort_value and beaufort_value <=  2:
                            Beaufort_three += 1
                            BeaufortALL_three += 1
                        elif 2 < beaufort_value and beaufort_value <=  3:
                            Beaufort_four += 1
                            BeaufortALL_four += 1
                        elif 3 < beaufort_value and beaufort_value <=  4:
                            Beaufort_five += 1
                            BeaufortALL_five += 1
                        elif 4 < beaufort_value and beaufort_value <=  5:
                            Beaufort_six += 1
                            BeaufortALL_six += 1
                        elif 5 < beaufort_value and beaufort_value <=  6:
                            Beaufort_seven += 1
                            BeaufortALL_seven += 1
                        elif 6 < beaufort_value and beaufort_value <=  7:
                            Beaufort_eight += 1
                            BeaufortALL_eight += 1
                        elif 7 < beaufort_value and beaufort_value <=  8:
                            Beaufort_nine += 1
                            BeaufortALL_nine += 1
                        elif 8 < beaufort_value and beaufort_value <=  9:
                            Beaufort_ten += 1
                            BeaufortALL_ten += 1
                        elif 9 < beaufort_value and beaufort_value <=  10:
                            Beaufort_eleven += 1
                            BeaufortALL_eleven += 1
                        elif 10 < beaufort_value and beaufort_value <=  11:
                            Beaufort_twelve += 1
                            BeaufortALL_twelve += 1
                        elif 11 < beaufort_value and beaufort_value <=  12:
                            Beaufort_thirteen += 1
                            BeaufortALL_thirteen += 1
                            
                    else:
                        Beaufort_zero += 1
                        BeaufortALL_zero += 1
                        
                    # FOC:最大値取得　※グラフ上限設定用-----------------------------
                    # if VALUE['foc'] != None:
                    #     max_foc = Total_FOC if max_foc < Total_FOC else max_foc
                    
                    # VoyageInfomartion-----------------------------
                    # 集計単位別
                    Total_FOC           += VALUE['foc'] if VALUE['foc'] != None else 0
                    ME_FOC              += VALUE['me_foc'] if VALUE['me_foc'] != None else 0
                    Total_Distance      += VALUE['distance'] if VALUE['distance'] != None else 0
                    Total_CO2_Emissions += VALUE['co2'] if VALUE['co2'] != None else 0
                    Avg_Wind_Speed      += VALUE['wind_speed'] if VALUE['wind_speed'] != None else 0
                    Avg_LOG_Speed       += VALUE['log_speed'] if VALUE['log_speed'] != None else 0
                    Avg_LOAD            += VALUE['me_load'] if VALUE['me_load'] != None else 0
                    Avg_RPM             += VALUE['me_rpm'] if VALUE['me_rpm'] != None else 0
                    Avg_Displacement    += VALUE['displacement'] if VALUE['displacement'] != None else 0
                    Avg_Beaufort        += VALUE['beaufort'] if VALUE['beaufort'] != None else 0
                    
                    # 全期間
                    Total_FOCALL           += VALUE['foc'] if VALUE['foc'] != None else 0
                    ME_FOCALL              += VALUE['me_foc'] if VALUE['me_foc'] != None else 0
                    Total_DistanceALL      += VALUE['distance'] if VALUE['distance'] != None else 0
                    Total_CO2_EmissionsALL += VALUE['co2'] if VALUE['co2'] != None else 0
                    Avg_Wind_SpeedALL      += VALUE['wind_speed'] if VALUE['wind_speed'] != None else 0
                    Avg_LOG_SpeedALL       += VALUE['log_speed'] if VALUE['log_speed'] != None else 0
                    Avg_LOADALL            += VALUE['me_load'] if VALUE['me_load'] != None else 0
                    Avg_RPMALL             += VALUE['me_rpm'] if VALUE['me_rpm'] != None else 0
                    Avg_DisplacementALL    += VALUE['displacement'] if VALUE['displacement'] != None else 0
                    Avg_BeaufortALL        += VALUE['beaufort'] if VALUE['beaufort'] != None else 0
                    
                
            
            # print(f"Number of non-blank data within the same month：{count} counts")
        
            # 集計単位別---------------------------------------------------------------------------------------
            LOGSpeedUnit = []
            MELoadUnit =[]
            DisplacementUnit = []
            BeaufortUnit = []
            
            # LOGSpeedRateUnit-----------------------------
            if count_log_speed != 0:
                LOGSpeedUnit = [
                    {"y": LOGSpeed_one / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_two / count_log_speed * 1000, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_three / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_four / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_five / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_six / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_seven / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_eight / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_nine / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_ten / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_eleven / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_twelve / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_thirteen / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_fourteen / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_fifteen / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_sixteen / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_seventeen / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_eighteen / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_nineteen / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_twenty / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeed_else / count_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": 0},
                    {"y": 0},
                    # {"y": LOGSpeed_zero / count_log_speed * 100, "color": '#FE349A'},
                ]
            LOGSpeedRateUnit.append(LOGSpeedUnit)
            
            accumulation = 0
            LOGSpeedUnit_accumulation = []
            for ls in range(0,len(LOGSpeedUnit)-2):
                accumulation += LOGSpeedUnit[ls]["y"]
                LOGSpeedUnit_accumulation.append(accumulation)
            LOGSpeedRateUnit_Accumulation.append(LOGSpeedUnit_accumulation)
                
            # MELoadRateUnit-----------------------------
            if count_me_load != 0:
                MELoadUnit = [
                    {"y": MELoad_one / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_two / count_me_load * 1000, "color": MELoadRate_color},
                    {"y": MELoad_three / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_four / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_five / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_six / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_seven / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_eight / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_nine / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_ten / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_eleven / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_twelve / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_thirteen / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_fourteen / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_fifteen / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_sixteen / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_seventeen / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_eighteen / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_nineteen / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_twenty / count_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoad_else / count_me_load * 100, "color": MELoadRate_color},
                    {"y": 0},
                    {"y": 0},
                    # {"y": MELoad_zero / count_me_load * 100, "color": '#FE349A'},
                ]
            MELoadRateUnit.append(MELoadUnit)
            
            accumulation = 0
            MELoadUnit_accumulation = []
            for ls in range(0,len(MELoadUnit)-2):
                accumulation += MELoadUnit[ls]["y"]
                MELoadUnit_accumulation.append(accumulation)
            MELoadRateUnit_Accumulation.append(MELoadUnit_accumulation)

            # DisplacementRateUnit-----------------------------
            if count_displacement != 0:
                DisplacementUnit = [
                    {"y": round(Displacement_less_ballast / count_displacement * 100,1), "x":ballast, "color": Displacement_color_first}, 
                    {"y": round(Displacement_more_ballast_more_laden / count_displacement * 100,1), "x":laden, "color": Displacement_color_second}, 
                    # {"y": Displacement_zero / count_displacement * 100, "color": 'green'},
                ]
            DisplacementRateUnit.append(DisplacementUnit)
            
            accumulation = 0
            DisplacementUnit_accumulation = []
            for ls in DisplacementUnit:
                accumulation += ls["y"]
                DisplacementUnit_accumulation.append(accumulation)
            DisplacementRateUnit_Accumulation.append(DisplacementUnit_accumulation)
                
            # BeaufortRateUnit-----------------------------
            if count_beaufort != 0:
                BeaufortUnit = [
                    {"y": Beaufort_one / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_two / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_three / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_four / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_five / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_six / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_seven / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_eight / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_nine / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_ten / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_eleven / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_twelve / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": Beaufort_thirteen / count_beaufort * 100, "color": BeaufortRate_color},
                    {"y": 0},
                    {"y": 0},
                    # {"y": Beaufort_zero / count_beaufort * 100, "color": '#E6B422'},
                ]
            BeaufortRateUnit.append(BeaufortUnit)
            
            accumulation = 0
            BeaufortUnit_accumulation = []
            for ls in range(0,len(BeaufortUnit)-2):
                accumulation += BeaufortUnit[ls]["y"]
                BeaufortUnit_accumulation.append(accumulation)
            BeaufortRateUnit_Accumulation.append(BeaufortUnit_accumulation)
            
            # FOC-----------------------------
            FOC.append([Timestamp_float, Total_FOC])
            
            # CII-----------------------------
            if Total_Distance != 0:
                CII_value, CII_Score, cii_Rating = calc_cii(imo, Total_CO2_Emissions, Total_Distance, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER)
            # CII.append([Timestamp_float, CII_value])
            CII.append([Timestamp_float, CII_value])

            
            # CIISCORE-----------------------------
            CIISCORE.append([Timestamp_float, CII_Score])
            
            # VoyageInformationUnit-----------------------------
            From                    = unit_timestamp_from.strftime('%Y/%m/%d')
            To                      = unit_timestamp_to.strftime('%Y/%m/%d')
            CII_Score               = CII_Score
            CII_value               = CII_value
            Total_FOC               = Total_FOC
            ME_FOC                  = ME_FOC
            Total_Distance          = Total_Distance
            Total_CO2_Emissions     = Total_CO2_Emissions
            Avg_Wind_Speed          = round(Avg_Wind_Speed / count_wind_speed, 1) if count_wind_speed != 0 else 0
            Avg_LOG_Speed           = round(Avg_LOG_Speed / count_log_speed, 1) if count_log_speed != 0 else 0
            Avg_LOAD                = round(Avg_LOAD / count_me_load, 1) if count_me_load != 0 else 0
            Avg_RPM                 = round(Avg_RPM / count_me_rpm, 1) if count_me_rpm != 0 else 0
            Avg_Beaufort            = round(Avg_Beaufort / count_beaufort, 1) if count_beaufort != 0 else 0
            Avg_Displacement        = round(Avg_Displacement / count_displacement, 1) if count_displacement != 0 else 0
            
            max_foc = Total_FOC if max_foc < Total_FOC else max_foc
               
            VoyageInfo = {
                "From"                  : From,
                "To"                    : To,
                "CII_Score"             : CII_Score,
                "CII_value"             : round(CII_value, 2),
                "Total_FOC"             : round(Total_FOC, 0),
                "ME_FOC"                : round(ME_FOC, 0),
                "Total_Distance"        : round(Total_Distance, 0),
                "Total_CO2_Emissions"   : round(Total_CO2_Emissions, 0),
                "Avg_Wind_Speed"        : Avg_Wind_Speed,
                "Avg_LOG_Speed"         : Avg_LOG_Speed,
                "Avg_LOAD"              : Avg_LOAD,
                "Avg_RPM"               : Avg_RPM,
                "Avg_Displacement"      : Avg_Displacement,
                "Avg_Beaufort"          : Avg_Beaufort,
            }
            VoyageInformationUnit.append(VoyageInfo)
            # print(f"VoyageInfo: {VoyageInfo}")
            
            
            # 累計---------------------------------------------------------------------------------------
            LOGSpeedAll = []
            MELoadAll = []
            DisplacementAll = []
            BeaufortAll = []
            
            if countAll_log_speed != 0:
                # LOGSpeedRate-----------------------------
                LOGSpeedAll = [
                    {"y": LOGSpeedALL_one / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_two / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_three / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_four / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_five / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_six / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_seven / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_eight / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_nine / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_ten / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_eleven / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_twelve / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_thirteen / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_fourteen / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_fifteen / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_sixteen / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_seventeen / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_eighteen / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_nineteen / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_twenty / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": LOGSpeedALL_else / countAll_log_speed * 100, "color": LOGSpeedRate_color},
                    {"y": 0},
                    {"y": 0},
                    # {"y": LOGSpeedALL_zero / countAll_log_speed * 100, "color": '#FE349A'},
                ]
                LOGSpeedRate.append(LOGSpeedAll)
                
                
                # LOGSpeedRate_Accumulation-----------------------------
                accumulation = 0
                LOGSpeedAll_Accumulation = []
                for ls in range(0,len(LOGSpeedAll)-2):
                    accumulation += LOGSpeedAll[ls]["y"]
                    LOGSpeedAll_Accumulation.append(accumulation)
                LOGSpeedRate_Accumulation.append(LOGSpeedAll_Accumulation)

            if countAll_me_load != 0:
                # MELoadRate-----------------------------
                MELoadAll = [
                    {"y": MELoadALL_one / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_two / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_three / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_four / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_five / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_six / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_seven / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_eight / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_nine / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_ten / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_eleven / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_twelve / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_thirteen / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_fourteen / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_fifteen / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_sixteen / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_seventeen / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_eighteen / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_nineteen / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_twenty / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": MELoadALL_else / countAll_me_load * 100, "color": MELoadRate_color},
                    {"y": 0},
                    {"y": 0},
                    # {"y": MELoadALL_zero / countAll_me_load * 100, "color": '#FE349A'},
                ]
                MELoadRate.append(MELoadAll)
                
                
                # MELoadRate_Accumulation-----------------------------
                accumulation = 0
                MELoadAll_Accumulation = []
                for ls in range(0,len(MELoadAll)-2):
                    accumulation += MELoadAll[ls]["y"]
                    MELoadAll_Accumulation.append(accumulation)
                MELoadRate_Accumulation.append(MELoadAll_Accumulation)

            if countAll_displacement != 0:
                # DisplacementRate-----------------------------
                DisplacementAll = [
                    {"y": round(DisplacementALL_less_ballast / countAll_displacement * 100,1), "x":ballast, "color": Displacement_color_first},
                    {"y": round(DisplacementALL_more_ballast_more_laden / countAll_displacement * 100,1), "x":laden, "color": Displacement_color_second},
                    # {"y": DisplacementALL_zero / countAll_displacement * 100, "color": 'green'},
                ]
                DisplacementRate.append(DisplacementAll)
                
                # DisplacementRate_Accumulation-----------------------------
                # accumulation = 0
                # DisplacementAll_Accumulation = []
                # for ls in LOGSpeedAll:
                #     accumulation += ls["y"]
                #     DisplacementAll_Accumulation.append(accumulation)
                # DisplacementRate_Accumulation.append(DisplacementAll_Accumulation)
                
            if countAll_beaufort != 0:
                # BeaufortRate-----------------------------
                BeaufortAll = [
                    {"y": BeaufortALL_one / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_two / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_three / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_four / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_five / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_six / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_seven / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_eight / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_nine / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_ten / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_eleven / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_twelve / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": BeaufortALL_thirteen / countAll_beaufort * 100, "color": BeaufortRate_color},
                    {"y": 0},
                    {"y": 0},
                    # {"y": BeaufortALL_zero / countAll_beaufort * 100, "color": '#E6B422'}, 
                ]
                BeaufortRate.append(BeaufortAll)
                
                # BeaufortRate_Accumulation-----------------------------
                accumulation = 0
                BeaufortAll_Accumulation = []
                for ls in range(0,len(BeaufortAll)-2):
                    accumulation += BeaufortAll[ls]["y"]
                    BeaufortAll_Accumulation.append(accumulation)
                BeaufortRate_Accumulation.append(BeaufortAll_Accumulation)
            
            # FOC-----------------------------
            FOCAll.append([Timestamp_float, Total_FOCALL])
            
            # CII-----------------------------
            if Total_DistanceALL != 0:
                CII_valueALL, CII_ScoreALL, cii_RatingAll = calc_cii(imo, Total_CO2_EmissionsALL, Total_DistanceALL, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER)
                print(f"Total_CO2_EmissionsALL: {Total_CO2_EmissionsALL}, Total_DistanceALL: {Total_DistanceALL}, CII_valueALL :{CII_valueALL}, CII_ScoreALL :{CII_ScoreALL}")
            CIIAll.append([Timestamp_float, CII_valueALL])
            
            # CIISCORE-----------------------------
            CIIAllSCORE.append([Timestamp_float, CII_ScoreALL])
            
            # VoyageInformation-----------------------------
            FromAll                = Timestamp_from_All.strftime('%Y/%m/%d')
            ToAll                  = unit_timestamp_to.strftime('%Y/%m/%d')
            CII_ScoreALL           = CII_ScoreALL
            CII_valueALL           = CII_valueALL
            Total_FOCAll           = Total_FOCALL
            ME_FOCAll              = ME_FOCALL
            Total_DistanceAll      = Total_DistanceALL
            Total_CO2_EmissionsAll = Total_CO2_EmissionsALL
            Avg_Wind_SpeedAll      = round(Avg_Wind_SpeedALL / countAll_wind_speed, 1) if countAll_wind_speed != 0 else 0
            Avg_LOG_SpeedAll       = round(Avg_LOG_SpeedALL / countAll_log_speed, 1) if countAll_log_speed != 0 else 0
            Avg_LOADAll            = round(Avg_LOADALL / countAll_me_load, 1) if countAll_me_load != 0 else 0
            Avg_RPMAll             = round(Avg_RPMALL / countAll_me_rpm, 1) if countAll_me_rpm != 0 else 0
            Avg_BeaufortAll        = round(Avg_BeaufortALL / countAll_beaufort, 1) if countAll_beaufort != 0 else 0
            Avg_DisplacementAll    = round(Avg_DisplacementALL / countAll_displacement, 1) if countAll_displacement != 0 else 0
            
            
            VoyageInfoAll = {
                "From"                      : FromAll,
                "To"                        : ToAll,
                "CII_Score"                 : CII_ScoreALL,
                "CII_value"                 : round(CII_valueALL, 2),
                "Total_FOC"                 : round(Total_FOCAll, 0),
                "ME_FOC"                    : round(ME_FOCAll, 0),
                "Total_Distance"            : round(Total_DistanceAll, 0),
                "Total_CO2_Emissions"       : round(Total_CO2_EmissionsAll, 0),
                "Avg_Wind_Speed"            : Avg_Wind_SpeedAll,
                "Avg_LOG_Speed"             : Avg_LOG_SpeedAll,
                "Avg_LOAD"                  : Avg_LOADAll,
                "Avg_RPM"                   : Avg_RPMAll,
                "Avg_Displacement"          : Avg_DisplacementAll,
                "Avg_Beaufort"              : Avg_BeaufortAll,
            }
            VoyageInformation.append(VoyageInfoAll)
            # print(f"VoyageInfoAll: {VoyageInfoAll}")
            
    
    print(f"LOGSpeedRate: {LOGSpeedRate}")
    print(f"LOGSpeedRateUnit: {LOGSpeedRateUnit}")
    print(f"LOGSpeedRate_Accumulation: {LOGSpeedRate_Accumulation}")
    print(f"LOGSpeedRateUnit_Accumulation: {LOGSpeedRateUnit_Accumulation}")

    print(f"MELoadRate: {MELoadRate}")
    print(f"MELoadRateUnit: {MELoadRateUnit}")
    print(f"MELoadRate_Accumulation: {MELoadRate_Accumulation}")
    print(f"MELoadRateUnit_Accumulation: {MELoadRateUnit_Accumulation}")
    
    print(f"Displacement_Categories: {Displacement_Categories}")
    print(f"DisplacementRate: {DisplacementRate}")
    print(f"DisplacementRateUnit: {DisplacementRateUnit}")
    print(f"DisplacementRate_Accumulation: {DisplacementRate_Accumulation}")
    print(f"DisplacementRateUnit_Accumulation: {DisplacementRateUnit_Accumulation}")
    
    print(f"BeaufortRate: {BeaufortRate}")
    print(f"BeaufortRateUnit: {BeaufortRateUnit}")
    print(f"BeaufortRate_Accumulation: {BeaufortRate_Accumulation}")
    print(f"BeaufortRateUnit_Accumulation: {BeaufortRateUnit_Accumulation}")
    
    print(f"FOC: {FOC}")
    print(f"FOCAll: {FOC}")
    
    print(f"CII_Rating: {cii_RatingAll}")
    print(f"CIILASTYEAR: {VESSELALERM["LastYear_val"]}")
    print(f"CII: {CII}")
    print(f"CIISCORE: {CIISCORE}")
    print(f"CIIAll: {CIIAll}")
    print(f"CIIAllSCORE: {CIIAllSCORE}")
    
    print(f"VoyageInformation: {VoyageInformation}")
    print(f"VoyageInformationUnit: {VoyageInformationUnit}")
    
    
    RESPONSE["LOGSpeedRate"] = LOGSpeedRate
    RESPONSE["LOGSpeedRateUnit"] = LOGSpeedRateUnit
    RESPONSE["LOGSpeedRate_Accumulation"] = LOGSpeedRate_Accumulation
    RESPONSE["LOGSpeedRateUnit_Accumulation"] = LOGSpeedRateUnit_Accumulation
    RESPONSE["MELoadRate"] = MELoadRate
    RESPONSE["MELoadRateUnit"] = MELoadRateUnit
    RESPONSE["MELoadRate_Accumulation"] = MELoadRate_Accumulation
    RESPONSE["MELoadRateUnit_Accumulation"] = MELoadRateUnit_Accumulation
    RESPONSE["Displacement_Categories"] = Displacement_Categories
    RESPONSE["DisplacementRate"] = DisplacementRate
    RESPONSE["DisplacementRateUnit"] = DisplacementRateUnit
    RESPONSE["DisplacementRate_Accumulation"] = DisplacementRate_Accumulation
    RESPONSE["DisplacementRateUnit_Accumulation"] = DisplacementRateUnit_Accumulation
    RESPONSE["BeaufortRate"] = BeaufortRate
    RESPONSE["BeaufortRateUnit"] = BeaufortRateUnit
    RESPONSE["BeaufortRate_Accumulation"] = BeaufortRate_Accumulation
    RESPONSE["BeaufortRateUnit_Accumulation"] = BeaufortRateUnit_Accumulation
    RESPONSE["FOC"] = FOC
    RESPONSE["FOCAll"] = FOCAll 
    # RESPONSE["FOC_YAXIS"] = {"max": round(max_foc + (max_foc / 2), 0) , "tickInterval":round(max_foc / 5, 0) }
    RESPONSE["FOC_YAXIS"] = {"max": round(max_foc, 0) , "tickInterval":round(max_foc / 5, 0) }
    RESPONSE["CIIRATING"] = cii_RatingAll
    RESPONSE["CIILASTYEAR"] = float(VESSELALERM["LastYear_val"])
    RESPONSE["CII"] = CII
    RESPONSE["CIISCORE"] = CIISCORE
    RESPONSE["CIIAll"] = CIIAll
    RESPONSE["CIIAllSCORE"] = CIIAllSCORE
    RESPONSE["VoyageInformation"] = VoyageInformation
    RESPONSE["VoyageInformationUnit"] = VoyageInformationUnit
        
    return RESPONSE
            

    
def util_EmissionBoard_main(imo, Timestamp_from, Timestamp_to, response, Unit):
    
    Timestamp_to_year = str(Util.timestamp_calc_datetime(Timestamp_to).year)

    dt_now = datetime.now()
    year = str(dt_now.year)
    
    VESSELMASTER = VesselMaster.VesselMaster(imo)
    FUELOILTYPE = FuelOilType.FuelOilType(VESSELMASTER["OilType"])
    VESSELALERM = VesselAlerm.VesselAlerm(imo, year)
    cii_ref = dynamodb.get_cii_ref(VESSELMASTER["VesselType"])
    cii_rating = dynamodb.get_cii_rating(VESSELMASTER["VesselType"])
    cii_reduction_rate = dynamodb.get_cii_reduction_rate(Timestamp_to_year)
    print(f"VESSELMASTER: {VESSELMASTER}, FUELOILTYPE: {FUELOILTYPE}, VESSELALERM: {VESSELALERM}, cii_ref: {cii_ref}, cii_rating: {cii_rating}, cii_reduction_rate: {cii_reduction_rate}")
    
    EmissionBoard = {}
    if len(response):
        # 集計単位のタイプ毎に、必要項目の集計結果を取得
        RESPONSE = util_EmissionBoard_Unit(imo, Timestamp_from, Timestamp_to, response, Unit, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER, FUELOILTYPE, VESSELALERM)
        
        # 返却値セット
        EmissionBoard = {
            "VESSELMASTER"                      : VESSELMASTER,
            "LOGSpeedRate"                      : RESPONSE["LOGSpeedRate"],
            "LOGSpeedRateUnit"                  : RESPONSE["LOGSpeedRateUnit"],
            "LOGSpeedRate_Accumulation"         : RESPONSE["LOGSpeedRate_Accumulation"],
            "LOGSpeedRateUnit_Accumulation"     : RESPONSE["LOGSpeedRateUnit_Accumulation"],
            "MELoadRate"                        : RESPONSE["MELoadRate"],
            "MELoadRateUnit"                    : RESPONSE["MELoadRateUnit"],
            "MELoadRate_Accumulation"           : RESPONSE["MELoadRate_Accumulation"],
            "MELoadRateUnit_Accumulation"       : RESPONSE["MELoadRateUnit_Accumulation"],
            "Displacement_Categories"           : RESPONSE["Displacement_Categories"],
            "DisplacementRate"                  : RESPONSE["DisplacementRate"],
            "DisplacementRateUnit"              : RESPONSE["DisplacementRateUnit"],
            "DisplacementRate_Accumulation"     : RESPONSE["DisplacementRate_Accumulation"],
            "DisplacementRateUnit_Accumulation" : RESPONSE["DisplacementRateUnit_Accumulation"],
            "BeaufortRate"                      : RESPONSE["BeaufortRate"],
            "BeaufortRateUnit"                  : RESPONSE["BeaufortRateUnit"],
            "BeaufortRate_Accumulation"         : RESPONSE["BeaufortRate_Accumulation"],
            "BeaufortRateUnit_Accumulation"     : RESPONSE["BeaufortRateUnit_Accumulation"],
            "FOC"                               : RESPONSE["FOC"],
            "FOCAll"                            : RESPONSE["FOCAll"],
            "FOC_YAXIS"                         : RESPONSE["FOC_YAXIS"],
            "CIILASTYEAR"                       : RESPONSE["CIILASTYEAR"],
            "CIIRATING"                         : RESPONSE["CIIRATING"],
            "CII"                               : RESPONSE["CII"],
            "CIISCORE"                          : RESPONSE["CIISCORE"],
            "CIIAll"                            : RESPONSE["CIIAll"],
            "CIIAllSCORE"                       : RESPONSE["CIIAllSCORE"],
            "VoyageInformation"                 : RESPONSE["VoyageInformation"],
            "VoyageInformationUnit"             : RESPONSE["VoyageInformationUnit"],
        }
    else:
        EmissionBoard = {
            "VESSELMASTER": VESSELMASTER,
            "LOGSpeedRate": [],
            "LOGSpeedRateUnit": [],
            "LOGSpeedRate_Accumulation": [],
            "LOGSpeedRateUnit_Accumulation": [],
            "MELoadRate": [],
            "MELoadRateUnit": [],
            "MELoadRate_Accumulation": [],
            "MELoadRateUnit_Accumulation": [],
            "Displacement_Categories": [],
            "DisplacementRate": [],
            "DisplacementRateUnit": [],
            "DisplacementRate_Accumulation": [],
            "DisplacementRateUnit_Accumulation": [],
            "BeaufortRate": [],
            "BeaufortRateUnit": [],
            "BeaufortRate_Accumulation": [],
            "BeaufortRateUnit_Accumulation": [],
            "FOC": [],
            "FOCAll": [],
            "FOC_YAXIS" : {},
            "CIIRATING" : [],
            "CIILASTYEAR" : "",
            "CII": [],
            "CIISCORE": [],
            "CIIAll": [],
            "CIIAllSCORE": [],
            "VoyageInformation": [],
            "VoyageInformationUnit": []
        }
    
    return EmissionBoard


def EmissionBoard(imo, Timestamp_from, Timestamp_to, Unit):
    response = dynamodb.get_noonreport(imo, Timestamp_from, Timestamp_to)
    result = util_EmissionBoard_main(imo, Timestamp_from, Timestamp_to, response, Unit)
    return result
