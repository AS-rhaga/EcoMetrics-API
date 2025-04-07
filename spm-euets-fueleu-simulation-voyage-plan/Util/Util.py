
import ast
import math
import dynamodb
from datetime import datetime

# vesselmaster
def util_vesselmaster(response):
    
    vesselmaster = {
        "BuildDate"         : response[0]["BuildDate"]["S"],
        "Deadweight"        : response[0]["Deadweight"]["S"],
        "Grosstongue"       : response[0]["Grosstongue"]["S"],
        "LastDockDate"      : response[0]["LastDockDate"]["S"],
        "HullCleaningDate"  : response[0]["HullCleaningDate"]["S"],
        "OilType"           : response[0]["OilType"]["S"],
        "Owner"             : response[0]["Owner"]["S"],
        "Size"              : response[0]["Size"]["S"],
        "VesselName"        : response[0]["VesselName"]["S"],
        "VesselType"        : response[0]["VesselType"]["S"],
        "Ballast"           : response[0]["Ballast"]["S"],
        "Laden"             : response[0]["Laden"]["S"],
    }
    
    return vesselmaster

# foc formulas
def util_focformulas(response):
    
    vesselmaster = {
        "imo"                : response[0]["imo"]["S"],
        "me_ballast"         : response[0]["me_ballast"]["S"],
        "me_laden"           : response[0]["me_laden"]["S"],
        "auxiliary_equipment": response[0]["auxiliary_equipment"]["S"]
    }
    
    return vesselmaster

def to_datetime(str_time):

    dt_time = datetime(
        year   = int(str_time[0:4]),
        month  = int(str_time[5:7]),
        day    = int(str_time[8:10]),
        hour   = int(str_time[11:13]),
        minute = int(str_time[14:16])
    )
    return dt_time

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

# datetimeをfloatに変換する。
def timestamp_datetime_to_float(timestamp):
    try:
        timestamp = round(datetime.timestamp(timestamp)*1000)
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""

def to_datetime(str_time):

    dt_time = datetime(
        year   = int(str_time[0:4]),
        month  = int(str_time[5:7]),
        day    = int(str_time[8:10]),
        hour   = int(str_time[11:13]),
        minute = int(str_time[14:16])
    )
    return dt_time
