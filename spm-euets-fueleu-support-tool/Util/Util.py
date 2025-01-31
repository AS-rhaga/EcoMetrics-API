
import ast
import math
from datetime import datetime
import re

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

# foc formulas
def util_focformulas(response):
    
    focformulas = {
        "imo"                : response[0]["imo"]["S"],
        "me_ballast"         : response[0]["me_ballast"]["S"],
        "me_laden"           : response[0]["me_laden"]["S"],
        "auxiliary_equipment": response[0]["auxiliary_equipment"]["S"]
    }
    
    return focformulas

# FuelListをリストに変換
def convertFuelOileStringToList(text):
    
    pattern = r'\([^()]*\([^()]*\)[^()]*\)|\([^()]*\)'
    matches = re.findall(pattern, text)

    # 前後の括弧を除去
    cleaned_matches = [match[1:-1] for match in matches]

    return cleaned_matches