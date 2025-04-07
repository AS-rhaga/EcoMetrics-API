
import ast
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# タイムゾーンを指定
tz_utc  = timezone(timedelta(hours=0))  # UTC+0（UTCの場合）

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

# EmissionBoardで日付範囲で条件分岐を行うためにdatetime型に変換。
def timestamp_calc_datetime(timestamp):
    timestamp = timestamp_str_delete_Z(timestamp)
    timestamp = timestamp_str_to_datetimeUtc(timestamp)
    return timestamp

# 末尾のZを取り除く
def timestamp_str_delete_Z(timestamp):
    
    try:
        timestamp = timestamp.rstrip('Z')
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""
    
    return timestamp

# 文字列「2023-01-01T02:00:00」をdatetimeに変換する。タイムゾーンはUTCとする。
def timestamp_str_to_datetimeUtc(timestamp):
    
    try:
        timestamp = datetime.fromisoformat(timestamp).replace(tzinfo=tz_utc)
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""
    
    return timestamp

# 全ての数値を小数点第位1位に変換
def format_to_one_decimal(value):
    try:
        # 数値型やstr型を浮動小数点数に変換
        num = float(value)
        # 小数点第1位まで四捨五入する
        formatted = f"{num:.1f}"
        # 整数の場合は '.0' を保持、それ以外は不要な末尾の0を削除
        if formatted.endswith('.0'):
            formatted = formatted
        else:
            formatted = formatted.rstrip('0').rstrip('.')
        return float(formatted)
    except ValueError:
        # 数値に変換できない場合は元の値をそのまま返す
        return str(value)