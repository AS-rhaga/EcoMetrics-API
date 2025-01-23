import json
import ast
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# タイムゾーンを指定
tz_utc  = timezone(timedelta(hours=0))  # UTC+0（UTCの場合）
tz_jp   = timezone(timedelta(hours=9))  # UTC+9（日本の場合）

    
import json
import ast
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# タイムゾーンを指定
tz_utc  = timezone(timedelta(hours=0))  # UTC+0（UTCの場合）
tz_jp   = timezone(timedelta(hours=9))  # UTC+9（日本の場合）


# EmissionBoardで日付範囲で条件分岐を行うためにdatetime型に変換。
def timestamp_calc_datetime(timestamp):
    timestamp = timestamp_str_delete_Z(timestamp)
    timestamp = timestamp_str_to_datetimeUtc(timestamp)
    return timestamp
    
    
# Haighcharts用に時系列データの日時をtimestamp型に変換
def timestamp_for_HighCharts(timestamp):
    timestamp = timestamp_str_delete_Z(timestamp)
    timestamp = timestamp_str_to_datetimeUtc(timestamp)
    timestamp = timestamp_datetime_to_float(timestamp)
    return timestamp


# Helper----------------------------------------------------------------------------------------------------------------
# datetimeを文字列「%Y/%m/%d %H:%M:%S」に変換する。
def timestamp_datetime_to_str(timestamp):
    try:
        timestamp = timestamp.strftime("%Y/%m/%d %H:%M:%S")
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""
        

# timestampをdatetimeに変換する。タイムゾーンはUTCとする。
def timestamp_float_to_datetimeUtc(timestamp):
    try:
        timestamp = datetime.fromtimestamp(int(timestamp) / 1000).replace(tzinfo=tz_utc)
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""
        

# datetimeをtimestampに変換する。
def timestamp_datetime_to_float(timestamp):
    try:
        timestamp = round(datetime.timestamp(timestamp)*1000)
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""


# 文字列「2023-01-01T02:00:00」をdatetimeに変換する。タイムゾーンはUTCとする。
def timestamp_str_to_datetimeUtc(timestamp):
    
    try:
        timestamp = datetime.fromisoformat(timestamp).replace(tzinfo=tz_utc)
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""
    
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

# Floatに変換できるかチェック
def isfloat(s):  # 浮動小数点数値を表しているかどうかを判定
    try:
        float(s)  # 文字列を実際にfloat関数で変換してみる
    except ValueError:
        return False
    else:
        return True
    
# 全ての数値を小数点第位1位に変換
def format_to_one_decimal(value):
    try:
        # 数値型やstr型を浮動小数点数に変換
        num = float(value)
        # 小数点第1位まで四捨五入して文字列に戻す
        formatted = f"{num:.1f}"
        # 整数の場合は '.0' を保持、それ以外は不要な末尾の0を削除
        return formatted if formatted.endswith('.0') else formatted.rstrip('0').rstrip('.')
    except ValueError:
        # 数値に変換できない場合は元の値をそのまま返す
        return str(value)