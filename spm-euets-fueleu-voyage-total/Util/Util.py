import json
import ast
from datetime import datetime, timedelta, timezone
from decimal import Decimal

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

# 全ての数値を小数点第位2位に変換
def format_to_two_decimal(value):
    try:
        # 数値型やstr型を浮動小数点数に変換
        num = float(value)
        # 小数点第1位まで四捨五入して文字列に戻す
        formatted = f"{num:.2f}"
        # 整数の場合は '.0' を保持、それ以外は不要な末尾の0を削除
        return formatted if formatted.endswith('.00') else formatted.rstrip('0').rstrip('.')
    except ValueError:
        # 数値に変換できない場合は元の値をそのまま返す
        return float(value)