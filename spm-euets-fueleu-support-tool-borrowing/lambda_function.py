
from datetime import datetime
import json
import re
import boto3

from dynamodb import insert, select

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    body = event['body']
    token = event['headers']['Authorization']
    
    # マルチパートデータの境界を解析
    boundary = re.search(r'------WebKitFormBoundary[\w\d]+', body).group()
    
    # 各パートを抽出
    parts = body.split(boundary)
    
    # フォームデータの辞書
    form_data = {}

    # 各パートを解析して値を取得
    for part in parts:
        if 'Content-Disposition' in part:
            name_match = re.search(r'name="([^"]+)"', part)
            if name_match:
                name = name_match.group(1)
                value = part.split('\r\n\r\n')[1].strip()
                form_data[name] = value

    imo       = form_data["imo"]
    operator  = form_data["operator"]
    borrowing = form_data["borrowing"]

    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # year-totalテーブル取得
    year_and_ope   = year_now + operator
    res_year_total = select.get_year_total(imo, year_and_ope)[0]

    # Borrowingの値を100万倍する。（ton ⇒ g に変換）
    borrowing_ton = str(float(borrowing) * 1000000)
   
    # year-totalテーブルのborrowing更新
    insert.upsert_year_total(imo, year_and_ope, borrowing_ton, res_year_total, dt_now_str)

    # 返却値設定
    data = {
        "result"                   : "Update complete",
    }

    data = json.dumps(data)
    print(f"data: {data}")

    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': data
    }
