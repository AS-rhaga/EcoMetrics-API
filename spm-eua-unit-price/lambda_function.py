
import json
import re

import auth
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

    userid = form_data["userid"]
    eua_price = form_data["price"]

    # ユーザ情報取得
    res_user = select.get_user(userid)

    # Groupテーブル取得
    company_id = res_user[0]["company_id"]["S"]
    res_group = select.get_group(company_id, "admin")
   
    # GroupテーブルのEUA単価更新
    insert.upsert_group(res_group, eua_price)

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
