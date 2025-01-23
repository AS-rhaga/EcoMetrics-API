
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

    imo = form_data["imo"]
    
    # 認可：IMO参照権限チェック
    authCheck = auth.imo_check(token, imo)
    if authCheck == 401 or authCheck == 500:
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },  
            'body': authCheck
        }
    
    # FOCFormulasテーブル更新
    insert.upsert_vesselmaster(form_data)

    # 取得
    res_foc_formulas = select.get_foc_formulas(imo)

    # 返却値設定
    data = {
        "imo"                   : res_foc_formulas[0]["imo"]["S"],
        "me_ballast"            : res_foc_formulas[0]["me_ballast"]["S"],
        "me_laden"              : res_foc_formulas[0]["me_laden"]["S"],
        "auxiliary_equipment"   : res_foc_formulas[0]["auxiliary_equipment"]["S"],
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
    