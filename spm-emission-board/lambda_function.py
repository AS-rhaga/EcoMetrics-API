import json
import ast
from datetime import datetime

import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


from poseidon import auth
from poseidon.EmissionBoard import EmissionBoard


def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    
    pathParameters = event['pathParameters']['proxy'].split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']
    
    # imo取得
    imo = pathParameters[0]
    print(f"imo{type(imo)}: {imo}")
    
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
        
    
    # 集計処理実行
    Timestamp_from = queryStringParameters['Timestamp_from']
    Timestamp_to = queryStringParameters['Timestamp_to']
    Unit = queryStringParameters['Unit']
    vesseloverview = EmissionBoard.EmissionBoard(imo, Timestamp_from, Timestamp_to, Unit)
    datas = {"datas":vesseloverview}
    
    datas = json.dumps(datas)
    print(f"datas{type(datas)}: {datas}")
    
    
    
    # リクエストペイロードのサイズを計算
    request_body_size = len(json.dumps(event['body']))
    
    # レスポンスペイロードのサイズを計算
    response_body = {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
    response_body_size = len(json.dumps(response_body))
    
    
    # ログにリクエストペイロードサイズとレスポンスペイロードサイズを記録
    logger.info(f"Request Payload Size: {request_body_size} bytes")
    logger.info(f"Response Payload Size: {response_body_size} bytes")
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
    