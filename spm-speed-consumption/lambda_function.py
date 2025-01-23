import json
import ast
from datetime import datetime

from poseidon import auth
from poseidon.SpeedConsumtion import SpeedConsumtion


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
        
        
    Timestamp_from = queryStringParameters['Timestamp_from']
    Timestamp_to = queryStringParameters['Timestamp_to']
    fromDisp = queryStringParameters['fromDisp']
    toDisp = queryStringParameters['toDisp']
    BallastLaden = queryStringParameters['BallastLaden']
    fromLogSpeed = queryStringParameters['fromLogSpeed']
    toLogSpeed = queryStringParameters['toLogSpeed']
    fromEngineLoad = queryStringParameters['fromEngineLoad']
    toEngineLoad = queryStringParameters['toEngineLoad']
    Beaufort = queryStringParameters['Beaufort']
    fromLogSpeedAF = queryStringParameters['fromLogSpeedAF']
    toLogSpeedAF = queryStringParameters['toLogSpeedAF']
    fromFOCAF = queryStringParameters['fromFOCAF']
    toFOCAF = queryStringParameters['toFOCAF']
    
    speedconsumption = SpeedConsumtion.SpeedConsumtion(imo, Timestamp_from, Timestamp_to, fromDisp, toDisp, BallastLaden, fromLogSpeed, toLogSpeed, fromEngineLoad, toEngineLoad, Beaufort, fromLogSpeedAF, toLogSpeedAF, fromFOCAF, toFOCAF)
    datas = {"datas":speedconsumption}

    
    datas = json.dumps(datas)
    print(f"datas{type(datas)}: {datas}")
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
    