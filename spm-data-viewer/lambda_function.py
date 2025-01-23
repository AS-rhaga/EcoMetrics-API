import json
import ast
import boto3
import boto3 as boto3_2
from datetime import datetime

from poseidon import auth
from poseidon.DataViewer import DataViewer


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
    
    vesseloverview = DataViewer.DataViewer(imo, Timestamp_from, Timestamp_to)
    datas = {"datas":vesseloverview}
    
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
    