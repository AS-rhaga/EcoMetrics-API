
import boto3
from datetime import datetime
import json
import re

import auth
from dynamodb import insert, select, delete
from YearTotal import year_total

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    body = event['body']
    pathParameters = event['pathParameters']['proxy'].split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']

    # パラメーター取得
    user_id        = queryStringParameters['user']
    
    body = event['body']
    print(f"body{type(body)}: {body}")
    token = event['headers']['Authorization']

    pooling_groups = json.loads(body)
    print(f"pooling_groups:{pooling_groups}")


    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # company_id取得
    company_id = select.get_user(user_id)[0]["company_id"]["S"]

    company_and_year = year_now + company_id

    # 既存Simulationテーブル削除
    delete.delete_pooling_table(company_and_year)

    # 新規Simulationテーブル登録
    for item in pooling_groups:
        
        group_name = item["pooling_group_name"]
        imo_list   = item["pooling_group_imo_list"]

        upsert_data = {
            "company_and_year": company_and_year,
            "group_name"      : group_name,
            "imo_list"        : imo_list
        }
        insert.upsert_pooling_table(upsert_data)
        print(f"upsert company_and_year: {company_and_year}, group_name: {company_and_year}")

    # 登録されたプーリンググループを取得
    res_pooling_group = select.get_pooling_table(company_and_year)

    # 各imoのyearテーブルのbanking項目の数値を再計算して上書き更新
    year_total.calc_banking(res_pooling_group, year_now, company_id)

    result = {
        "result": "Pooling Group TBL registration completed."
    }

    datas = json.dumps(result)

    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
