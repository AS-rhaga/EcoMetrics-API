from time import sleep
from botocore.errorfactory import ClientError
import boto3
import json
from datetime import datetime
import ast
import os
from concurrent.futures import ThreadPoolExecutor

import queries
from Cii import cii_caluclate
from EUA_CB import eua_cb_caluclate

_dynamodb_client = boto3.client('dynamodb')
_table_name_user            = os.environ['USER']
_table_name_group           = os.environ['GROUP']
_table_name_favorite        = os.environ['FAVORITE']
_table_name_vessel_master   = os.environ['VESSEL_MASTER']
_table_name_vessel_type     = os.environ['VESSEL_TYPE']
_table_name_vessel_alarm    = os.environ['VESSEL_ALARM']
_table_name_noonreport      = os.environ['NOONREPORT']
_table_name_lo_code_master  = os.environ['LO_CODE_MASTER']
_table_name_euets_fueleu_leg_total = os.environ['EUETS_FUELEU_LEG_TOTAL']

# Favoriteテーブルに登録
def upsert_favorite(dataSet, retry_count: int=42) -> None:

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_favorite,
                Item={
                    "user_id"       : {"S":  dataSet["user_id"]},
                    "company_id"    : {"S":  dataSet["company_id"]},
                    "imo_list"      : {"S":  dataSet["imo_list"]},
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                ConditionExpression="attribute_not_exists(#b)",
                ExpressionAttributeNames={
                    "#b": "b",
                },
            )
            print(f"insert comleted. dataSet: {dataSet}")
                
            return "success"
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"ClientError: code: {code}, Exception: {e}")

        print(f"upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)

# Userテーブルに登録
def upsert_user(dataSetUser, retry_count: int=42) -> None:

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_user,
                Item={
                    "user_id"        : {"S":  dataSetUser["user_id"]},
                    "company_id"     : {"S":  dataSetUser["company_id"]},
                    "group_id"       : {"S":  dataSetUser["group_id"]},
                    "last_disp_gid"  : {"S":  dataSetUser["last_disp_gid"] }                   
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
            )
            print(f"insert comleted. dataSet: {dataSetUser}")
                
            return "success"
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"ClientError: code: {code}, Exception: {e}")

        print(f"upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)

# Userを取得
def get_user(user_id):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_user,
        ExpressionAttributeNames={
            '#name0': 'user_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': user_id}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data

# Groupを取得
def get_group(company_id):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_group,
        ExpressionAttributeNames={
            '#name0': 'company_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': company_id}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data

# Favoriteを取得
def get_favorite(user_id):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_favorite,
        ExpressionAttributeNames={
            '#name0': 'user_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': user_id},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data

# Vessel Masterを一括取得
def batch_get_vessel_masters(imo_list):
    keys = [{"imo": {"S": imo}} for imo in imo_list]
    response = _dynamodb_client.batch_get_item(
        RequestItems={_table_name_vessel_master: {"Keys": keys}}
    )
    return {item["imo"]["S"]: item for item in response.get("Responses", {}).get(_table_name_vessel_master, [])}

# Vessel Typeを一括取得
def get_vessel_type(vessel_type):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_vessel_type,
        ExpressionAttributeNames={
            '#type': 'type', 
        },
        ExpressionAttributeValues={
            ':type_value': {'S': vessel_type},
        },
        KeyConditionExpression='#type = :type_value'
    )
    data = response['Items']
    
    return data

# Noon Reportを一括取得
def batch_get_noonreports(imo_list):
    noonreport_dict = {}

    for imo in imo_list:
        response = _dynamodb_client.query(
            TableName=_table_name_noonreport,
            KeyConditionExpression='#partition_key = :partition_value',
            ExpressionAttributeNames={'#partition_key': 'imo'},
            ExpressionAttributeValues={':partition_value': {'S': imo}},
            ScanIndexForward=False,  # 降順ソート
            Limit=1  # 1件取得
        )
        
        # 取得データがある場合のみ辞書に追加
        if response.get("Items"):
            noonreport_dict[imo] = response["Items"][0]

    return noonreport_dict

# 特定の IMO & 年の Vessel Alarm を取得
def fetch_vessel_alarm(imo, year):
    response = _dynamodb_client.query(
        TableName=_table_name_vessel_alarm,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 = :value1'
    )
    return {imo: response["Items"][0]} if response["Items"] else {imo: {}}

# Vessel Alarm を並列取得
def batch_get_vessel_alarms(imo_list, year):
    vessel_alarm_dict = {}

    # 並列処理
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(lambda imo: fetch_vessel_alarm(imo, year), imo_list)

    # 辞書に格納
    for result in results:
        vessel_alarm_dict.update(result)

    return vessel_alarm_dict

# Lo Code Master取得
def get_lo_code_master(lo_code):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_lo_code_master,
        KeyConditionExpression='#lo_code = :lo_code_value',
        ExpressionAttributeNames={
            '#lo_code': 'lo_code',
        },
        ExpressionAttributeValues={
            ':lo_code_value': {'S': lo_code},
        },
        ScanIndexForward=False,  # 降順ソート
        Limit=1  # 1件取得
    )
    data = response['Items']
    
    return data

# eco-euets-fueleu-leg-totalテーブルから対象の年の最新のレコードを取得
def get_euets_fueleu_leg_total(imo, year):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_euets_fueleu_leg_total,
        ExpressionAttributeNames={
            '#imo': 'imo',
            '#leg_no': 'leg_no',
        },
        ExpressionAttributeValues={
            ':imo_value': {'S': imo},
            ':year_value': {'S': year},  # YYYY に一致する leg_no を取得
        },
        KeyConditionExpression='#imo = :imo_value AND begins_with(#leg_no, :year_value)',
        ScanIndexForward=False,  # 降順ソート
        Limit=1  # 1件取得
    )
    data = response['Items']
    
    return data
