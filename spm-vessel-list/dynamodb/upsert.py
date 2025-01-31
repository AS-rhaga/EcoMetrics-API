
from botocore.errorfactory import ClientError
import boto3
from datetime import datetime
import os

_dynamodb_client = boto3.client('dynamodb')
_table_name_user            = os.environ['USER']
_table_name_favorite        = os.environ['FAVORITE']

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
