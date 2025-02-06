
#標準ライブラリ 
import os
import sys
import json
import boto3
from botocore.errorfactory import ClientError
import os

# 自作
import dynamodb

_dynamodb_client = boto3.client('dynamodb')
_table_name_year_total = os.environ['YEAR_TOTAL']

def upsert_year_total(data, retry_count: int=42) -> None:

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_year_total,
                Item={
                    # キー項目
                    "imo"         : { "S": data["imo"] },
                    "year_and_ope": { "S": data["year_and_ope"] },
                    
                    # バリュー項目(borrowing以外)
                    "distance"    : { "S": data["distance"] },
                    "total_lng"   : { "S": data["total_lng"] },
                    "total_hfo"   : { "S": data["total_hfo"] },
                    "total_lfo"   : { "S": data["total_lfo"] },
                    "total_mdo"   : { "S": data["total_mdo"] },
                    "total_mgo"   : { "S": data["total_mgo"] },
                    "total_foc"   : { "S": data["total_foc"] },
                    "eua"         : { "S": data["eua"] },
                    "cb"          : { "S": data["cb"] },
                    "banking"     : { "S": data["banking"] },
                    "fine_flag"   : { "S": data["fine_flag"] },
                    "pooling_group": { "S": data["pooling_group"]},
                    "timestamp"   : { "S": data["timestamp"] }
                },

                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                ConditionExpression="attribute_not_exists(#b)",
                ExpressionAttributeNames={
                    "#b": "b",
                },
            )
            print(f"insert comleted. imo: {data["imo"]}, data: {data}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"ClientError: code: {code}, Exception: {e}")

        print(f"upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)