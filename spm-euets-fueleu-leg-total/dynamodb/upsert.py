
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
_table_name_leg_total            = os.environ['LEG_TOTAL']

def upsert_leg_total(data, retry_count: int=42) -> None:

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_leg_total,
                Item={
                    # キー項目
                    "imo"   : { "S": data["imo"] },
                    "leg_no": { "S": data["leg_no"] },
                    
                    # バリュー項目
                    "departure_port"        : { "S": data["departure_port"] },
                    "departure_time"        : { "S": data["departure_time"] },
                    "arrival_port"          : { "S": data["arrival_port"] },
                    "arrival_time"          : { "S": data["arrival_time"] },
                    "eu_rate"               : { "S": data["eu_rate"] },
                    "displacement"          : { "S": data["displacement"] },
                    "leg_type"              : { "S": data["leg_type"] },
                    "distance"              : { "S": data["distance"] },
                    "total_lng"             : { "S": data["total_lng"] },
                    "total_hfo"             : { "S": data["total_hfo"] },
                    "total_lfo"             : { "S": data["total_lfo"] },
                    "total_mdo"             : { "S": data["total_mdo"] },
                    "total_mgo"             : { "S": data["total_mgo"] },
                    "total_foc"             : { "S": data["total_foc"] },
                    "eta_local_date"        : { "S": data["eta_local_date"] },
                    "latest_course"         : { "S": data["latest_course"] },
                    "latest_wind_direction" : { "S": data["latest_wind_direction"] },
                    "latest_beaufort"       : { "S": data["latest_beaufort"] },
                    "latest_log_speed"      : { "S": data["latest_log_speed"] },
                    "latest_me_rpm"         : { "S": data["latest_me_rpm"] },
                    "latest_me_load"        : { "S": data["latest_me_load"] },
                    "latest_foc"            : { "S": data["latest_foc"] },
                    "eua"                   : { "S": data["eua"] },
                    "cb"                    : { "S": data["cb"] },
                    "timestamp"             : { "S": data["timestamp"] }
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                ConditionExpression="attribute_not_exists(#b)",
                ExpressionAttributeNames={
                    "#b": "b",
                },
            )
            print(f"insert completed. imo: {data["imo"]}, data: {data}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"ClientError: code: {code}, Exception: {e}")

        print(f"upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)