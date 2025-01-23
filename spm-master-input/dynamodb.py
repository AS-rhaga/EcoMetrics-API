
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

_table_name_user            = os.environ['USER']
_table_name_group           = os.environ['GROUP']
_table_name_vesselmaster    = os.environ['VESSEL_MASTER']
_table_name_data_channel    = os.environ['DATA_CHANNEL']
_table_name_spcurve         = os.environ['SPEED_CONSUMPTION_CURVE']
_table_name_fuel_oil_type   = os.environ['FUEL_OIL_TYPE']
_table_name_vessel_type     = os.environ['VESSEL_TYPE']



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
    

def upsert_vesselmaster(data, retry_count: int=42) -> None:

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_vesselmaster,
                Item={
                    # キー項目
                    "imo"           : {"S": data["imo"]},
                    
                    # バリュー項目
                    "Ballast"           : {"S": data["Ballast"]},
                    "BuildDate"         : {"S": data["BuildDate"]},
                    "Deadweight"        : {"S": data["Deadweight"]},
                    "Grosstongue"       : {"S": data["Grosstongue"] },
                    "Laden"             : {"S": data["Laden"]},
                    "LastDockDate"      : {"S": data["LastDockDate"]},
                    "HullCleaningDate"  : {"S": data["HullCleaningDate"]},
                    "OilType"           : {"S": data["OilType"]},
                    "Owner"             : {"S": data["Owner"]},
                    "Size"              : {"S": data["Size"]},
                    "VesselName"        : {"S": data["VesselName"]},
                    "VesselType"        : {"S": data["VesselType"]},
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

def upsert_datachannel(data, retry_count: int=42) -> None:  

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_data_channel,
                Item={
                    # キー項目
                    "imo": { "S": data["imo"] },
                    "rev": { "S": data["rev"] },
                    
                    # バリュー項目
                    "lower_beaufort": { "S": data["lower_beaufort"] },
                    "lower_boiler_foc": { "S": data["lower_boiler_foc"] },
                    "lower_course": { "S": data["lower_course"] },
                    "lower_displacement": { "S": data["lower_displacement"] },
                    "lower_dwt": { "S": data["lower_dwt"] },
                    "lower_ge_foc": { "S": data["lower_ge_foc"] },
                    "lower_gt": { "S": data["lower_gt"] },
                    "lower_lat": { "S": data["lower_lat"] },
                    "lower_lng": { "S": data["lower_lng"] },
                    "lower_log_distance": { "S": data["lower_log_distance"] },
                    "lower_log_speed": { "S": data["lower_log_speed"] },
                    "lower_me_foc": { "S": data["lower_me_foc"] },
                    "lower_me_load": { "S": data["lower_me_load"] },
                    "lower_me_rpm": { "S": data["lower_me_rpm"] },
                    "lower_og_distance": { "S": data["lower_og_distance"] },
                    "lower_og_speed": { "S": data["lower_og_speed"] },
                    "lower_swell_direction": { "S": data["lower_swell_direction"] },
                    "lower_swell_height": { "S": data["lower_swell_height"] },
                    "lower_swell_period": { "S": data["lower_swell_period"] },
                    "lower_total_foc": { "S": data["lower_total_foc"] },
                    "lower_wave_direction": { "S": data["lower_wave_direction"] },
                    "lower_wave_height": { "S": data["lower_wave_height"] },
                    "lower_wave_period": { "S": data["lower_wave_period"] },
                    "lower_wind_direction": { "S": data["lower_wind_direction"] },
                    "lower_wind_speed": { "S": data["lower_wind_speed"] },
                    "upper_beaufort": { "S": data["upper_beaufort"] },
                    "upper_boiler_foc": { "S": data["upper_boiler_foc"] },
                    "upper_course": { "S": data["upper_course"] },
                    "upper_displacement": { "S": data["upper_displacement"] },
                    "upper_dwt": { "S": data["upper_dwt"] },
                    "upper_ge_foc": { "S": data["upper_ge_foc"] },
                    "upper_gt": { "S": data["upper_gt"] },
                    "upper_lat": { "S": data["upper_lat"] },
                    "upper_lng": { "S": data["upper_lng"] },
                    "upper_log_distance": { "S": data["upper_log_distance"] },
                    "upper_log_speed": { "S": data["upper_log_speed"] },
                    "upper_me_foc": { "S": data["upper_me_foc"] },
                    "upper_me_load": { "S": data["upper_me_load"] },
                    "upper_me_rpm": { "S": data["upper_me_rpm"] },
                    "upper_og_distance": { "S": data["upper_og_distance"] },
                    "upper_og_speed": { "S": data["upper_og_speed"] },
                    "upper_swell_direction": { "S": data["upper_swell_direction"] },
                    "upper_swell_height": { "S": data["upper_swell_height"] },
                    "upper_swell_period": { "S": data["upper_swell_period"] },
                    "upper_total_foc": { "S": data["upper_total_foc"] },
                    "upper_wave_direction": { "S": data["upper_wave_direction"] },
                    "upper_wave_height": { "S": data["upper_wave_height"] },
                    "upper_wave_period": { "S": data["upper_wave_period"] },
                    "upper_wind_direction": { "S": data["upper_wind_direction"] },
                    "upper_wind_speed": { "S": data["upper_wind_speed"] },
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
        

def upsert_spcurve(data, retry_count: int=42) -> None:  

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_spcurve,
                Item={
                    # キー項目
                    "imo": { "S": data["imo"] },
                    "no": { "S": data["no"] },
                    
                    # バリュー項目
                    "name"      : { "S": data["name"] },
                    "a"         : { "S": data["a"] },
                    "alpha"     : { "S": data["alpha"] },
                    "C"         : { "S": data["c"] },
                    "display"   : { "S": data["display"] },
                    "description"   : { "S": data["description"] },
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



def get_vesselmaster(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_vesselmaster,
        ExpressionAttributeNames={
            '#name0': 'imo',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
def get_datachannel(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_data_channel,
        ExpressionAttributeNames={
            '#name0': 'imo',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
def get_spcurve(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_spcurve,
        ExpressionAttributeNames={
            '#name0': 'imo',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
    
def get_fuel_oil_type():
    data = []
    response = _dynamodb_client.scan(
        TableName=_table_name_fuel_oil_type,
    )
    data = response['Items']
    
    return data
    
def get_vessel_type():
    data = []
    response = _dynamodb_client.scan(
        TableName=_table_name_vessel_type,
    )
    data = response['Items']
    
    return data