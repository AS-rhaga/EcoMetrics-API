from time import sleep
from botocore.errorfactory import ClientError
import boto3
import json
from datetime import datetime
import ast
import os

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
    
def get_vessel_master(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_vessel_master,
        ExpressionAttributeNames={
            '#name0': 'imo',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data

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

def get_vessel_alarm(imo, year):
    data = []
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
    data = response['Items']
    
    return data

# noonreportテーブルからimoをキーとしてtimestampが最新の１レコードを取得する
def get_noonreport(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_noonreport,
        KeyConditionExpression='#partition_key = :partition_value',
        ExpressionAttributeNames={
            '#partition_key': 'imo',
        },
        ExpressionAttributeValues={
            ':partition_value': {'S': imo},
        },
        ScanIndexForward=False,  # 降順にソート（最新の timestamp が先頭）
        Limit=1  # 1件だけ取得
    )
    data = response['Items']
    
    return data

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
        ScanIndexForward=False,  # 降順にソート（最新の timestamp が先頭）
        Limit=1  # 1件だけ取得
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
        ScanIndexForward=False,  # 降順ソート（最新の leg_no を先頭にする）
        Limit=1  # 最新の1件のみ取得
    )
    data = response['Items']
    
    return data

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    # イベントからクエリ取得-------------------------------------------------------
    pathParameters = event['pathParameters']
    pathParameters = pathParameters['proxy']
    pathParameters = pathParameters.split("/")
    queryStringParameters = event['queryStringParameters']
    token = event['headers']['Authorization']
    
    # クエリからユーザID取得-------------------------------------------------------
    user_id = queryStringParameters['user']
    
    # ユーザ付随情報取得-------------------------------------------------------
    res_user      = get_user(user_id)
    group_id      = res_user[0]["group_id"]["S"]
    company_id    = res_user[0]["company_id"]["S"]
    last_disp_gid = res_user[0]["last_disp_gid"]["S"]

    # 初回利用ログイン時 or ログイン時初期表示 or 画面遷移を判定---------------------------------------------
    if "init" in queryStringParameters:
        if len(last_disp_gid) > 0 :
            gid = last_disp_gid
        else:
            gid = ast.literal_eval(group_id)[0]
    else:
        gid = queryStringParameters['GID']

    # お気に入りリスト取得-------------------------------------------------------
    res_favorite = get_favorite(user_id)
    favorite_imo_list = ast.literal_eval(res_favorite[0]["imo_list"]["S"])
    
    # Favoriteボタンが押下された時の処理-------------------------------------------------------
    if "favorite" in queryStringParameters:
        
        # クエリからお気に入り選択されたImoの情報を取得-------------------------------------------------------
        favorite_query = queryStringParameters['favorite']
        imo_query = queryStringParameters['imo']
        
        # 選択されたImoがお気に入りリストにない場合のみデータ登録-------------------------------------------------------
        if len(favorite_imo_list) == 0: 
            favorite_imo_list.append(imo_query)
        else:
            if imo_query not in favorite_imo_list and favorite_query == "default":
                favorite_imo_list.append(imo_query)
            elif imo_query in favorite_imo_list and favorite_query == "checked":
                favorite_imo_list.remove(imo_query)
        
        dataSet = {
            "user_id"       : user_id,
            "company_id"    : company_id,
            "imo_list"      : json.dumps(favorite_imo_list),
        }
        message = upsert_favorite(dataSet)
        
    # Userリストを更新-----------------------------------------------------
    dataSetUser = {
        "user_id"       : user_id,
        "company_id"    : company_id,
        "group_id"      : group_id,
        "last_disp_gid" : gid,
    }
    message = upsert_user(dataSetUser)

    imo_list = []
    favorite_check = ""
    
    # Imoリストを取得-------------------------------------------------------
    res_group = get_group(company_id)
    if gid != "Favorite":
        
        for res_group_item in res_group:
            if res_group_item['group_id']['S'] == gid:
                imo_list = ast.literal_eval(res_group_item["imo_list"]["S"])
                    
    else:
        favorite_check = "ok"
        imo_list = favorite_imo_list
    
    imo_list = list(set(imo_list))

    dt_now = datetime.now()
    year = str(dt_now.year)
    
    # Imoリストを画面表示用に整形
    data_list = []
    group_imo_list = []
    for imo in imo_list:
        res_vessel_master = get_vessel_master(imo)
        res_vessel_type = get_vessel_type(res_vessel_master[0]["VesselType"]["S"])
        res_vessel_alarm = get_vessel_alarm(imo, year)
        res_noonreport = get_noonreport(imo)
        # res_lo_code_master_departure = get_lo_code_master(res_noonreport[0]["port_code"]["S"])
        
        # noonreportのTimeStampから年(YYYY)を取得
        noonreport_timestamp_str = res_noonreport[0]["timestamp"]["S"]
        noonreport_timestamp_dt = datetime.strptime(noonreport_timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
        noonreport_timestamp_year_str = str(noonreport_timestamp_dt.year)
        res_euets_fueleu_leg_total = get_euets_fueleu_leg_total(imo, noonreport_timestamp_year_str)

        # End of YearのCii算出
        res_cii_data = cii_caluclate.calc_cii(imo, res_vessel_master, res_vessel_alarm)

        # Year to Date、End of YearのEUA、CB算出
        res_eua_cb_data = eua_cb_caluclate.calc_eua_cb(imo)        
        
        favorite               = "default"
        imo                    = res_vessel_master[0].get("imo", {}).get("S", "")
        vesselName             = res_vessel_master[0].get("VesselName", {}).get("S", "")

        oneMonth               = res_vessel_alarm[0]["oneMonth"]["S"]
        januaryToNow           = res_vessel_alarm[0]["Januarytonow"]["S"]
        lastYear               = res_vessel_alarm[0]["LastYear"]["S"]
        
        grosstongue            = res_vessel_master[0].get("Grosstongue", {}).get("S", "")
        vesselType             = res_vessel_type[0].get("type_name", {}).get("S", "")
        latitude               = res_noonreport[0].get("lat", {}).get("S", "")
        longitude              = res_noonreport[0].get("lng", {}).get("S", "")
        latestUpdate           = res_noonreport[0].get("timestamp", {}).get("S", "")
        beaufort               = res_noonreport[0].get("beaufort", {}).get("S", "")
        windDirection          = res_noonreport[0].get("wind_direction", {}).get("S", "")
        course                 = res_noonreport[0].get("course", {}).get("S", "")
        portOfDeparture        = res_euets_fueleu_leg_total[0].get("departure_port", {}).get("S", "")
        portOfArrival          = res_euets_fueleu_leg_total[0].get("arrival_port", {}).get("S", "")
        actualTimeOfDeparture  = res_noonreport[0].get("start_local_date", {}).get("S", "")
        estimatedTimeOfArrival = res_noonreport[0].get("eta_local_date", {}).get("S", "")
        logSpeed               = res_noonreport[0].get("log_speed", {}).get("S", "")
        # ToDo vessel_masterにlog_speed_maxが追加されたらべたを削除し、以下のコメントアウトを外す
        # logSpeedMax            = res_vessel_master[0].get("log_speed_max", {}).get("S", "")
        logSpeedMax            = "20"
        meRPM                  = res_noonreport[0].get("me_rpm", {}).get("S", "")
        # ToDo vessel_masterにme_rpm_maxが追加されたらべたを削除し、以下のコメントアウトを外す
        # meRPMMax               = res_vessel_master[0].get("me_rpm_max", {}).get("S", "")
        meRPMMax               = "100"
        meLoad                 = res_noonreport[0].get("me_load", {}).get("S", "")
        foc                    = res_noonreport[0].get("total_foc", {}).get("S", "")
        cpCurveAlarm           = float(res_vessel_alarm[0].get("cp", {}).get("S", ""))
        ciiYearToDate          = res_vessel_alarm[0].get("Januarytonow", {}).get("S", "")
        # 以下計算が必要。いったん空文字設定する
        ciiEndOfYear           = res_cii_data["eoy_cii_score"]
        euaYearToDate          = res_eua_cb_data["ytd_eua"]
        euaEndOfYear           = res_eua_cb_data["eoy_eua"]
        cbYearToDate           = res_eua_cb_data["ytd_cb"]
        cbEndOfYear            = res_eua_cb_data["eoy_cb"]

        # stateの設定値決定
        state = ""
        if "state" in res_noonreport and res_noonreport[0]["state"]["S"] != "":
            state = res_noonreport[0]["state"]["S"]
        else:
            if res_euets_fueleu_leg_total[0]["leg_type"] == "Sailing":
                state = "AT SEA"
            elif res_euets_fueleu_leg_total[0]["leg_type"] == "Port":
                state = "IN PORT"
            else:
                state = "-"
        
        print(f"imo:{imo}   lambda_function res_eua_cb_data:{res_eua_cb_data}")
        print(f"imo:{imo}   lambda_function euaEndOfYear:{euaEndOfYear}, cbEndOfYear:{cbEndOfYear}")

        data = {
            "imo"                    : imo,
            "favorite"               : favorite, 
            "vesselName"             : vesselName, 
            "oneMonth"               : oneMonth,
            "januaryToNow"           : januaryToNow, 
            "lastYear"               : lastYear, 
            "grosstongue"            : grosstongue, 
            "vesselType"             : vesselType, 
            "latitude"               : latitude, 
            "longitude"              : longitude, 
            "latestUpdate"           : latestUpdate, 
            "beaufort"               : beaufort,
            "windDirection"          : windDirection,
            "course"                 : course, 
            "portOfDeparture"        : portOfDeparture, 
            "portOfArrival"          : portOfArrival, 
            "actualTimeOfDeparture"  : actualTimeOfDeparture, 
            "estimatedTimeOfArrival" : estimatedTimeOfArrival, 
            "logSpeed"               : logSpeed, 
            "logSpeedMax"            : logSpeedMax, 
            "meRPM"                  : meRPM, 
            "meRPMMax"                  : meRPMMax, 
            "meLoad"                 : meLoad, 
            "foc"                    : foc, 
            "cpCurveAlarm"           : cpCurveAlarm,
            "ciiYearToDate"          : ciiYearToDate,
            "ciiEndOfYear"           : ciiEndOfYear,
            "euaYearToDate"          : euaYearToDate,
            "euaEndOfYear"           : euaEndOfYear,
            "cbYearToDate"           : cbYearToDate,
            "cbEndOfYear"            : cbEndOfYear,
            "state"                  : state,
        }
        
        # お気に入り登録されているImoだけにお気に入り表示するための判定-------------------------------------------------------
        if favorite_check =="ok":
            if imo in favorite_imo_list:
                data["favorite"] = "checked"
                data_list.append(data)
                group_imo_list.append({"imoNo":imo,"VesselName": vesselName})
        else:
            if imo in favorite_imo_list:
                data["favorite"] = "checked"
            data_list.append(data)
            group_imo_list.append({"imoNo":imo,"VesselName": vesselName})
    
    
    # ソート実行-------------------------------------------------------
    new_group_imo_list = sorted(group_imo_list, key=lambda x: x['VesselName'])
    
    new_data_list = sorted(data_list, key=lambda x: x['vesselName'])
       
    group_list = ast.literal_eval(group_id)
    # new_group_list = sorted(group_list)
    new_group_list = group_list
    
    group_id = group_list[0]
    
    # 選択肢の先頭にFavoriteを追加
    new_group_list.insert(0, "Favorite")
    datas = {
        "datas":
            {
                "user_id":user_id,
                "group_id":group_id,
                "company_id":company_id,
                "gid":gid,
                "gidList":new_group_list,
                "imoList":new_group_imo_list,
                "rows": new_data_list, 
            }
    }
    datas = json.dumps(datas)
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
