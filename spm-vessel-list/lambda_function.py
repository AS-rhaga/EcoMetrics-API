
from time import sleep
from botocore.errorfactory import ClientError
import boto3
import json
from datetime import datetime
import ast
import os


_dynamodb_client = boto3.client('dynamodb')
_table_name_user            = os.environ['USER']
_table_name_group           = os.environ['GROUP']
_table_name_favorite        = os.environ['FAVORITE']
_table_name_vessel_master   = os.environ['VESSEL_MASTER']
#_table_name_vessel_alerm    = os.environ['VESSEL_ALERM']
_table_name_vessel_alarm    = os.environ['VESSEL_ALARM']

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

def get_vessel_alerm(imo, year):
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
                
        # for res_group_item in res_group:
        #     if res_group_item['group_id']['S'] in group_list:
        #         imoList = ast.literal_eval(res_group_item["imo_list"]["S"])
        #         for imo_item in imoList:
        #             imo_list.append(imo_item)
        
        
        # res = get_group(company_id)
        # for res_item in res:
        #     group_list.append(res_item["group_id"]["S"])
        #     if res_item["group_id"]["S"] == gid:
        #         imoList = ast.literal_eval(res_item["imo_list"]["S"])
        #         for imo_item in imoList:
        #             imo_list.append(imo_item)
                    
    else:
        favorite_check = "ok"
        imo_list = favorite_imo_list
        
        
        # favorite_check = "ok"
        # res = get_group(company_id)
        # for res_item in res:
        #     group_list.append(res_item["group_id"]["S"])
        #     imoList = ast.literal_eval(res_item["imo_list"]["S"])
        #     for imo_item in imoList:
        #         imo_list.append(imo_item)
    
    imo_list = list(set(imo_list))

    dt_now = datetime.now()
    year = str(dt_now.year)
    
    # Imoリストを画面表示用に整形
    data_list = []
    group_imo_list = []
    for imo in imo_list:
        res = get_vessel_alerm(imo, year)
        res_vesselmaster = get_vessel_master(imo)
        
        favorite            = "default"
        imo                 = res[0]["imo"]["S"]
        VesselName          = res_vesselmaster[0]["VesselName"]["S"]
        LatestUpdate        = res[0]["LatestUpdate"]["S"]
        oneMonth            = res[0]["oneMonth"]["S"]
        oneMonth_from       = res[0]["oneMonth_from"]["S"]
        oneMonth_to         = res[0]["oneMonth_to"]["S"]
        oneMonth_count      = res[0]["oneMonth_count"]["S"]
        Januarytonow        = res[0]["Januarytonow"]["S"]
        Januarytonow_from   = res[0]["Januarytonow_from"]["S"]
        Januarytonow_to     = res[0]["Januarytonow_to"]["S"]
        LastYear            = res[0]["LastYear"]["S"]
        LastYear_from       = res[0]["LastYear_from"]["S"]
        LastYear_to         = res[0]["LastYear_to"]["S"]
        cp                  = float(res[0]["cp"]["S"])
        cp_from             = res[0]["cp_from"]["S"]    
        cp_to               = res[0]["cp_to"]["S"]
        rf                  = float(res[0]["rf"]["S"])
        rf_from             = res[0]["rf_from"]["S"]
        rf_to               = res[0]["rf_to"]["S"]
        
        data = {
            "imo"               : imo,
            "favorite"          : favorite, 
            "VesselName"        : VesselName, 
            "LatestUpdate"      : LatestUpdate, 
            "oneMonth"          : oneMonth, 
            "oneMonth_from"     : oneMonth_from, 
            "oneMonth_to"       : oneMonth_to,
            "oneMonth_count"    : oneMonth_count,
            "Januarytonow"      : Januarytonow, 
            "Januarytonow_from" : Januarytonow_from, 
            "Januarytonow_to"   : Januarytonow_to, 
            "LastYear"          : LastYear, 
            "LastYear_from"     : LastYear_from, 
            "LastYear_to"       : LastYear_to, 
            "cp"                : cp, 
            "cp_from"           : cp_from, 
            "cp_to"             : cp_to, 
            "rf"                : rf, 
            "rf_from"           : rf_from, 
            "rf_to"             : rf_to, 
        }
        
        # お気に入り登録されているImoだけにお気に入り表示するための判定-------------------------------------------------------
        if favorite_check =="ok":
            if imo in favorite_imo_list:
                data["favorite"] = "checked"
                data_list.append(data)
                group_imo_list.append({"imoNo":imo,"VesselName": VesselName})
        else:
            if imo in favorite_imo_list:
                data["favorite"] = "checked"
            data_list.append(data)
            group_imo_list.append({"imoNo":imo,"VesselName": VesselName})
    
    
    # ソート実行-------------------------------------------------------
    new_group_imo_list = sorted(group_imo_list, key=lambda x: x['VesselName'])
    
    new_data_list = sorted(data_list, key=lambda x: x['VesselName'])
       
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
