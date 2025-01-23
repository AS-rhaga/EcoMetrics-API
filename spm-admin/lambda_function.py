import json
import ast
import jwt
import boto3
import os

_dynamodb_client = boto3.client('dynamodb')

_table_name_user            = os.environ['USER']
_table_name_group           = os.environ['GROUP']

# 認可：IMO参照権限チェック
def admin_check(token):

    try:
        # JWTのデコード
        decoded = jwt.decode(token, algorithms=['HS256'], options={'verify_signature': False})
    
        user_id = decoded['sub']
        res_user = get_user(user_id)
        
        company_id = res_user[0]["company_id"]["S"]
        
        group_id      = res_user[0]["group_id"]["S"]
        group_id_list = ast.literal_eval(group_id)
        print(f"group_id_list: {group_id_list}")
        
        group_id_list_all = get_group(company_id)
        print(f"group_id_list_all: {group_id_list_all}")
        
        admin_count = 0
        for gid_all in group_id_list_all:
            print(f"gid_all: {gid_all}")
            
            if gid_all["group_id"]["S"] in group_id_list:
                if gid_all['admin']['S'] == 'admin':
                    admin_count += 1
        
        print(f"admin_count: {admin_count}")
        if admin_count > 0:
            return 'admin'
        else:
            return 'not_admin'
        
    except jwt.InvalidTokenError as e:
        print('Error decoding JWT:', e)
        return 500

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
            # '#name1': 'group_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': company_id}
            # ':value1': {'S': group_id}
        },
        KeyConditionExpression='#name0 = :value0'
        # KeyConditionExpression='#name0 = :value0 and #name1 = :value1'
    )
    data = response['Items']
    
    return data

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    token = event['headers']['Authorization']
    
    admin = admin_check(token)
    print(f"admin: {admin}")
    
    datas = json.dumps(admin)
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
