
import ast
import jwt
from dynamodb import insert, select

# 認可：IMO参照権限チェック
def imo_check(token, imo):

    try:
        # JWTのデコード
        decoded = jwt.decode(token, algorithms=['HS256'], options={'verify_signature': False})
    
        user_id = decoded['sub']
        res_user = select.get_user(user_id)
        
        group_id      = res_user[0]["group_id"]["S"]
        company_id    = res_user[0]["company_id"]["S"]
        
        res_group = select.get_group(company_id)
        
        imo_list = []
        group_list = []
        # Imoリストを取得-------------------------------------------------------
        for item_group in res_group:
            group_list.append(item_group["group_id"]["S"])
            group_imo_list = ast.literal_eval(item_group["imo_list"]["S"])
            for imo_item in group_imo_list:
                imo_list.append(imo_item)
        print(f"imo_list{type(imo_list)}: {imo_list}")
        
        
        if imo in imo_list:
            print(f"auth check OK")
            return 200
        else:
            print(f"auth check NG")
            return 401
            
        
    except jwt.InvalidTokenError as e:
        print('Error decoding JWT:', e)
        return 500