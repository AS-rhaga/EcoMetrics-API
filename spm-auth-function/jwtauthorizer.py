from urllib import request
import json
from jose import jwt
from typing import Any, Dict
from enum import Enum

class DenyResponse(Enum):
    NO_MATCH_KEYWORD = 0
    CAN_NOT_FIND_KEYWORD = 1
    FORBIDDEN_REQUEST = 2
    UNEXPECTED_ERROR = 3
    
Errorlist = {
    DenyResponse.NO_MATCH_KEYWORD : "必要なキーワードが異なる",
    DenyResponse.CAN_NOT_FIND_KEYWORD: "キーワードを見つけられない",
    DenyResponse.FORBIDDEN_REQUEST: "非認証",
    DenyResponse.UNEXPECTED_ERROR: "予期しないエラー"
}

#レスポンス作成
def create_response(resource : str,effect="Deny") -> dict:
    authResponse = {}
    authResponse['principalId'] = "user"
    policyDocument = {}
    policyDocument['Version'] = "2012-10-17"
    policyDocument['Statement'] = []
    statementOne = {}
    statementOne['Action'] = "execute-api:Invoke"
    statementOne['Effect'] = effect
    statementOne['Resource'] = resource
    policyDocument['Statement'] = [statementOne]
    authResponse['policyDocument'] = policyDocument
    
    return authResponse

def pubkey_acquisition(alg,typ) -> str:
    #JWT形式かチェック
    if typ != "JWT":
        raise ValueError
    
    #公開鍵取得
    res = request.Request("https://ipfuolzg5c.execute-api.ap-northeast-1.amazonaws.com/example-jwt/.well-known/jwks.json",headers={})
    with request.urlopen(res) as r:
        body = r.read().decode('utf-8')
    key = {}
    body = json.loads(body)
    key["keys"] = [body]
    
    #kidとalgの整合性チェック
    # pub_key = [x for x in key["keys"] if x["kid"] == kid and x["alg"] == alg]
    # return pub_key[0]
    
    #algのチェック
    pub_key = [x for x in key["keys"] if x["alg"] == alg]
    return pub_key[0]

def jwt_verify(key,token) -> dict:
    #確認のために有効時間の確認を外した
    payload: Dict[str: Any] = jwt.decode(
        token,
        key,
        algorithms=["RS256"],
        audience="EcoMetrics",
        issuer="WADATSUMI",
    )
    
    return payload


def authorizer(event):
    try:
        try:
            token = event["authorizationToken"]
        except KeyError:
            print(Errorlist[DenyResponse.CAN_NOT_FIND_KEYWORD])
            return False
        
        try:
            header = jwt.get_unverified_header(token)
        except:
            return False
        
        try:
            alg = header["alg"]
            typ = header["typ"]
            # kid = header["kid"]
        except KeyError:
            print(Errorlist[DenyResponse.CAN_NOT_FIND_KEYWORD])
            return False
            
        try:
            key = pubkey_acquisition(alg,typ)
        except ValueError:
            print(Errorlist[DenyResponse.CAN_NOT_FIND_KEYWORD])
            return False
        
        except NameError:
            print(Errorlist[DenyResponse.NO_MATCH_KEYWORD])
            return False
        
        try:
            jwt_verify(key,token)
        except jwt.JWTError:
            print(Errorlist[DenyResponse.FORBIDDEN_REQUEST])
            return False
    except:
        print(Errorlist[DenyResponse.UNEXPECTED_ERROR])
        return False
    return True

