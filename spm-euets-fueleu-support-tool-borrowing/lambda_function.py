
from datetime import datetime
import json
import re
import boto3

from dynamodb import insert, select

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")

    queryStringParameters = event['queryStringParameters']
    input_year = queryStringParameters["year"]

    body = event['body']
   
    # マルチパートデータの境界を解析
    boundary = re.search(r'------WebKitFormBoundary[\w\d]+', body).group()
    
    # 各パートを抽出
    parts = body.split(boundary)
    
    # フォームデータの辞書
    form_data = {}

    # 各パートを解析して値を取得
    for part in parts:
        if 'Content-Disposition' in part:
            name_match = re.search(r'name="([^"]+)"', part)
            if name_match:
                name = name_match.group(1)
                value = part.split('\r\n\r\n')[1].strip()
                form_data[name] = value

    imo       = form_data["imo"]
    operator  = form_data["operator"]
    borrowing = form_data["borrowing"]
    eoy_borrowing = form_data["eoy_borrowing"]

    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]

    # year-totalテーブル取得
    year_and_ope   = input_year + operator
    res_year_total = select.get_year_total(imo, year_and_ope)[0]

    # 罰金フラグの設定
    fine_flag = "0"
    
    # 更新前のyear-totalレコードから既存のborrowing, cbを保持する
    bk_this_year_borrowing     = float(res_year_total["borrowing"]["S"]) if "borrowing" in res_year_total and res_year_total["borrowing"]["S"] != "" else 0.0
    bk_this_year_eoy_borrowing = float(res_year_total["eoy_borrowing"]["S"]) if "eoy_borrowing" in res_year_total and res_year_total["eoy_borrowing"]["S"] != "" else 0.0
    bk_this_year_cb = float(res_year_total["cb"]["S"]) if "cb" in res_year_total and res_year_total["cb"]["S"] != "" else 0.0

    # 更新する項目がborrowingの場合
    # （今年分のYtDのborrowingを画面で設定した場合）と（前年以前分のEoYのborrowingを画面で設定した場合）
    if (input_year == year_now and borrowing != "") or (input_year != year_now and eoy_borrowing != ""):

        # 前年以前分のEoYのborrowingを画面で設定した場合、borrowing, eoy_borrowingを同時に更新する
        if input_year != year_now and eoy_borrowing != "":
            borrowing               = eoy_borrowing
            bk_this_year_eoy_borrowing = float(eoy_borrowing) * 1000000

        # Borrowingの値を100万倍する。（ton ⇒ g に変換）
        borrowing_ton     = float(borrowing) * 1000000

        # 去年分のyearレコードを取得する
        last_year_and_ope = str(int(input_year) - 1) + operator
        last_year_record = select.get_year_total(imo, last_year_and_ope)
        print(f"last_year_record: {(last_year_record)}")

        last_year_banking_cb   = 0
        if len(last_year_record) > 0:
            last_year_banking_cb   = float(last_year_record[0]["banking"]["S"]) if "banking" in last_year_record[0] and last_year_record[0]["banking"]["S"] != "" else 0.0

        print(f"imo+ope:{(imo)}{(operator)} bk_this_year_cb:{(bk_this_year_cb)} borrowing_ton:{(borrowing_ton)} last_year_banking_cb:{(last_year_banking_cb)}")
        # 今年のborrowing, 昨年のbankingと合わせて罰金フラグを確認する
        if last_year_banking_cb > 0:
            if last_year_banking_cb + borrowing_ton + bk_this_year_cb < 0:
                fine_flag = "1"

        # 前年分のbankingがない場合
        else:
            # 今年のborrowingがある場合
            if borrowing_ton > 0:
                if bk_this_year_cb + borrowing_ton < 0:
                    fine_flag = "1"

        print(f"fine_flag: {(fine_flag)}")
    
        # year-totalテーブルのborrowing更新
        insert.upsert_year_total(imo, year_and_ope, str(borrowing_ton), str(bk_this_year_eoy_borrowing), fine_flag, res_year_total, dt_now_str)

    # 更新するのが今年分のeoy_borrowingの場合
    else:
        # Borrowingの値を100万倍する。（ton ⇒ g に変換）
        eoy_borrowing_ton = float(eoy_borrowing) * 1000000

        insert.upsert_year_total(imo, year_and_ope, str(bk_this_year_borrowing), str(eoy_borrowing_ton), fine_flag, res_year_total, dt_now_str)


    # 返却値設定
    data = {
        "result"                   : "Update complete",
    }

    data = json.dumps(data)
    print(f"data: {data}")

    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': data
    }
