
import ast
from datetime import datetime
import json
import math
import re

import auth
from dynamodb import insert, select, delete
from Util import Util

def calc_banking(res_pooling_group, year):

    # プーリンググループ内の合計CB
    total_cb = 0
    # 単体でのCBがプラスのものの合計CB
    total_cb_plus = 0
    # 単体でのCBがプラスのもののimo_and_opeリスト
    plus_sk_list = []

    for pooling_group in res_pooling_group:

        imo_list = pooling_group["imo_list"]["S"]

        for imo in imo_list:
            vessel_year_list = select.get_year_total_by_year(imo, year)

            for year_record in vessel_year_list:
                loop_year_and_ope = year_record["year_and_ope"]
                loop_cb           = float(year_record["cb"]["S"])   if 'cb' in year_record and year_record["cb"]["S"] != "" else 0.0

                # 去年分のyearレコードを取得する
                last_year_and_ope   = str(int(loop_year_and_ope[0:4]) - 1) + loop_year_and_ope[4:7]
                last_year_record    = select.get_year_total(imo, last_year_and_ope)
                last_year_borrowing = float(last_year_record[0]["borrowing"]) if 'borrowing' in last_year_record and last_year_record["borrowing"]["S"] != "" else 0.0
                this_year_total_cb  = loop_cb - last_year_borrowing

                if this_year_total_cb > 0:
                    total_cb_plus += this_year_total_cb
                    data = {
                        "year_record":        year_record,
                        "this_year_total_cb": this_year_total_cb
                    }
                    plus_sk_list.append(data)

                total_cb += this_year_total_cb

        # プーリンググループ内の合計CBがプラスの場合、プラス分を按分する
        if total_cb > 0:
            for r in plus_sk_list:
                banking_cb = r["this_year_total_cb"] / total_cb_plus * total_cb
                insert.upsert_year_total(r["year_record"], banking_cb)

    print("banking upserting is finished.")
