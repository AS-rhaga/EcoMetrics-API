
import ast
from datetime import datetime
import json
import math
import re

import auth
from dynamodb import insert, select, delete
from Util import Util

def calc_banking(res_pooling_group, year, company_id):

    for pooling_group in res_pooling_group:

        # プーリンググループ内の合計CB
        total_cb = 0
        # 単体でのCBがプラスのものの合計CB
        total_cb_plus = 0
        # 全レコードのリスト
        all_sk_list = []

        imo_list = ast.literal_eval(pooling_group["imo_list"]["S"])
        print(f"imo_list{type(imo_list)}:{(imo_list)}")
        group_name = pooling_group["group_name"]["S"]

        pooling_group_info = company_id + ", " + group_name
        print(f"pooling_group_info:{(pooling_group_info)}")

        for imo in imo_list:
            vessel_year_list = select.get_year_total_by_year(imo, year)

            for year_record in vessel_year_list:
                loop_year_and_ope = year_record["year_and_ope"]["S"]
                loop_cb           = float(year_record["cb"]["S"])   if 'cb' in year_record and year_record["cb"]["S"] != "" else 0.0

                # 去年分のyearレコードを取得する
                last_year_and_ope   = str(int(loop_year_and_ope[0:4]) - 1) + loop_year_and_ope[4:7]
                print(f"last_year_and_ope:{(last_year_and_ope)}")
                last_year_record    = select.get_year_total(imo, last_year_and_ope)
                last_year_banking = float(last_year_record[0]["banking"]) if 'banking' in last_year_record and last_year_record["banking"]["S"] != "" else 0.0

                last_year_borrowing = float(last_year_record[0]["borrowing"]) if 'borrowing' in last_year_record and last_year_record["borrowing"]["S"] != "" else 0.0
                this_year_total_cb  = loop_cb - (last_year_borrowing * 1.1) + last_year_banking

                # 全レコードの情報を入れるリスト
                all_data = {
                    "year_record":        year_record,
                    "this_year_total_cb": this_year_total_cb
                }
                all_sk_list.append(all_data)

                # プラスの場合
                if this_year_total_cb > 0:
                    total_cb_plus += this_year_total_cb

                total_cb += this_year_total_cb

        # プーリンググループ内の合計CBがプラスの場合、プラス分を按分する
        print(f"all_sk_list:{(all_sk_list)}")
        for all_sk in all_sk_list:

            banking_cb = 0
            # プーリンググループのCBがプラスの場合
            if total_cb > 0:
                # 単体のCBがプラスの場合
                if all_sk["this_year_total_cb"] > 0:
                    banking_cb = all_sk["this_year_total_cb"] / total_cb_plus * total_cb
                    insert.upsert_year_total(all_sk["year_record"], banking_cb, pooling_group_info)
                # 単体のCBはゼロ以下→banking=0
                else:
                    insert.upsert_year_total(all_sk["year_record"], banking_cb, pooling_group_info)

            else:
                insert.upsert_year_total(all_sk["year_record"], banking_cb, pooling_group_info)

    print("banking upserting is finished.")
