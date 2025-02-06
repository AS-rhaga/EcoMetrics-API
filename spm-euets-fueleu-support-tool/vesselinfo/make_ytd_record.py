
from dynamodb import select
from Util import Util
from calculate import calculate_function
from vesselinfo import make_eoy_record

def make_recoed(imo, vessel_name, year, para_year, fuel_oil_type_info_list):

    # 必要な変数・リストを作成
    last_year = 0
    voyage_flag = "0"
    speed_flag  = "0"
    penalty_factor = 1.0

    thisyear_year_total_list = []
    operator_total_list      = []
    ytd_grouped_vessel_info  = []
    eoy_grouped_vessel_info  = []

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_imo(imo)

    # FOCFormulas取得
    res_foc_formulas = select.get_foc_formulas(imo)

    # シミュレーション用テーブルを取得
    simulation_plan_voyage_list = select.get_simulation_voyage(imo, year)
    simulation_plan_speed       = select.get_simulation_speed(imo, year)

    # どちらのSimulationを使用しているか確認
    if simulation_plan_voyage_list:
        voyage_flag = simulation_plan_voyage_list[0]["flag"]["S"] if "flag" in simulation_plan_voyage_list[0] else "0"
    
    # speed-planのシミュレーション使用フラグ確認
    if simulation_plan_speed:
        speed_flag = simulation_plan_speed[0]["flag"]["S"] if "flag" in simulation_plan_speed[0] else "0"
        
    # シミュレーションプラン管理用リスト
    ytd_exist_voyage_list     = []
    ytd_not_exist_voyage_list = []
    ytd_exist_speed_list      = []
    ytd_not_exist_speed_list  = []

    # End of Yearの燃料使用料
    total_eoy_hfo = 0
    total_eoy_lfo = 0
    total_eoy_mdo = 0
    total_eoy_mgo = 0
    total_eoy_lng_oms = 0
    total_eoy_lng_oss = 0
    total_eoy_lng_ods = 0
    total_eoy_lpg_p = 0
    total_eoy_lpg_b = 0
    total_eoy_h2_ng = 0
    total_eoy_nh3_ng = 0
    total_eoy_methanol_ng = 0
    total_eoy_nh3_ef = 0
    total_eoy_energy = 0

    # 同一imoのyear-totalリストでループ
    for year_rec in total_year_total_list:

        if year_rec["year_and_ope"]["S"][0:4] == year:
            thisyear_year_total_list.append(year_rec)

        # operator = year_rec["year_and_ope"]["S"][4:50]

        # # 現時点ではオペレーターは複数ないので飛ばす
        # # if operator not in operator_list : # 上限は適当
        #     # オペレーター毎に振り分ける
        #     # operator_list.append(operator)
        #     # index = operator_list.index(operator)

        #     # operator_total_list.append([])
        # operator_total_list.append(year_rec)

    # 各種燃料の消費量と、消費エネルギーの合計値用変数を設定する。
    ytd_lng    = 0
    ytd_hfo    = 0
    ytd_lfo    = 0
    ytd_mdo    = 0
    ytd_mgo    = 0
    ytd_energy = 0

    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:

        # オペレータ
        operator = rec["year_and_ope"]["S"][4:50]

        # 昨年分のレコードを入れるリスト
        last_year_rec = []

        print(f"total_year_total_list:{total_year_total_list}")
        # 同一imoのyear-totalリストでループ
        for year_rec in total_year_total_list:

            tmp_operator = year_rec["year_and_ope"]["S"][4:50]

            # 同一オペレータのレコードを抽出
            if tmp_operator == operator:
                operator_total_list.append(year_rec)

                # 西暦部分の確認、昨年のレコードであれば保持しておく。
                tmp_year = year_rec["year_and_ope"]["S"][0:4]
                if tmp_year == str(int(year) - 1):
                    last_year_rec = year_rec

        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0
        year_count = 0

        for operator_rec in operator_total_list:
            
            # 先頭要素はスキップ（先頭要素は今年のレコード、前年以前を見たいため、スキップで良い）
            if year_count == 0:
                year_count += 1
                continue

            # １年ずつさかのぼる（年が飛んだ時点で確認不要のためbreak）
            if operator_rec["year_and_ope"]["S"][0:4] == str(int(year) - year_count):
                # 罰金フラグの確認
                fine_flag = operator_rec["fine_flag"]["S"]

                if fine_flag == "1":
                    consecutive_years += 1
                else:
                    break
            else:
                break
            
            year_count += 1

        # 今年borrowingができるかどうかのフラグを設定
        thisYear_borrowing = True

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"])
            last_year_borrowing = float(last_year_rec["borrowing"]["S"] if "borrowing" in last_year_rec else "0")

            if last_year_borrowing > 0:
                last_year = last_year_borrowing * (-1.1)
                thisYear_borrowing = False
            elif last_year_banking > 0:
                last_year = last_year_banking
            else:
                last_year = 0

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        operator  = rec["year_and_ope"]["S"][4:50]
        lng       = float(rec["total_lng"]["S"])
        hfo       = float(rec["total_hfo"]["S"])
        lfo       = float(rec["total_lfo"]["S"])
        mdo       = float(rec["total_mdo"]["S"])
        mgo       = float(rec["total_mgo"]["S"])
        distance  = float(rec["distance"]["S"])
        cb        = float(rec["cb"]["S"])
        banking   = float(rec["banking"]["S"])
        borrowing = float(rec["borrowing"]["S"] if "borrowing" in rec else "0")

        # CBから消費量エネルギー（EU Rate考慮済）を算出する
        GHG_Max    = calculate_function.calc_GHG_Max(year)
        GHG_Actual = calculate_function.calc_GHG_Actual(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_info_list)
        energy          = cb / (GHG_Max - GHG_Actual)

        # 必要な計算を行う
        foc             = lng + hfo + lfo + mdo + mgo
        total_cb        = cb + borrowing + banking + last_year
        borrowing_limit = calculate_function.calc_borrowing_limit(thisYear_borrowing, year, energy)
        penalty_factor  = (consecutive_years) / 10 + 1
        print(f"penalty_factor:{penalty_factor}")
        print(f"consecutive_years:{consecutive_years}")

        cost = 0
        if total_cb < 0:
            cost = abs(total_cb) * 2400 * penalty_factor / (GHG_Actual * 41000)

        dataset = {
            "imo"            : imo,
            "vessel_name"    : vessel_name,
            "operator"       : operator,
            "distance"       : distance,
            "foc"            : round(foc),
            "year_to_date"   : round(cb / 1000000, 1),     # 修正。実際のCBを表示するべき
            "last_year"      : round(last_year / 1000000, 1),
            "borrowing_limit": round(borrowing_limit / 1000000),
            "borrowing"      : round(borrowing / 1000000, 1),
            "banking"        : round(banking / 1000000, 1),
            "total"          : round((total_cb + last_year) / 1000000, 1),
            "penalty_factor" : penalty_factor,
            "cost"           : round(cost)
        }

        ytd_grouped_vessel_info.append(dataset)

        # 合計用変数に加算する。
        ytd_lng    += lng
        ytd_hfo    += hfo
        ytd_lfo    += lfo
        ytd_mdo    += mdo
        ytd_mgo    += mgo
        ytd_energy += energy

        # シミュレーション結果を反映したEnd of Yearのデータを作成する。

        # voyage-planのシミュレーション使用フラグ確認
        if voyage_flag == "1":

            # voyageのオペレーターのオペレーターを確認する
            for simulation_voyage in simulation_plan_voyage_list:
                simulation_operator = simulation_voyage["operator"]["S"]

                # 基準のyear-totalレコードのオペレーターと一致する場合
                if simulation_operator == operator:
                    # 実測データが存在するリストに追加
                    ytd_exist_voyage_list.append(simulation_voyage)
                    # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                    simulation_plan_voyage_list.remove(simulation_voyage)
        
        # speed-planのシミュレーション使用フラグ確認
        if speed_flag == "1":

            # オペレーターのオペレーターを確認する
            simulation_operator = simulation_plan_speed[0]["operator"]["S"]

            # 基準のyear-totalレコードのオペレーターと一致する場合
            if simulation_operator == operator:
                # 実測データが存在するリストに追加
                ytd_exist_speed_list.append(simulation_plan_speed[0])
                # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                simulation_plan_speed.remove(simulation_plan_speed[0])


        print(f"voyage_flag:{voyage_flag}")
        print(f"simulation_plan_voyage_list:{simulation_plan_voyage_list}")        
        print(f"ytd_exist_voyage_list:{ytd_exist_voyage_list}")


        # シミュレーションリストの処理
        # 実測データ有りvoyage-plan
        if len(ytd_exist_voyage_list) > 0:
            for i in range(len(ytd_exist_voyage_list)):
                eoy_grouped_vessel_data, total_fuel_list = make_eoy_record.make_voyage_plans_data(imo, vessel_name, rec, ytd_exist_voyage_list, res_foc_formulas, fuel_oil_type_info_list, penalty_factor, last_year, energy)
                eoy_grouped_vessel_info.append(eoy_grouped_vessel_data)

                # End of Yearの燃料消費量Total値に合算
                total_eoy_hfo += total_fuel_list["eoy_hfo"]
                total_eoy_lfo += total_fuel_list["eoy_lfo"]
                total_eoy_mdo += total_fuel_list["eoy_mdo"]
                total_eoy_mgo += total_fuel_list["eoy_mgo"]
                total_eoy_lng_oms += total_fuel_list["eoy_lng_oms"]
                total_eoy_lng_oss += total_fuel_list["eoy_lng_oss"]
                total_eoy_lng_ods += total_fuel_list["eoy_lng_ods"]
                total_eoy_lpg_p += total_fuel_list["eoy_lpg_p"]
                total_eoy_lpg_b += total_fuel_list["eoy_lpg_b"]
                total_eoy_h2_ng += total_fuel_list["eoy_h2_ng"]
                total_eoy_nh3_ng += total_fuel_list["eoy_nh3_ng"]
                total_eoy_methanol_ng += total_fuel_list["eoy_methanol_ng"]
                total_eoy_nh3_ef += total_fuel_list["eoy_nh3_ef"]
                total_eoy_energy += total_fuel_list["eoy_energy"]

        # 実測データ有りspeed-plan
        elif len(ytd_exist_speed_list) > 0:
            eoy_grouped_vessel_data, total_fuel_list = make_eoy_record.make_speed_plans_data(imo, vessel_name, year, rec, ytd_exist_speed_list, res_foc_formulas, fuel_oil_type_info_list, penalty_factor, last_year, energy)
            eoy_grouped_vessel_info.append(eoy_grouped_vessel_data)

            # End of Yearの燃料消費量Total値に合算
            total_eoy_hfo += total_fuel_list["eoy_hfo"]
            total_eoy_lfo += total_fuel_list["eoy_lfo"]
            total_eoy_mdo += total_fuel_list["eoy_mdo"]
            total_eoy_mgo += total_fuel_list["eoy_mgo"]
            total_eoy_lng_oms += total_fuel_list["eoy_lng_oms"]
            total_eoy_lng_oss += total_fuel_list["eoy_lng_oss"]
            total_eoy_lng_ods += total_fuel_list["eoy_lng_ods"]
            total_eoy_lpg_p += total_fuel_list["eoy_lpg_p"]
            total_eoy_lpg_b += total_fuel_list["eoy_lpg_b"]
            total_eoy_h2_ng += total_fuel_list["eoy_h2_ng"]
            total_eoy_nh3_ng += total_fuel_list["eoy_nh3_ng"]
            total_eoy_methanol_ng += total_fuel_list["eoy_methanol_ng"]
            total_eoy_nh3_ef += total_fuel_list["eoy_nh3_ef"]
            total_eoy_energy += total_fuel_list["eoy_energy"]

        # 実測データ有り かつ シミュレーションなし
        else:
            
            # End of Yearには、Year to Dateと同じ値を設定
            dataset = {
                "imo"                : imo,
                "vessel_name"        : vessel_name,
                "operator"           : operator,
                "distance"           : distance,
                "foc"                : round(foc),
                "end_of_year"        : round(cb / 1000000, 1),
                "last_year"          : round(last_year / 1000000, 1),
                "borrowing_limit"    : round(borrowing_limit / 1000000, 1),
                "borrowing"          : round(borrowing / 1000000, 1),
                "banking"            : round(banking / 1000000, 1),
                "total"              : round((total_cb + last_year) / 1000000, 1),
                "penalty_factor"     : penalty_factor,
                "cost"               : round(cost)
            }
            eoy_grouped_vessel_info.append(dataset)

    # year-totalループ終了後、各シミュレーションリストに残っているものは、実測データ無しオペレーター
    if voyage_flag == "1" and len(simulation_plan_voyage_list) > 0:
        for simulation_voyage in simulation_plan_voyage_list:
            # 実測データが存在しないリストに追加
            ytd_not_exist_voyage_list.append(simulation_voyage)
    if speed_flag == "1" and len(simulation_plan_speed) > 0:
        ytd_not_exist_speed_list.append(simulation_plan_speed[0])
    
    # 実測データ無しvoyage-plan
    if len(ytd_not_exist_voyage_list) > 0:
        eoy_grouped_vessel_data, total_fuel_list = make_eoy_record.make_voyage_plans_data(imo, vessel_name, None, ytd_not_exist_voyage_list, res_foc_formulas, fuel_oil_type_info_list, penalty_factor, last_year, energy)
        eoy_grouped_vessel_info.append(eoy_grouped_vessel_data)

        print(f"実測データ無しvoyage-planのtotal_fuel_list:{(total_fuel_list)}")

        # End of Yearの燃料消費量Total値に合算
        total_eoy_hfo += total_fuel_list["eoy_hfo"]
        total_eoy_lfo += total_fuel_list["eoy_lfo"]
        total_eoy_mdo += total_fuel_list["eoy_mdo"]
        total_eoy_mgo += total_fuel_list["eoy_mgo"]
        total_eoy_lng_oms += total_fuel_list["eoy_lng_oms"]
        total_eoy_lng_oss += total_fuel_list["eoy_lng_oss"]
        total_eoy_lng_ods += total_fuel_list["eoy_lng_ods"]
        total_eoy_lpg_p += total_fuel_list["eoy_lpg_p"]
        total_eoy_lpg_b += total_fuel_list["eoy_lpg_b"]
        total_eoy_h2_ng += total_fuel_list["eoy_h2_ng"]
        total_eoy_nh3_ng += total_fuel_list["eoy_nh3_ng"]
        total_eoy_methanol_ng += total_fuel_list["eoy_methanol_ng"]
        total_eoy_nh3_ef += total_fuel_list["eoy_nh3_ef"]
        total_eoy_energy += total_fuel_list["eoy_energy"]

        # 空のytd_grouped_vessel_dataレコードを追加
        ytd_grouped_vessel_data_zero = {
            "imo"            : imo,
            "vessel_name"    : vessel_name,
            "operator"       : ytd_not_exist_voyage_list[0]["operator"]["S"],
            "distance"       : 0,
            "foc"            : 0,
            "year_to_date"   : 0,
            "last_year"      : 0,
            "borrowing_limit": 0,
            "borrowing"      : 0,
            "banking"        : 0,
            "total"          : 0,
            "penalty_factor" : eoy_grouped_vessel_data["penalty_factor"],
            "cost"           : 0
        }
        ytd_grouped_vessel_info.append(ytd_grouped_vessel_data_zero)

    # 実測データ無しspeed-plan
    elif len(ytd_not_exist_speed_list) > 0:
        eoy_grouped_vessel_data, total_fuel_list = make_eoy_record.make_speed_plans_data(imo, vessel_name, year, rec, ytd_not_exist_speed_list, res_foc_formulas, fuel_oil_type_info_list, penalty_factor, last_year, energy)
        eoy_grouped_vessel_info.append(eoy_grouped_vessel_data)

        # End of Yearの燃料消費量Total値に合算
        total_eoy_hfo += total_fuel_list["eoy_hfo"]
        total_eoy_lfo += total_fuel_list["eoy_lfo"]
        total_eoy_mdo += total_fuel_list["eoy_mdo"]
        total_eoy_mgo += total_fuel_list["eoy_mgo"]
        total_eoy_lng_oms += total_fuel_list["eoy_lng_oms"]
        total_eoy_lng_oss += total_fuel_list["eoy_lng_oss"]
        total_eoy_lng_ods += total_fuel_list["eoy_lng_ods"]
        total_eoy_lpg_p += total_fuel_list["eoy_lpg_p"]
        total_eoy_lpg_b += total_fuel_list["eoy_lpg_b"]
        total_eoy_h2_ng += total_fuel_list["eoy_h2_ng"]
        total_eoy_nh3_ng += total_fuel_list["eoy_nh3_ng"]
        total_eoy_methanol_ng += total_fuel_list["eoy_methanol_ng"]
        total_eoy_nh3_ef += total_fuel_list["eoy_nh3_ef"]
        total_eoy_energy += total_fuel_list["eoy_energy"]

        # 空のytd_grouped_vessel_dataレコードを追加
        ytd_grouped_vessel_data_zero = {
            "imo"            : imo,
            "vessel_name"    : vessel_name,
            "operator"       : ytd_not_exist_speed_list[0]["operator"]["S"],
            "distance"       : 0,
            "foc"            : 0,
            "year_to_date"   : 0,
            "last_year"      : 0,
            "borrowing_limit": 0,
            "borrowing"      : 0,
            "banking"        : 0,
            "total"          : 0,
            "penalty_factor" : eoy_grouped_vessel_data["penalty_factor"],
            "cost"           : 0
        }
        ytd_grouped_vessel_info.append(ytd_grouped_vessel_data_zero)

    # 実測値なし、シミュレーションもなし
    if len(ytd_grouped_vessel_info) == 0:
        
        dataset_ytd = {
            "imo"                : imo,
            "vessel_name"        : vessel_name,
            "operator"           : "",
            "distance"           : 0,
            "foc"                : 0,
            "year_to_date"       : 0,
            "last_year"          : 0,
            "borrowing_limit"    : 0,
            "borrowing"          : 0,
            "banking"            : 0,
            "total"              : 0,
            "penalty_factor"     : 1.0,
            "cost"               : 0
        }
        ytd_grouped_vessel_info.append(dataset_ytd)
        
        dataset_eoy = {
            "imo"                : imo,
            "vessel_name"        : vessel_name,
            "operator"           : "",
            "distance"           : 0,
            "foc"                : 0,
            "end_of_year"        : 0,
            "last_year"          : 0,
            "borrowing_limit"    : 0,
            "borrowing"          : 0,
            "banking"            : 0,
            "total"              : 0,
            "penalty_factor"     : 1.0,
            "cost"               : 0
        }
        eoy_grouped_vessel_info.append(dataset_eoy)

    return ytd_grouped_vessel_info, ytd_lng, ytd_hfo, ytd_lfo, ytd_mdo, ytd_mgo, ytd_energy, eoy_grouped_vessel_info, total_eoy_hfo, total_eoy_lfo, total_eoy_mdo, total_eoy_mgo, total_eoy_lng_oms, total_eoy_lng_oss, total_eoy_lng_ods, total_eoy_lpg_p, total_eoy_lpg_b, total_eoy_h2_ng, total_eoy_nh3_ng, total_eoy_methanol_ng, total_eoy_nh3_ef, total_eoy_energy

# 前年以前のデータ取得
def make_recoed_past(imo, vessel_name, year, fuel_oil_type_list):

    # 必要な変数・リストを作成
    last_year = 0

    thisyear_year_total_list = []
    operator_total_list      = []
    ytd_grouped_vessel_info  = []
    eoy_grouped_vessel_info  = []

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_imo(imo)

    # 同一imoのyear-totalリストでループ
    for year_rec in total_year_total_list:

        if year_rec["year_and_ope"]["S"][0:4] == year:
            thisyear_year_total_list.append(year_rec)

        # operator = year_rec["year_and_ope"]["S"][4:50]

        # # 現時点ではオペレーターは複数ないので飛ばす
        # # if operator not in operator_list : # 上限は適当
        #     # オペレーター毎に振り分ける
        #     # operator_list.append(operator)
        #     # index = operator_list.index(operator)

        #     # operator_total_list.append([])
        # operator_total_list.append(year_rec)

    total_lng = 0
    total_hfo = 0
    total_lfo = 0
    total_mdo = 0
    total_mgo = 0
    total_energy = 0
        
    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:

        operator = rec["year_and_ope"]["S"][4:50]

        # 昨年分のレコードを入れるリスト
        last_year_rec = []

        print(f"total_year_total_list:{total_year_total_list}")
        # 同一imoのyear-totalリストでループ
        for year_rec in total_year_total_list:

            tmp_operator = year_rec["year_and_ope"]["S"][4:50]

            # 同一オペレータのレコードを抽出
            if tmp_operator == operator:
                operator_total_list.append(year_rec)

                # 西暦部分の確認、昨年のレコードであれば保持しておく。
                tmp_year = year_rec["year_and_ope"]["S"][0:4]
                if tmp_year == str(int(year) - 1):
                    last_year_rec = year_rec
        
        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0
        year_count = 0

        for operator_rec in operator_total_list:
            
            # 先頭要素はスキップ（先頭要素は今年のレコード、前年以前を見たいため、スキップで良い）
            if year_count == 0:
                year_count += 1
                continue

            # １年ずつさかのぼる（年が飛んだ時点で確認不要のためbreak）
            if operator_rec["year_and_ope"]["S"][0:4] == str(int(year) - year_count):
                # 罰金フラグの確認
                fine_flag = operator_rec["fine_flag"]["S"]

                if fine_flag == "1":
                    consecutive_years += 1
                else:
                    break
            else:
                break
            
            year_count += 1

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"])
            last_year_borrowing = float(last_year_rec["borrowing"]["S"] if "borrowing" in last_year_rec else "0")


            if last_year_borrowing > 0:
                last_year += last_year_borrowing * (-1.1)
                thisYear_borrowing = False
            elif last_year_banking > 0:
                last_year += last_year_borrowing
            else:
                last_year += 0

        # 各項目を取得する
        operator  = rec["year_and_ope"]["S"][4:50]
        lng       = float(rec["total_lng"]["S"])
        hfo       = float(rec["total_hfo"]["S"])
        lfo       = float(rec["total_lfo"]["S"])
        mdo       = float(rec["total_mdo"]["S"])
        mgo       = float(rec["total_mgo"]["S"])
        distance  = float(rec["distance"]["S"])
        cb        = float(rec["cb"]["S"])
        banking   = float(rec["banking"]["S"])
        borrowing = float(rec["borrowing"]["S"] if "borrowing" in rec else "0")

        # CBから消費量エネルギー（EU Rate考慮済）を算出する
        GHG_Max    = calculate_function.calc_GHG_Max(year)
        GHG_Actual = calculate_function.calc_GHG_Actual(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)
        energy          = cb / (GHG_Max - GHG_Actual)

        # total値に加算
        total_lng += float(lng)
        total_hfo += float(hfo)
        total_lfo += float(lfo)
        total_mdo += float(mdo)
        total_mgo += float(mgo)
        total_energy += energy

        # 必要な計算を行う
        foc             = lng + hfo + lfo + mdo + mgo
        total_cb        = cb + borrowing + banking + last_year
        penalty_factor  = (consecutive_years) / 10 + 1

        cb_cost = 0
        if total_cb < 0:
            cb_cost = abs(total_cb) * 2400 * penalty_factor / (GHG_Actual * 41000)

        borrowing_limit = calculate_function.calc_borrowing_limit(True, year, energy)

        # 全てゼロのytdデータを合わせて、データセットを作成
        # End of Yearには、Year to Dateと同じ値を設定
        dataset = {
            "imo"                : imo,
            "vessel_name"        : vessel_name,
            "operator"           : operator,
            "distance"           : distance,
            "foc"                : round(foc),
            "end_of_year"        : round(total_cb / 1000000, 1),
            "last_year"          : round(last_year / 1000000, 1),
            "borrowing_limit"    : round(borrowing_limit / 1000000, 1),
            "borrowing"          : round(borrowing / 1000000, 1),
            "banking"            : round(banking / 1000000, 1),
            "total"              : round((cb + last_year) / 1000000, 1),
            "penalty_factor"     : penalty_factor,
            "cost"               : round(cb_cost)
        }
        eoy_grouped_vessel_info.append(dataset)

        # ytdとeoyで同じ数の要素が入っている必要があるため、ytdにも設定（過去年を指定され場合、当年データは画面には表示しない）
        dataset_all_zero = {
            "imo"                : imo,
            "vessel_name"        : vessel_name,
            "operator"           : operator,
            "distance"           : 0,
            "foc"                : 0,
            "year_to_date"       : 0,
            "last_year"          : 0,
            "borrowing_limit"    : 0,
            "borrowing"          : 0,
            "banking"            : 0,
            "total"              : 0,
            "penalty_factor"     : 1.0,
            "cost"               : 0
        }
        ytd_grouped_vessel_info.append(dataset_all_zero)

    if len(eoy_grouped_vessel_info) == 0:
        # yearテーブルが取れない場合（その年の航行実績なしの場合）、ALL0のデータをセット
        ytd_dataset = {
            "imo"                : imo,
            "vessel_name"        : vessel_name,
            "operator"           : "",
            "distance"           : 0,
            "foc"                : 0,
            "year_to_date"       : 0,
            "last_year"          : 0,
            "borrowing_limit"    : 0,
            "borrowing"          : 0,
            "banking"            : 0,
            "total"              : 0,
            "penalty_factor"     : 1.0,
            "cost"               : 0
        }
        ytd_grouped_vessel_info.append(ytd_dataset)

        eoy_dataset = {
            "imo"                : imo,
            "vessel_name"        : vessel_name,
            "operator"           : "",
            "distance"           : 0,
            "foc"                : 0,
            "end_of_year"        : 0,
            "last_year"          : 0,
            "borrowing_limit"    : 0,
            "borrowing"          : 0,
            "banking"            : 0,
            "total"              : 0,
            "penalty_factor"     : 1.0,
            "cost"               : 0
        }
        eoy_grouped_vessel_info.append(eoy_dataset)

    return ytd_grouped_vessel_info, 0, 0, 0, 0, 0, 0, eoy_grouped_vessel_info, total_hfo, total_lfo, total_mdo, total_mgo, total_lng, 0, 0, 0, 0, 0, 0, 0, 0, total_energy