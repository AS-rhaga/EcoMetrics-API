import json
import ast
from datetime import datetime, timedelta
from decimal import Decimal


from poseidon import dynamodb
from poseidon.Util.VesselMaster import VesselMaster
from poseidon.Util.VesselAlerm import VesselAlerm
from poseidon.Util.DataChannel import DataChannel
from poseidon.Util import Util


def create_responce_list(imo, response, DATACHANNEL):
    print(f"response[{type(response)}]: {response}")
    
    beaufort_count = 0
    course_count = 0
    displacement_count = 0
    dwt_count = 0
    gt_count = 0
    lat_count = 0
    lng_count = 0
    log_distance_count = 0
    log_speed_count = 0
    me_bog_count = 0
    me_hfo_count = 0
    me_lsfo_count = 0
    me_do_count = 0
    me_lsgo_count = 0
    dg_bog_count = 0
    dg_hfo_count = 0
    dg_lsfo_count = 0
    dg_do_count = 0
    dg_lsgo_count = 0
    boiler_hfo_count = 0
    boiler_lsfo_count = 0
    boiler_do_count = 0
    boiler_lsgo_count = 0
    igg_go_count = 0
    igg_lsgo_count = 0
    gcu_bog_count = 0
    me_foc_count = 0
    me_load_count = 0
    me_rpm_count = 0
    og_distance_count = 0
    og_speed_count = 0
    swell_direction_count = 0
    swell_height_count = 0
    swell_period_count = 0
    total_foc_count = 0
    wave_direction_count = 0
    wave_height_count = 0
    wave_period_count = 0
    wind_direction_count = 0
    wind_speed_count = 0

    
    # imo = DATACHANNEL["imo"]
    # rev = DATACHANNEL["rev"]
    lower_beaufort = float(DATACHANNEL["lower_beaufort"]) if DATACHANNEL["lower_beaufort"] != "" else -9999999
    lower_boiler_foc = float(DATACHANNEL["lower_boiler_foc"]) if DATACHANNEL["lower_boiler_foc"] != "" else -9999999
    lower_course = float(DATACHANNEL["lower_course"]) if DATACHANNEL["lower_course"] != "" else -9999999
    lower_displacement = float(DATACHANNEL["lower_displacement"]) if DATACHANNEL["lower_displacement"] != "" else -9999999
    lower_dwt = float(DATACHANNEL["lower_dwt"]) if DATACHANNEL["lower_dwt"] != "" else -9999999
    lower_ge_foc = float(DATACHANNEL["lower_ge_foc"]) if DATACHANNEL["lower_ge_foc"] != "" else -9999999
    lower_gt = float(DATACHANNEL["lower_gt"]) if DATACHANNEL["lower_gt"] != "" else -9999999
    lower_lat = float(DATACHANNEL["lower_lat"]) if DATACHANNEL["lower_lat"] != "" else -9999999
    lower_lng = float(DATACHANNEL["lower_lng"]) if DATACHANNEL["lower_lng"] != "" else -9999999
    lower_log_distance = float(DATACHANNEL["lower_log_distance"]) if DATACHANNEL["lower_log_distance"] != "" else -9999999
    lower_log_speed = float(DATACHANNEL["lower_log_speed"]) if DATACHANNEL["lower_log_speed"] != "" else -9999999
    lower_me_bog  = float(DATACHANNEL["lower_me_bog"])  if DATACHANNEL["lower_me_bog"] != "" else -9999999
    lower_me_hfo  = float(DATACHANNEL["lower_me_hfo"])  if DATACHANNEL["lower_me_hfo"] != "" else -9999999
    lower_me_lsfo = float(DATACHANNEL["lower_me_lsfo"]) if DATACHANNEL["lower_me_lsfo"] != "" else -9999999
    lower_me_do   = float(DATACHANNEL["lower_me_do"])   if DATACHANNEL["lower_me_do"] != "" else -9999999
    lower_me_lsgo = float(DATACHANNEL["lower_me_lsgo"]) if DATACHANNEL["lower_me_lsgo"] != "" else -9999999
    lower_dg_bog  = float(DATACHANNEL["lower_dg_bog"])  if DATACHANNEL["lower_dg_bog"] != "" else -9999999
    lower_dg_hfo  = float(DATACHANNEL["lower_dg_hfo"])  if DATACHANNEL["lower_dg_hfo"] != "" else -9999999
    lower_dg_do   = float(DATACHANNEL["lower_dg_do"])   if DATACHANNEL["lower_dg_do"] != "" else -9999999
    lower_dg_lsgo = float(DATACHANNEL["lower_dg_lsgo"]) if DATACHANNEL["lower_dg_lsgo"] != "" else -9999999
    lower_boiler_hfo  = float(DATACHANNEL["lower_boiler_hfo"])  if DATACHANNEL["lower_boiler_hfo"] != "" else -9999999
    lower_boiler_do   = float(DATACHANNEL["lower_boiler_do"])   if DATACHANNEL["lower_boiler_do"] != "" else -9999999
    lower_boiler_lsgo = float(DATACHANNEL["lower_boiler_lsgo"]) if DATACHANNEL["lower_boiler_lsgo"] != "" else -9999999
    lower_igg_go   = float(DATACHANNEL["lower_igg_go"])   if DATACHANNEL["lower_igg_go"] != "" else -9999999
    lower_igg_lsgo = float(DATACHANNEL["lower_igg_lsgo"]) if DATACHANNEL["lower_igg_lsgo"] != "" else -9999999
    lower_gcu_bog  = float(DATACHANNEL["lower_gcu_bog"])  if DATACHANNEL["lower_gcu_bog"] != "" else -9999999
    lower_me_foc = float(DATACHANNEL["lower_me_foc"]) if DATACHANNEL["lower_me_foc"] != "" else -9999999
    lower_me_load = float(DATACHANNEL["lower_me_load"]) if DATACHANNEL["lower_me_load"] != "" else -9999999
    lower_me_rpm = float(DATACHANNEL["lower_me_rpm"]) if DATACHANNEL["lower_me_rpm"] != "" else -9999999
    lower_og_distance = float(DATACHANNEL["lower_og_distance"]) if DATACHANNEL["lower_og_distance"] != "" else -9999999
    lower_og_speed = float(DATACHANNEL["lower_og_speed"]) if DATACHANNEL["lower_og_speed"] != "" else -9999999
    lower_swell_direction = float(DATACHANNEL["lower_swell_direction"]) if DATACHANNEL["lower_swell_direction"] != "" else -9999999
    lower_swell_height = float(DATACHANNEL["lower_swell_height"]) if DATACHANNEL["lower_swell_height"] != "" else -9999999
    lower_swell_period = float(DATACHANNEL["lower_swell_period"]) if DATACHANNEL["lower_total_foc"] != "" else -9999999
    lower_total_foc = float(DATACHANNEL["lower_total_foc"]) if DATACHANNEL["lower_beaufort"] != "" else -9999999
    lower_wave_direction = float(DATACHANNEL["lower_wave_direction"]) if DATACHANNEL["lower_wave_direction"] != "" else -9999999
    lower_wave_height = float(DATACHANNEL["lower_wave_height"]) if DATACHANNEL["lower_wave_height"] != "" else -9999999
    lower_wave_period = float(DATACHANNEL["lower_wave_period"]) if DATACHANNEL["lower_wave_period"] != "" else -9999999
    lower_wind_direction = float(DATACHANNEL["lower_wind_direction"]) if DATACHANNEL["lower_wind_direction"] != "" else -9999999
    lower_wind_speed = float(DATACHANNEL["lower_wind_speed"]) if DATACHANNEL["lower_wind_speed"] != "" else -9999999
    
    upper_beaufort = float(DATACHANNEL["upper_beaufort"]) if DATACHANNEL["upper_beaufort"] != "" else 9999999
    upper_boiler_foc = float(DATACHANNEL["upper_boiler_foc"]) if DATACHANNEL["upper_boiler_foc"] != "" else 9999999
    upper_course = float(DATACHANNEL["upper_course"]) if DATACHANNEL["upper_course"] != "" else 9999999
    upper_displacement = float(DATACHANNEL["upper_displacement"]) if DATACHANNEL["upper_displacement"] != "" else 9999999
    upper_dwt = float(DATACHANNEL["upper_dwt"]) if DATACHANNEL["upper_dwt"] != "" else 9999999
    upper_ge_foc = float(DATACHANNEL["upper_ge_foc"]) if DATACHANNEL["upper_ge_foc"] != "" else 9999999
    upper_gt = float(DATACHANNEL["upper_gt"]) if DATACHANNEL["upper_gt"] != "" else 9999999
    upper_lat = float(DATACHANNEL["upper_lat"]) if DATACHANNEL["upper_lat"] != "" else 9999999
    upper_lng = float(DATACHANNEL["upper_lng"]) if DATACHANNEL["upper_lng"] != "" else 9999999
    upper_log_distance = float(DATACHANNEL["upper_log_distance"]) if DATACHANNEL["upper_log_distance"] != "" else 9999999
    upper_log_speed = float(DATACHANNEL["upper_log_speed"]) if DATACHANNEL["upper_log_speed"] != "" else 9999999
    upper_me_bog  = float(DATACHANNEL["upper_me_bog"])  if DATACHANNEL["upper_me_bog"] != "" else 9999999
    upper_me_hfo  = float(DATACHANNEL["upper_me_hfo"])  if DATACHANNEL["upper_me_hfo"] != "" else 9999999
    upper_me_lsfo = float(DATACHANNEL["upper_me_lsfo"]) if DATACHANNEL["upper_me_lsfo"] != "" else 9999999
    upper_me_do   = float(DATACHANNEL["upper_me_do"])   if DATACHANNEL["upper_me_do"] != "" else 9999999
    upper_me_lsgo = float(DATACHANNEL["upper_me_lsgo"]) if DATACHANNEL["upper_me_lsgo"] != "" else 9999999
    upper_dg_bog  = float(DATACHANNEL["upper_dg_bog"])  if DATACHANNEL["upper_dg_bog"] != "" else 9999999
    upper_dg_hfo  = float(DATACHANNEL["upper_dg_hfo"])  if DATACHANNEL["upper_dg_hfo"] != "" else 9999999
    upper_dg_do   = float(DATACHANNEL["upper_dg_do"])   if DATACHANNEL["upper_dg_do"] != "" else 9999999
    upper_dg_lsgo = float(DATACHANNEL["upper_dg_lsgo"]) if DATACHANNEL["upper_dg_lsgo"] != "" else 9999999
    upper_boiler_hfo  = float(DATACHANNEL["upper_boiler_hfo"])  if DATACHANNEL["upper_boiler_hfo"] != "" else 9999999
    upper_boiler_do   = float(DATACHANNEL["upper_boiler_do"])   if DATACHANNEL["upper_boiler_do"] != "" else 9999999
    upper_boiler_lsgo = float(DATACHANNEL["upper_boiler_lsgo"]) if DATACHANNEL["upper_boiler_lsgo"] != "" else 9999999
    upper_igg_go   = float(DATACHANNEL["upper_igg_go"])   if DATACHANNEL["upper_igg_go"] != "" else 9999999
    upper_igg_lsgo = float(DATACHANNEL["upper_igg_lsgo"]) if DATACHANNEL["upper_igg_lsgo"] != "" else 9999999
    upper_gcu_bog  = float(DATACHANNEL["upper_gcu_bog"])  if DATACHANNEL["upper_gcu_bog"] != "" else 9999999
    upper_me_foc = float(DATACHANNEL["upper_me_foc"]) if DATACHANNEL["upper_me_foc"] != "" else 9999999
    upper_me_load = float(DATACHANNEL["upper_me_load"]) if DATACHANNEL["upper_me_load"] != "" else 9999999
    upper_me_rpm = float(DATACHANNEL["upper_me_rpm"]) if DATACHANNEL["upper_me_rpm"] != "" else 9999999
    upper_og_distance = float(DATACHANNEL["upper_og_distance"]) if DATACHANNEL["upper_og_distance"] != "" else 9999999
    upper_og_speed = float(DATACHANNEL["upper_og_speed"]) if DATACHANNEL["upper_og_speed"] != "" else 9999999
    upper_swell_direction = float(DATACHANNEL["upper_swell_direction"]) if DATACHANNEL["upper_swell_direction"] != "" else 9999999
    upper_swell_height = float(DATACHANNEL["upper_swell_height"]) if DATACHANNEL["upper_swell_height"] != "" else 9999999
    upper_swell_period = float(DATACHANNEL["upper_swell_period"]) if DATACHANNEL["upper_swell_period"] != "" else 9999999
    upper_total_foc = float(DATACHANNEL["upper_total_foc"]) if DATACHANNEL["upper_total_foc"] != "" else 9999999
    upper_wave_direction = float(DATACHANNEL["upper_wave_direction"]) if DATACHANNEL["upper_wave_direction"] != "" else 9999999
    upper_wave_height = float(DATACHANNEL["upper_wave_height"]) if DATACHANNEL["upper_wave_height"] != "" else 9999999
    upper_wave_period = float(DATACHANNEL["upper_wave_period"]) if DATACHANNEL["upper_wave_period"] != "" else 9999999
    upper_wind_direction = float(DATACHANNEL["upper_wind_direction"]) if DATACHANNEL["upper_wind_direction"] != "" else 9999999
    upper_wind_speed = float(DATACHANNEL["upper_wind_speed"]) if DATACHANNEL["upper_wind_speed"] != "" else 9999999
    
    VALUE_LIST = []
    RESPONSE_LIST = []
    
    # DynamoDBから取得してきたレコードをリストに移管
    res_count = 0
    for res in response:
        res_count += 1
        # print(f"res[{type(len(res))}][{len(res)}]: {res}")
        
        # SPAS仕様----------------------------------------------------------------------
        # 文字列項目、上下限チェック不要
        local_date      = res["local_date"]["S"] if "local_date" in res and res["local_date"]["S"] != "" else ""
        if "timestamp" in res and res["timestamp"]["S"] != "":
            # 元のタイムスタンプ
            timestamp = "2024-07-09T03:00:00Z"
            timestamp = res["timestamp"]["S"]
            
            # タイムスタンプをdatetimeオブジェクトに変換
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
            
            # 希望の形式に変換
            utc_date = dt.strftime("%Y/%m/%d %H:%M")
        else:
            utc_date = ""
            
        state           = res["state"]["S"] if "state" in res and res["state"]["S"] != "" else ""
        port_name       = res["port_name"]["S"] if "port_name" in res and res["port_name"]["S"] != "" else ""
        
        # 数値項目、上下限チェック不要
        voyage_no       = res["voyage_no"]["S"] if "voyage_no" in res and res["voyage_no"]["S"] != "" else ""
        leg_no          = res["leg_no"]["S"] if "leg_no" in res and res["leg_no"]["S"] != "" else ""
        leg             = res["leg"]["S"] if "leg" in res and res["leg"]["S"] != "" else ""
        eta_local_date  = res["eta_local_date"]["S"] if "eta_local_date" in res and res["eta_local_date"]["S"] != "" else ""
        eta_utc_date    = res["eta_utc_date"]["S"] if "eta_utc_date" in res and res["eta_utc_date"]["S"] != "" else ""
        eta_destination = res["eta_destination"]["S"] if "eta_destination" in res and res["eta_destination"]["S"] != "" else ""
        ablog_id        = res["ablog_id"]["S"] if "ablog_id" in res and res["ablog_id"]["S"] != "" else ""
        co2_factor      = float(res["co2_factor"]["S"]) if "co2_factor" in res and res["co2_factor"]["S"] != "" else 1
        start_local_date = res["start_local_date"]["S"] if "start_local_date" in res and res["start_local_date"]["S"] != "" else ""
        start_utc_date   = res["start_utc_date"]["S"] if "start_utc_date" in res and res["start_utc_date"]["S"] != "" else ""
        port_code        = res["port_code"]["S"] if "port_code" in res and res["port_code"]["S"] != "" else ""
        eta_port_code    = res["eta_port_code"]["S"] if "eta_port_code" in res and res["eta_port_code"]["S"] != "" else ""
        operator         = res["operator"]["S"] if "operator" in res and res["operator"]["S"] != "" else ""

        # 数値項目、上下限チェック開始
        beaufort = ""
        course = ""
        displacement = ""
        dwt = ""
        gt = ""
        lat = ""
        lng = ""
        log_distance = ""
        log_speed = ""
        me_bog = ""
        me_hfo = ""
        me_lsfo = ""
        me_do = ""
        me_lsgo = ""
        dg_bog = ""
        dg_hfo = ""
        dg_lsfo = ""
        dg_do = ""
        dg_lsgo = ""
        boiler_hfo = ""
        boiler_lsfo = ""
        boiler_do = ""
        boiler_lsgo = ""
        igg_go = ""
        igg_lsgo = ""
        gcu_bog = ""
        me_foc = ""
        me_load = ""
        me_rpm = ""
        og_distance = ""
        og_speed = ""
        swell_direction = ""
        swell_height = ""
        swell_period = ""
        total_foc = ""
        wave_direction = ""
        wave_height = ""
        wave_period = ""
        wind_direction = ""
        wind_speed = ""
        
        beaufort_alerm = 0
        course_alerm = 0
        displacement_alerm = 0
        dwt_alerm = 0
        gt_alerm = 0
        lat_alerm = 0
        lng_alerm = 0
        log_distance_alerm = 0
        log_speed_alerm = 0
        me_bog_alerm = 0
        me_hfo_alerm = 0
        me_lsfo_alerm = 0
        me_do_alerm = 0
        me_lsgo_alerm = 0
        dg_bog_alerm = 0
        dg_hfo_alerm = 0
        dg_lsfo_alerm = 0
        dg_do_alerm = 0
        dg_lsgo_alerm = 0
        boiler_hfo_alerm = 0
        boiler_lsfo_alerm = 0
        boiler_do_alerm = 0
        boiler_lsgo_alerm = 0
        igg_go_alerm = 0
        igg_lsgo_alerm = 0
        gcu_bog_alerm = 0
        me_foc_alerm = 0
        me_load_alerm = 0
        me_rpm_alerm = 0
        og_distance_alerm = 0
        og_speed_alerm = 0
        swell_direction_alerm = 0
        swell_height_alerm = 0
        swell_period_alerm = 0
        total_foc_alerm = 0
        wave_direction_alerm = 0
        wave_height_alerm = 0
        wave_period_alerm = 0
        wind_direction_alerm = 0
        wind_speed_alerm = 0

        if "beaufort" in res and res["beaufort"]["S"] != "": 
            beaufort = float(res["beaufort"]["S"]) 
            if beaufort < lower_beaufort or upper_beaufort < beaufort: 
                beaufort_count += 1
                beaufort_alerm = 1
        if "course" in res and res["course"]["S"] != "": 
            course = float(res["course"]["S"]) 
            if course < lower_course or upper_course < course: 
                course_count += 1
                course_alerm = 1
        if "displacement" in res and res["displacement"]["S"] != "": 
            displacement = float(res["displacement"]["S"]) 
            if displacement < lower_displacement or upper_displacement < displacement: 
                displacement_count += 1
                displacement_alerm = 1
        if "dwt" in res and res["dwt"]["S"] != "": 
            dwt = float(res["dwt"]["S"]) 
            if dwt < lower_dwt or upper_dwt < dwt: 
                dwt_count += 1
                dwt_alerm = 1
        if "gt" in res and res["gt"]["S"] != "": 
            gt = float(res["gt"]["S"]) 
            if gt < lower_gt or upper_gt < gt: 
                gt_count += 1
                gt_alerm = 1
        if "lat" in res and res["lat"]["S"] != "": 
            lat = float(res["lat"]["S"]) 
            if lat < lower_lat or upper_lat < lat: 
                lat_count += 1
                lat_alerm = 1
        if "lng" in res and res["lng"]["S"] != "": 
            lng = float(res["lng"]["S"]) 
            if lng < lower_lng or upper_lng < lng: 
                lng_count += 1
                lng_alerm = 1
        if "log_distance" in res and res["log_distance"]["S"] != "": 
            log_distance = float(res["log_distance"]["S"]) 
            if log_distance < lower_log_distance or upper_log_distance < log_distance: 
                log_distance_count += 1
                log_distance_alerm = 1
        if "log_speed" in res and res["log_speed"]["S"] != "": 
            log_speed = float(res["log_speed"]["S"]) 
            if log_speed < lower_log_speed or upper_log_speed < log_speed: 
                log_speed_count += 1
                log_speed_alerm = 1
        if "me_bog" in res and res["me_bog"]["S"] != "": 
            me_bog = float(res["me_bog"]["S"]) 
            if me_bog < lower_me_bog or upper_me_bog < me_bog: 
                me_bog_count += 1
                me_bog_alerm = 1
        if "me_hfo" in res and res["me_hfo"]["S"] != "": 
            me_hfo = float(res["me_hfo"]["S"]) 
            if me_hfo < lower_me_hfo or upper_me_hfo < me_hfo: 
                me_hfo_count += 1
                me_hfo_alerm = 1
        if "me_lsfo" in res and res["me_lsfo"]["S"] != "": 
            me_lsfo = float(res["me_lsfo"]["S"]) 
            if me_lsfo < lower_me_lsfo or upper_me_lsfo < me_lsfo: 
                me_lsfo_count += 1
                me_lsfo_alerm = 1
        if "me_do" in res and res["me_do"]["S"] != "": 
            me_do = float(res["me_do"]["S"]) 
            if me_do < lower_me_do or upper_me_do < me_do: 
                me_do_count += 1
                me_do_alerm = 1
        if "me_lsgo" in res and res["me_lsgo"]["S"] != "": 
            me_lsgo = float(res["me_lsgo"]["S"]) 
            if me_lsgo < lower_me_lsgo or upper_me_lsgo < me_lsgo: 
                me_lsgo_count += 1
                me_lsgo_alerm = 1
        if "dg_bog" in res and res["dg_bog"]["S"] != "": 
            dg_bog = float(res["dg_bog"]["S"]) 
            if dg_bog < lower_dg_bog or upper_dg_bog < dg_bog: 
                dg_bog_count += 1
                dg_bog_alerm = 1
        if "dg_hfo" in res and res["dg_hfo"]["S"] != "": 
            dg_hfo = float(res["dg_hfo"]["S"]) 
            if dg_hfo < lower_dg_hfo or upper_dg_hfo < dg_hfo: 
                dg_hfo_count += 1
                dg_hfo_alerm = 1
        if "ge_foc" in res and res["ge_foc"]["S"] != "": 
            dg_lsfo = float(res["ge_foc"]["S"]) 
            if dg_lsfo < lower_ge_foc or upper_ge_foc < dg_lsfo: 
                dg_lsfo_count += 1
                dg_lsfo_alerm = 1
        if "dg_do" in res and res["dg_do"]["S"] != "": 
            dg_do = float(res["dg_do"]["S"]) 
            if dg_do < lower_dg_do or upper_dg_do < dg_do: 
                dg_do_count += 1
                dg_do_alerm = 1
        if "dg_lsgo" in res and res["dg_lsgo"]["S"] != "": 
            dg_lsgo = float(res["dg_lsgo"]["S"]) 
            if dg_lsgo < lower_dg_lsgo or upper_dg_lsgo < dg_lsgo: 
                dg_lsgo_count += 1
                dg_lsgo_alerm = 1
        if "boiler_hfo" in res and res["boiler_hfo"]["S"] != "": 
            boiler_hfo = float(res["boiler_hfo"]["S"])
            if boiler_hfo < lower_boiler_hfo or upper_boiler_hfo < boiler_hfo: 
                boiler_hfo_count += 1
                boiler_hfo_alerm = 1
        if "boiler_foc" in res and res["boiler_foc"]["S"] != "": 
            boiler_lsfo = float(res["boiler_foc"]["S"]) 
            if boiler_lsfo < lower_boiler_foc or upper_boiler_foc < boiler_lsfo: 
                boiler_lsfo_count += 1
                boiler_lsfo_alerm = 1
        if "boiler_do" in res and res["boiler_do"]["S"] != "": 
            boiler_do = float(res["boiler_do"]["S"]) 
            if boiler_do < lower_boiler_do or upper_boiler_do < boiler_do: 
                boiler_do_count += 1
                boiler_do_alerm = 1
        if "boiler_lsgo" in res and res["boiler_lsgo"]["S"] != "": 
            boiler_lsgo = float(res["boiler_lsgo"]["S"]) 
            if boiler_lsgo < lower_boiler_lsgo or upper_boiler_lsgo < boiler_lsgo: 
                boiler_lsgo_count += 1
                boiler_lsgo_alerm = 1
        if "igg_go" in res and res["igg_go"]["S"] != "": 
            igg_go = float(res["igg_go"]["S"]) 
            if igg_go < lower_igg_go or upper_igg_go < igg_go: 
                igg_go_count += 1
                igg_go_alerm = 1
        if "igg_lsgo" in res and res["igg_lsgo"]["S"] != "": 
            igg_lsgo = float(res["igg_lsgo"]["S"]) 
            if igg_lsgo < lower_igg_lsgo or upper_igg_lsgo < igg_lsgo: 
                igg_lsgo_count += 1
                igg_lsgo_alerm = 1
        if "gcu_bog" in res and res["gcu_bog"]["S"] != "": 
            gcu_bog = float(res["gcu_bog"]["S"]) 
            if gcu_bog < lower_gcu_bog or upper_gcu_bog < gcu_bog: 
                gcu_bog_count += 1
                gcu_bog_alerm = 1
        if "me_foc" in res and res["me_foc"]["S"] != "": 
            me_foc = float(res["me_foc"]["S"]) 
            if me_foc < lower_me_foc or upper_me_foc < me_foc: 
                me_foc_count += 1
                me_foc_alerm = 1
        if "me_load" in res and res["me_load"]["S"] != "": 
            me_load = float(res["me_load"]["S"]) 
            if me_load < lower_me_load or upper_me_load < me_load: 
                me_load_count += 1
                me_load_alerm = 1
        if "me_rpm" in res and res["me_rpm"]["S"] != "": 
            me_rpm = float(res["me_rpm"]["S"]) 
            if me_rpm < lower_me_rpm or upper_me_rpm < me_rpm: 
                me_rpm_count += 1
                me_rpm_alerm = 1
        if "og_distance" in res and res["og_distance"]["S"] != "": 
            og_distance = float(res["og_distance"]["S"]) 
            if og_distance < lower_og_distance or upper_og_distance < og_distance: 
                og_distance_count += 1	
                og_distance_alerm = 1
        if "og_speed" in res and res["og_speed"]["S"] != "": 
            og_speed = float(res["og_speed"]["S"]) 
            if og_speed < lower_og_speed or upper_og_speed < og_speed: 
                og_speed_count += 1
                og_speed_alerm = 1
        if "swell_direction" in res and res["swell_direction"]["S"] != "": 
            swell_direction = float(res["swell_direction"]["S"]) 
            if swell_direction < lower_swell_direction or upper_swell_direction < swell_direction: 
                swell_direction_count += 1
                swell_direction_alerm = 1
        if "swell_height" in res and res["swell_height"]["S"] != "": 
            swell_height = float(res["swell_height"]["S"]) 
            if swell_height < lower_swell_height or upper_swell_height < swell_height: 
                swell_height_count += 1
                swell_height_alerm = 1
        if "swell_period" in res and res["swell_period"]["S"] != "": 
            swell_period = float(res["swell_period"]["S"]) 
            if swell_period < lower_swell_period or upper_swell_period < swell_period: 
                swell_period_count += 1
                swell_period_alerm = 1
        if "total_foc" in res and res["total_foc"]["S"] != "": 
            total_foc = float(res["total_foc"]["S"])
            if total_foc < lower_total_foc or upper_total_foc < total_foc: 
                total_foc_count += 1
                total_foc_alerm = 1
        if "wave_direction" in res and res["wave_direction"]["S"] != "": 
            wave_direction = float(res["wave_direction"]["S"]) 
            if wave_direction < lower_wave_direction or upper_wave_direction < wave_direction: 
                wave_direction_count += 1
                wave_direction_alerm = 1
        if "wave_height" in res and res["wave_height"]["S"] != "": 
            wave_height = float(res["wave_height"]["S"]) 
            if wave_height < lower_wave_height or upper_wave_height < wave_height: 
                wave_height_count += 1
                wave_height_alerm = 1
        if "wave_period" in res and res["wave_period"]["S"] != "": 
            wave_period = float(res["wave_period"]["S"]) 
            if wave_period < lower_wave_period or upper_wave_period < wave_period: 
                wave_period_count += 1
                wave_period_alerm = 1
        if "wind_direction" in res and res["wind_direction"]["S"] != "": 
            wind_direction = float(res["wind_direction"]["S"]) 
            if wind_direction < lower_wind_direction or upper_wind_direction < wind_direction: 
                wind_direction_count += 1
                wind_direction_alerm = 1
        if "wind_speed" in res and res["wind_speed"]["S"] != "": 
            wind_speed = float(res["wind_speed"]["S"]) 
            if wind_speed < lower_wind_speed or upper_wind_speed < wind_speed: 
                wind_speed_count += 1
                wind_speed_alerm = 1
                
        # -----------------------------------------------------------------------------
        
        VALUE_LIST = {
            
            # NoonReport項目
            "local_date": local_date,
            "utc_date": utc_date,
            "state": state,
            "port_name": port_name,
            "lat": lat,
            "lng": lng,
            "voyage_no": voyage_no,
            "leg_no": leg_no,
            "leg": leg,
            "co2_factor": co2_factor,
            "course": course,
            "beaufort": beaufort,
            "log_distance": log_distance,
            "og_distance": og_distance,
            "log_speed": log_speed,
            "og_speed": og_speed,
            "me_load": me_load,
            "me_rpm": me_rpm,
            "me_bog" : me_bog,
            "me_hfo" : me_hfo,
            "me_lsfo" : me_lsfo,
            "me_do" : me_do,
            "me_lsgo" : me_lsgo,
            "dg_bog" : dg_bog,
            "dg_hfo" : dg_hfo,
            "dg_lsfo" : dg_lsfo,
            "dg_do" : dg_do,
            "dg_lsgo" : dg_lsgo,
            "boiler_hfo": boiler_hfo,
            "boiler_lsfo": boiler_lsfo,
            "boiler_do": boiler_do,
            "boiler_lsgo": boiler_lsgo,
            "igg_go": igg_go,
            "igg_lsgo": igg_lsgo,
            "gcu_bog": gcu_bog,
            "me_foc": me_foc,
            "total_foc": total_foc,
            "eta_local_date": eta_local_date,
            "eta_utc_date": eta_utc_date,
            "eta_destination": eta_destination,
            "displacement": displacement,
            "gt": gt,
            "dwt": dwt,
            "start_local_date": start_local_date,
            "start_utc_date": start_utc_date,
            "port_code": port_code,
            "wind_speed": wind_speed,
            "wind_direction": wind_direction,
            "wave_height": wave_height,
            "wave_period": wave_period,
            "wave_direction": wave_direction,
            "swell_height": swell_height,
            "swell_period": swell_period,
            "swell_direction": swell_direction,
            "ablog_id": ablog_id,
            "eta_port_code": eta_port_code,
            "operator": operator,
            
            # 上下限値アラーム
            "beaufort_alerm" : beaufort_alerm,
            "course_alerm" : course_alerm,
            "displacement_alerm" : displacement_alerm,
            "dwt_alerm" : dwt_alerm,
            "gt_alerm" : gt_alerm,
            "lat_alerm" : lat_alerm,
            "lng_alerm" : lng_alerm,
            "log_distance_alerm" : log_distance_alerm,
            "log_speed_alerm" : log_speed_alerm,
            "me_bog_alerm" : me_bog_alerm,
            "me_hfo_alerm" : me_hfo_alerm,
            "me_lsfo_alerm" : me_lsfo_alerm,
            "me_do_alerm" : me_do_alerm,
            "me_lsgo_alerm" : me_lsgo_alerm,
            "dg_bog_alerm" : dg_bog_alerm,
            "dg_hfo_alerm" : dg_hfo_alerm,
            "dg_lsfo_alerm" : dg_lsfo_alerm,
            "dg_do_alerm" : dg_do_alerm,
            "dg_lsgo_alerm" : dg_lsgo_alerm,
            "boiler_hfo_alerm": boiler_hfo_alerm,
            "boiler_lsfo_alerm": boiler_lsfo_alerm,
            "boiler_do_alerm": boiler_do_alerm,
            "boiler_lsgo_alerm": boiler_lsgo_alerm,
            "igg_go_alerm": igg_go_alerm,
            "igg_lsgo_alerm": igg_lsgo_alerm,
            "gcu_bog_alerm": gcu_bog_alerm,
            "me_foc_alerm" : me_foc_alerm,
            "me_load_alerm" : me_load_alerm,
            "me_rpm_alerm" : me_rpm_alerm,
            "og_distance_alerm" : og_distance_alerm,
            "og_speed_alerm" : og_speed_alerm,
            "swell_direction_alerm" : swell_direction_alerm,
            "swell_height_alerm" : swell_height_alerm,
            "swell_period_alerm" : swell_period_alerm,
            "total_foc_alerm" : total_foc_alerm,
            "wave_direction_alerm" : wave_direction_alerm,
            "wave_height_alerm" : wave_height_alerm,
            "wave_period_alerm" : wave_period_alerm,
            "wind_direction_alerm" : wind_direction_alerm,
            "wind_speed_alerm" : wind_speed_alerm,
        }
        RESPONSE_LIST.append(VALUE_LIST)
        # print(f"VALUE_LIST[{type(len(VALUE_LIST))}][{len(VALUE_LIST)}]: {VALUE_LIST}")
    
    ALERM_LIST = {
        "beaufort_alerm_count": beaufort_count,
        "course_alerm_count": course_count,
        "displacement_alerm_count": displacement_count,
        "dwt_alerm_count": dwt_count,
        "gt_alerm_count": gt_count,
        "lat_alerm_count": lat_count,
        "lng_alerm_count": lng_count,
        "log_distance_alerm_count": log_distance_count,
        "log_speed_alerm_count": log_speed_count,
        "me_bog_alerm_count": me_bog_count,
        "me_hfo_alerm_count": me_hfo_count,
        "me_lsfo_alerm_count": me_lsfo_count,
        "me_do_alerm_count": me_do_count,
        "me_lsgo_alerm_count": me_lsgo_count,
        "dg_bog_alerm_count": dg_bog_count,
        "dg_hfo_alerm_count": dg_hfo_count,
        "dg_lsfo_alerm_count": dg_lsfo_count,
        "dg_do_alerm_count": dg_do_count,
        "dg_lsgo_alerm_count": dg_lsgo_count,
        "boiler_hfo_alerm_count": boiler_hfo_count,
        "boiler_lsfo_alerm_count": boiler_lsfo_count,
        "boiler_do_alerm_count": boiler_do_count,
        "boiler_lsgo_alerm_count": boiler_lsgo_count,
        "igg_go_alerm_count": igg_go_count,
        "igg_lsgo_alerm_count": igg_lsgo_count,
        "gcu_bog_alerm_count": gcu_bog_count,
        "me_foc_alerm_count": me_foc_count,
        "me_load_alerm_count": me_load_count,
        "me_rpm_alerm_count": me_rpm_count,
        "og_distance_alerm_count": og_distance_count,
        "og_speed_alerm_count": og_speed_count,
        "swell_direction_alerm_count": swell_direction_count,
        "swell_height_alerm_count": swell_height_count,
        "swell_period_alerm_count": swell_period_count,
        "total_foc_alerm_count": total_foc_count,
        "wave_direction_alerm_count": wave_direction_count,
        "wave_height_alerm_count": wave_height_count,
        "wave_period_alerm_count": wave_period_count,
        "wind_direction_alerm_count": wind_direction_count,
        "wind_speed_alerm_count": wind_speed_count,
    }
    
    
    print(f"res_count: {res_count}")
    print(f"RESPONSE_LIST[{type(len(RESPONSE_LIST))}][{len(RESPONSE_LIST)}]: {RESPONSE_LIST}")
    return RESPONSE_LIST, ALERM_LIST
    
    
def util_DataViewer(imo, response):
    
    VESSELMATSER = VesselMaster.VesselMaster(imo)
    DATACHANNEL = DataChannel.DataChannel(imo)
    RESPONSE_LIST, ALERM_LIST = create_responce_list(imo, response, DATACHANNEL)
    print(f"DATACHANNEL: {DATACHANNEL}")
    DataViewer = {
        "VESSELMATSER": VESSELMATSER,
        "DATACHANNEL": DATACHANNEL,
        "ALERMLIST": ALERM_LIST,
        "TIMESERIES": RESPONSE_LIST,
    }
    
    return DataViewer
    
    
def DataViewer(imo, Timestamp_from, Timestamp_to):
    response = dynamodb.get_noonreport(imo, Timestamp_from, Timestamp_to)
    response = util_DataViewer(imo, response)
    return response
