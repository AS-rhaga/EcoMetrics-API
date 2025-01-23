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
    boiler_foc_count = 0
    course_count = 0
    displacement_count = 0
    dwt_count = 0
    ge_foc_count = 0
    gt_count = 0
    lat_count = 0
    lng_count = 0
    log_distance_count = 0
    log_speed_count = 0
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
        
        # 数値項目、上下限チェック開始
        beaufort = ""
        boiler_foc = ""
        course = ""
        displacement = ""
        dwt = ""
        ge_foc = ""
        gt = ""
        lat = ""
        lng = ""
        log_distance = ""
        log_speed = ""
        me_foc = ""
        me_load = ""
        me_rpm = ""
        og_distance = ""
        og_speed = ""
        swell_direction = ""
        swell_height = ""
        swell_period = ""
        total_foc = ""
        co2 = ""
        wave_direction = ""
        wave_height = ""
        wave_period = ""
        wind_direction = ""
        wind_speed = ""
        
        beaufort_alerm = 0
        boiler_foc_alerm = 0
        course_alerm = 0
        displacement_alerm = 0
        dwt_alerm = 0
        ge_foc_alerm = 0
        gt_alerm = 0
        lat_alerm = 0
        lng_alerm = 0
        log_distance_alerm = 0
        log_speed_alerm = 0
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
        if "boiler_foc" in res and res["boiler_foc"]["S"] != "": 
            boiler_foc = float(res["boiler_foc"]["S"]) 
            if boiler_foc < lower_boiler_foc or upper_boiler_foc < boiler_foc: 
                boiler_foc_count += 1
                boiler_foc_alerm = 1
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
        if "ge_foc" in res and res["ge_foc"]["S"] != "": 
            ge_foc = float(res["ge_foc"]["S"]) 
            if ge_foc < lower_ge_foc or upper_ge_foc < ge_foc: 
                ge_foc_count += 1
                ge_foc_alerm = 1
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
            co2 = total_foc * co2_factor
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
            "co2": co2_factor,
            "course": course,
            "beaufort": beaufort,
            "log_distance": log_distance,
            "og_distance": og_distance,
            "log_speed": log_speed,
            "og_speed": og_speed,
            "me_rpm": me_rpm,
            "ge_foc": ge_foc,
            "boiler_foc": boiler_foc,
            "me_foc": me_foc,
            "total_foc": total_foc,
            "me_load": me_load,
            "eta_local_date": eta_local_date,
            "eta_utc_date": eta_utc_date,
            "eta_destination": eta_destination,
            "displacement": displacement,
            "gt": gt,
            "dwt": dwt,
            "wind_speed": wind_speed,
            "wind_direction": wind_direction,
            "wave_period": wave_period,
            "wave_direction": wave_direction,
            "wave_height": wave_height,
            "swell_height": swell_height,
            "swell_period": swell_period,
            "swell_direction": swell_direction,
            "ablog_id": ablog_id,
            
            # 上下限値アラーム
            "beaufort_alerm" : beaufort_alerm,
            "boiler_foc_alerm" : boiler_foc_alerm,
            "course_alerm" : course_alerm,
            "displacement_alerm" : displacement_alerm,
            "dwt_alerm" : dwt_alerm,
            "ge_foc_alerm" : ge_foc_alerm,
            "gt_alerm" : gt_alerm,
            "lat_alerm" : lat_alerm,
            "lng_alerm" : lng_alerm,
            "log_distance_alerm" : log_distance_alerm,
            "log_speed_alerm" : log_speed_alerm,
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

            # 上下限値
            # "lower_beaufort": lower_beaufort,
            # "lower_boiler_foc": lower_boiler_foc,
            # "lower_course": lower_course,
            # "lower_displacement": lower_displacement,
            # "lower_dwt": lower_dwt,
            # "lower_ge_foc": lower_ge_foc,
            # "lower_gt": lower_gt,
            # "lower_lat": lower_lat,
            # "lower_lng": lower_lng,
            # "lower_log_distance": lower_log_distance,
            # "lower_log_speed": lower_log_speed,
            # "lower_me_foc": lower_me_foc,
            # "lower_me_load": lower_me_load,
            # "lower_me_rpm": lower_me_rpm,
            # "lower_og_distance": lower_og_distance,
            # "lower_og_speed": lower_og_speed,
            # "lower_swell_direction": lower_swell_direction,
            # "lower_swell_height": lower_swell_height,
            # "lower_swell_period": lower_swell_period,
            # "lower_total_foc": lower_total_foc,
            # "lower_wave_direction": lower_wave_direction,
            # "lower_wave_height": lower_wave_height,
            # "lower_wave_period": lower_wave_period,
            # "lower_wind_direction": lower_wind_direction,
            # "lower_wind_speed": lower_wind_speed,
            # "upper_beaufort": upper_beaufort,
            # "upper_boiler_foc": upper_boiler_foc,
            # "upper_course": upper_course,
            # "upper_displacement": upper_displacement,
            # "upper_dwt": upper_dwt,
            # "upper_ge_foc": upper_ge_foc,
            # "upper_gt": upper_gt,
            # "upper_lat": upper_lat,
            # "upper_lng": upper_lng,
            # "upper_log_distance": upper_log_distance,
            # "upper_log_speed": upper_log_speed,
            # "upper_me_foc": upper_me_foc,
            # "upper_me_load": upper_me_load,
            # "upper_me_rpm": upper_me_rpm,
            # "upper_og_distance": upper_og_distance,
            # "upper_og_speed": upper_og_speed,
            # "upper_swell_direction": upper_swell_direction,
            # "upper_swell_height": upper_swell_height,
            # "upper_swell_period": upper_swell_period,
            # "upper_total_foc": upper_total_foc,
            # "upper_wave_direction": upper_wave_direction,
            # "upper_wave_height": upper_wave_height,
            # "upper_wave_period": upper_wave_period,
            # "upper_wind_direction": upper_wind_direction,
            # "upper_wind_speed": upper_wind_speed,
        }
        RESPONSE_LIST.append(VALUE_LIST)
        # print(f"VALUE_LIST[{type(len(VALUE_LIST))}][{len(VALUE_LIST)}]: {VALUE_LIST}")
    
    ALERM_LIST = {
        "beaufort_alerm_count": beaufort_count,
        "boiler_foc_alerm_count": boiler_foc_count,
        "course_alerm_count": course_count,
        "displacement_alerm_count": displacement_count,
        "dwt_alerm_count": dwt_count,
        "ge_foc_alerm_count": ge_foc_count,
        "gt_alerm_count": gt_count,
        "lat_alerm_count": lat_count,
        "lng_alerm_count": lng_count,
        "log_distance_alerm_count": log_distance_count,
        "log_speed_alerm_count": log_speed_count,
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
