import ast

from poseidon import dynamodb


# vesselmaster
def util_vesselalerm(response):
    
    vesselalerm = {
        "imo"               : response[0]["imo"]["S"],
        "cp"                : response[0]["cp"]["S"],
        "cp_from"           : response[0]["cp_from"]["S"],
        "cp_to"             : response[0]["cp_to"]["S"],
        "favorite"          : response[0]["favorite"]["S"],
        "Januarytonow_val"  : response[0]["Januarytonow_val"]["S"],
        "Januarytonow"      : response[0]["Januarytonow"]["S"],
        "Januarytonow_from" : response[0]["Januarytonow_from"]["S"],
        "Januarytonow_to"   : response[0]["Januarytonow_to"]["S"],
        "LastYear_val"      : response[0]["LastYear_val"]["S"],
        "LastYear"          : response[0]["LastYear"]["S"],
        "LastYear_from"     : response[0]["LastYear_from"]["S"],
        "LastYear_to"       : response[0]["LastYear_to"]["S"],
        "LatestUpdate"      : response[0]["LatestUpdate"]["S"],
        "oneMonth_val"      : response[0]["oneMonth_val"]["S"],
        "oneMonth"          : response[0]["oneMonth"]["S"],
        "oneMonth_from"     : response[0]["oneMonth_from"]["S"],
        "oneMonth_to"       : response[0]["oneMonth_to"]["S"],
        "rf"                : response[0]["rf"]["S"],
        "rf_from"           : response[0]["rf_from"]["S"],
        "rf_to"             : response[0]["rf_to"]["S"],
        "VesselName"         : response[0]["VesselName"]["S"],
    }
    
    return vesselalerm

def VesselAlerm(imo_no):
    response = dynamodb.get_vesselalerm(imo_no)
    response = util_vesselalerm(response)
    return response