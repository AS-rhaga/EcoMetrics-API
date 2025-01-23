import ast

from dynamodb import select


# vesselmaster
def util_vesselmaster(response):
    
    vesselmaster = {
        "BuildDate"         : response[0]["BuildDate"]["S"],
        "Deadweight"        : response[0]["Deadweight"]["S"],
        "Grosstongue"       : response[0]["Grosstongue"]["S"],
        "LastDockDate"      : response[0]["LastDockDate"]["S"],
        "HullCleaningDate"  : response[0]["HullCleaningDate"]["S"],
        "OilType"           : response[0]["OilType"]["S"],
        "Owner"             : response[0]["Owner"]["S"],
        "Size"              : response[0]["Size"]["S"],
        "VesselName"        : response[0]["VesselName"]["S"],
        "VesselType"        : response[0]["VesselType"]["S"],
        "Ballast"           : response[0]["Ballast"]["S"],
        "Laden"             : response[0]["Laden"]["S"],
    }
    
    return vesselmaster

def VesselMaster(imo_no):
    response = select.get_vessel_master(imo_no)
    response = util_vesselmaster(response)
    return response