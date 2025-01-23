import ast

from poseidon import dynamodb


# fuel_oil_type
def util_fuel_oil_type(response):
    
    emission_factors = {
        "fuel_oil_type"         : response[0]["fuel_oil_type"]["S"],
        "fuel_oil_type_name"    : response[0]["fuel_oil_type_name"]["S"],
        "emission_factor"       : response[0]["emission_factor"]["S"],
    }
    
    return emission_factors

def FuelOilType(fuel_oil_type):
    response = dynamodb.get_fuel_oil_type(fuel_oil_type)
    response = util_fuel_oil_type(response)
    return response