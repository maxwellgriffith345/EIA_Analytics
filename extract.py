import requests
import pandas as pd
import os
from config.py import EIA_KEY

def make_request(req_name: str, route: str, params: dict) -> pd.DataFrame:

    #should update the start and end date here for all requests
    start_date = "2025-01"
    end_date = "2025-01"
    params["start"] = start_date
    params["end"] = end_date

    result = requests.get(route, params)

    #chec the request is good?
    result.raise_for_status()

    #check if the rquest is over the row limit

    #convert to json
    result_data = result.json() #dict

    #check for warnings
    #add logic here to check if warnings is empty before pulling warning
    #add request name here to the print statement
    try:
        warnings = result_data['warnings'][0]['warning']
        print(warnings)
    except:
        print("no warnings")
    #get the data
    result_df = pd.DataFrame(result_data['response']['data'])

    #any other clean up needed?

    return result_df


def get_gen_data() -> pd.DataFrame:
    gen_url = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data"

    gen_params = {
        "frequency": "monthly",
        "data[0]": "generation",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
        "api_key": EIA_KEY
    }

    gen_df = make_request(gen_url, gen_params)

    #clean up the data here

    return gen_df


def get_price_data()-> pd.DataFrame:
    price_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"

    price_params = {
        "frequency": "monthly",
        "data[0]": "price",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
        "api_key": EIA_KEY
    }

    price_df = make_request(price_url, price_params)

    return price_df

def get_fuel_data() -> pd.DataFrame:
    fuel_url = "https://api.eia.gov/v2/natural-gas/pri/sum/data/"

    fuel_params = {
        "frequency": "monthly",
        "data[0]": "value",
        "facets[process][]": "PEU",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
        "api_key": EIA_KEY
    }

    fuel_df = make_request(fuel_url, fuel_params)
    return fuel_df


def dump_to_csv():
    #Code to dump dataframes to csv
    data_path = "data/"

    gen = get_gen_data()
    price = get_price_data()
    fuel = get_fuel_data()

    #safer way to do paths?
    gen.to_csv(data_path + "gen_data.csv", index=False)
    price.to_csv(data_path + "price_data.csv", index=False)
    fuel.to_csv(data_path + "fuel_data.csv", index=False)


if __name__ == '__main__':
    dump_to_csv()
