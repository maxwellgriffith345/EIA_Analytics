import requests
import pandas as pd
import os
import time
from config import EIA_KEY

def make_request(name: str, route: str, params: dict) -> pd.DataFrame:

    #should update the start and end date here for all requests
    start_date = "2025-01"
    end_date = "2025-03"
    params["start"] = start_date
    params["end"] = end_date
    params["length"] = 5000
    params["offset"] = 0

    all_rows = []

    #infinite loop, will continue to loop unitl a break statement
    while True:

        #give it three trys to get a good response
        for attempt in range(3):
            try:
                response = requests.get(route, params = params)
                response.raise_for_status()
                break #when the request is successful
            except requests.exceptions.HTTPError as error: #catch an API error
                #502 bad gateway
                #503 Service Unavailable
                #504 Gateway Timeout
                if response.status_code in (502,503,504):
                    wait = 2 ** attempt
                    time.sleep(wait)
                else:
                    raise #other erros we don't want to retry
        else: #through an error if it loops three times without breaking
            raise Exception("Failed after three attempts "+name, )

        data = response.json() #dict

        #check for warnings ie row limit warning
        #need to paganate using offset if we hit the row limit
        if params["offset"] == 0:
            warnings = data.get("warnings")
            if warnings:
                print(warnings[0].get('warning') + " for " + name)

        #returns a list of all the data rows
        #this way we can append new rows to the list
        rows = data['response']['data']
        total_rows = int(data['response']['total'])

        #if no rows returned excit loop
        if not rows:
            break

        #use extend to add another list to a list
        #append is for single elements
        all_rows.extend(rows)
        print(f"  {name}: fetched {len(all_rows)} / {total_rows} rows")
        #exit loop if we have all the rows
        if len(all_rows) >= total_rows:
            break

        #small delay between successful returns
        time.sleep(0.5)
        params["offset"] += 5000

    #convert json list to df and return
    return pd.DataFrame(all_rows)

def get_gen_data() -> pd.DataFrame:
    name = "generation"
    gen_url = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data"
    gen_params = {
        "frequency": "monthly",
        "data[0]": "generation",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "api_key": EIA_KEY
    }

    gen_df = make_request(name, gen_url, gen_params)

    return gen_df

def get_price_data()-> pd.DataFrame:
    name = "price"
    price_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    price_params = {
        "frequency": "monthly",
        "data[0]": "price",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "api_key": EIA_KEY
    }

    price_df = make_request(name, price_url, price_params)

    return price_df

def get_fuel_data() -> pd.DataFrame:
    name = "fuel"
    fuel_url = "https://api.eia.gov/v2/natural-gas/pri/sum/data/"
    fuel_params = {
        "frequency": "monthly",
        "data[0]": "value",
        "facets[process][]": "PEU",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "api_key": EIA_KEY
    }

    fuel_df = make_request(name, fuel_url, fuel_params)
    return fuel_df

def dump_to_csv():
    #Code to dump dataframes to csv
    data_path = "data/"
    os.makedirs(data_path, exist_ok = True)

    gen = get_gen_data()
    price = get_price_data()
    fuel = get_fuel_data()

    #safer way to do paths?
    gen.to_csv(os.path.join(data_path, "gen_data.csv"), index=False)
    price.to_csv(os.path.join(data_path,"price_data.csv"), index=False)
    fuel.to_csv(os.path.join(data_path,"fuel_data.csv"), index=False)


if __name__ == '__main__':
    dump_to_csv()
