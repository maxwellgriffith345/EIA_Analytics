import requests
import pandas as pd
import os
import time
from config import EIA_KEY

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
    "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT",
    "VT","VA","WA","WV","WI","WY"
]

def make_request(name: str, route: str, params: dict) -> pd.DataFrame:

    years = range(2020, 2025)  # 2020 through 2024
    all_dfs = []

    for year in years:
        params["start"]  = f"{year}-01"
        params["end"]    = f"{year}-12"
        params["offset"] = 0
        params["length"] = 5000
        year_rows = []

        print(f"  {name} {year}: starting...")

        while True:
            for attempt in range(3):
                try:
                    response = requests.get(route, params=params)
                    response.raise_for_status()
                    break
                except requests.exceptions.HTTPError as e:
                    if response.status_code in (500, 502, 503, 504):
                        wait = 2 ** attempt
                        print(f"    Server error attempt {attempt + 1}, retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        raise
            else:
                raise Exception(f"Failed after 3 attempts — {name} {year} offset {params['offset']}")

            data = response.json()
            rows = data["response"]["data"]
            total = int(data["response"]["total"])

            if not rows:
                break

            year_rows.extend(rows)
            print(f"  {name} {year}: fetched {len(year_rows)} / {total} rows")

            if len(year_rows) >= total:
                break

            params["offset"] += 5000
            time.sleep(0.5)

        all_dfs.append(pd.DataFrame(year_rows))
        print(f"  {name} {year}: complete")

    return pd.concat(all_dfs, ignore_index=True)
# https://www.eia.gov/opendata/browser/electricity/electric-power-operational-data?frequency=monthly&data=generation;&facets=sectorid;&sectorid=1;2;3;4;5;6;7;8;&start=2025-01&end=2025-02&sortColumn=period;&sortDirection=desc;
def get_gen_data() -> pd.DataFrame:
    name = "generation"
    gen_url = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data"

    all_dfs = []

    for state in STATES:
        print(f" pulling {name} for {state}...")
        gen_params = {
            "frequency": "monthly",
            "data[0]": "generation",
            "facets[location][]": [state],
            "facets[sectorid][]": [1,2,3,4,5,6,7,8],
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "api_key": EIA_KEY
        }

        state_df = make_request(name, gen_url, gen_params)
        if not state_df.empty:
            all_dfs.append(state_df)
        time.sleep(0.5)

    return pd.concat(all_dfs, ignore_index = True)

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
    data_path = "data/raw/"
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
