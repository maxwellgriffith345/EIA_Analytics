import pandas as pd
import os

def get_state_map_long_to_short(df: pd.DataFrame) -> dict:
    #maps stateDescription to stateid (Taxes to TX)
    state_map = pd.Series(df.stateid.values, index=df.stateDescription).to_dict()
    return state_map

def get_state_map_short_to_long(df: pd.DataFrame) -> dict:
    #should verify that column stateDescription is there
    state_map = pd.Series(df.stateDescription.values, index=df.stateid).to_dict()
    return state_map

# TODO DROP NAs
########### Transform Functions ############
def transform_gen(df: pd.DataFrame, stateMap: dict, idMap={}, CSV = False) -> pd.DataFrame:
    df = df.copy()
    #Drop Pacific and Puerto Rico states they only show in gen data
    df = df[df.stateDescription.isin(["Pacific", "Puerto Rico"]) == False]
    df = df[df.fuelTypeDescription.isin(["biomass"]) == False]

    df["stateShort"] = df.stateDescription.map(stateMap) #long to short map
    df["date_id"] = df.period.str.replace("-", "").astype(int)

    if CSV:
        df = df[["period", "date_id", "stateShort", "stateDescription",
                    "fueltypeid", "fuelTypeDescription", "generation"]]

        df = df.rename(columns={
            "stateDescription": "stateLong",
            "fueltypeid": "fuelShort",
            "fuelTypeDescription": "fuelLong"
        })
    else:
        df["state_id"]=df.stateShort.map(idMap["state"])
        df["fuel_id"]=df.fueltypeid.map(idMap["fuel"])
        df = df[["date_id", "state_id", "fuel_id", "generation"]]

    return df

def transform_prices(df: pd.DataFrame, idMap={}, CSV = False) -> pd.DataFrame:
    df = df.copy()
    df["date_id"] = df.period.str.replace("-", "").astype(int)
    # price is in cents per kilowatt-hou

    if CSV:
        df = df.drop(columns = ["price-units"], errors = "ignore")
        df = df.rename(columns={
            "stateid" : "stateShort",
            "stateDescription": "stateLong",
            "sectorid": "sectorShort",
            "sectorName": "sectorLong",
            "price" : "price_per_kwh"
        })
    else:
        df["state_id"] = df.stateid.map(idMap["state"])
        df["sector_id"] = df.sectorName.map(idMap["sector"])
        df = df[["date_id", "state_id", "sector_id", "price"]]
        df = df.rename(columns = {"price": "price_per_kwh"})

    return df

def transform_fuel(df: pd.DataFrame, stateMap: dict, idMap={}, CSV = False) -> pd.DataFrame:
    df = df.copy()
    df["stateShort"] = df.duoarea.str[1:]
    df["stateLong"] = df.stateShort.map(stateMap)
    df["date_id"] = df.period.str.replace("-", "").astype(int)
    df["fuelShort"] = df["product-name"].map({"Natural Gas": "NG"})

    if CSV:
        df = df[["period", "date_id", "stateShort", "stateLong", "fuelShort",
                        "product-name", "value"]]

        df = df.rename(columns = {
            "value": "price",
            "product-name": "fuel_long"
        })

    else:
        df["state_id"]=df.stateShort.map(idMap["state"])
        df["fuel_id"]=df.fuelShort.map(idMap["fuel"])
        df =df[["date_id", "state_id", "fuel_id", "value"]]
        df = df.rename(columns = {"value": "price_per_mmbtu"})

    return df

# -- All together ----
def transform_csv():
    dataIn_path = "data/raw"
    dataOut_path = "data/cleaned"
    os.makedirs(dataOut_path, exist_ok=True) #create folder if it's not there

    gen = pd.read_csv(os.path.join(dataIn_path, "gen_data.csv"))
    price = pd.read_csv(os.path.join(dataIn_path, "price_data.csv"))
    fuel = pd.read_csv(os.path.join(dataIn_path, "fuel_data.csv"))

    long_to_short = get_state_map_long_to_short(price)
    short_to_long = get_state_map_short_to_long(price)

    gen = transform_gen(gen, long_to_short, CSV = True)
    price = transform_prices(price, CSV = True)
    fuel = transform_fuel(fuel, short_to_long, CSV = True)

        # ---- Sanity checks ----
    checks = {
        "gen missing stateShort":    gen["stateShort"].isna().sum(),
        "gen missing fuelShort":     gen["fuelShort"].isna().sum(),
        "price missing stateShort":  price["stateShort"].isna().sum(),
        "price missing sectorShort": price["sectorShort"].isna().sum(),
        "fuel missing stateShort":   fuel["stateShort"].isna().sum(),
        "fuel missing fuelShort":    fuel["fuelShort"].isna().sum(),
    }

    all_passed = True
    for check, count in checks.items():
        if count > 0:
            print(f"WARNING: {count} unmapped rows — {check}")
            all_passed = False

    if all_passed:
        print("All sanity checks passed")
    else:
        print("Fix mapping issues before loading to database")
        return  # stop here, don't write bad data to CSV



    gen.to_csv(os.path.join(dataOut_path, "gen_data.csv"), index=False)
    price.to_csv(os.path.join(dataOut_path,"price_data.csv"), index=False)
    fuel.to_csv(os.path.join(dataOut_path,"fuel_data.csv"), index=False)


if __name__ == '__main__':
    transform_csv()
