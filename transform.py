import pandas as pd
import os

def getStateMap_ShortToLong(df: pd.DataFrame) -> dict:
    #should verify that column stateDescription is there
    map = pd.Series(df.stateDescription.values, index=df.stateid).to_dict()
    return map

def getStateMap_LongToShort(df: pd.DataFrame) -> dict:
    map = pd.Series(df.stateid.values, index=price_df.stateDescription).to_dict()
    return map

########### Transform Functions ############
def transform_gen(df: pd.DataFrame, stateMap: dict, idMap={}, CSV = False) -> pd.DataFrame:
    df["stateShort"] = df.stateDescription.map(stateMap)
    df["date_id"] = df.period.str.replace("-", "")

    if CSV:
        #columns: period, dateShort, stateShort, stateDescription, fueltypeid, fueTypeDescription,
        # generation
        df = df[["period", "date_id", "stateShort", "stateDescription",
                    "fueltypeid", "fuelTypeDescription", "generation"]]

        df = df.rename(columns={
            "stateDescription": "stateLong",
            "fueltypeid": "fuelShort",
            "fuelTypeDescription": "fuelLong"
        })
    else:
        # map: state_id, and fuel_id
        df["state_id"]=df.stateShort.map(idMap["statemap"])
        df["fuel_id"]=df.fueltypeid.map(idMap["fuelmap"])
        # columns:date_id, state_id, fuel_id, generation
        df = df[["date_id", "state_id", "fuel_id", "generation"]]

    return df

def transform_prices(df: pd.DataFrame, statemap: dict, idMap={}, CSV = False) -> pd.DataFrame:
    #don't need to create state long an short columns just rename
    df["date_id"] = df.period.str.replace("-", "")
    # price is in cents per kilowatt-hou

    if CSV:
        # drop columns
        #period 	stateid 	stateDescription 	sectorid 	sectorName 	price
        # don't add the "ID columns"
        df = df.drop(columns = ["price-units"])
        df = df.rename(columns={
            "stateid" : "stateShort",
            "stateDescription": "stateLong",
            "sectorid": "sectorShort",
            "sectorName": "sectorLong",
            "price" : "price_per_kwh"
        })
    else:
        # map: state_id, sector_id
        df["state_id"] = df.stateid.map(idMap["statemap"])
        df["sector_id"] = df.sectorid.map(idMap["sectormap"])
        # olumns: date_id, state_id, sector_id, price_per
        df = df[["date_id", "state_id", "sector_id", "price"]]
        df = df.rename(columns = {"price": "price_per_kwh"})

    return df

def transform_fuel(df: pd.DataFrame, stateMap: dict, idMap={}, CSV = False) -> pd.DataFrame:
    #do stuff
    #get date id column
    df["stateShort"] = df.duoarea.str[1:]
    df["stateLong"] = df.stateShort.map(stateMap)
    df["date_id"] = df.period.str.replace("-", "")

    df["fuelShort"] = df["product-name"].map({"Natural Gas": "NG"})

    if CSV:
        # columns
        #period date_id stateShort stateLong	duoarea 	area-name 	product 	product-name 	process 	process-name 	series 	series-description 	value 	units
            df = df[["period", "date_id", "stateShort", "stateLong", "fuelShort",
                        "product-name", "fuelTypeDescription", "value"]]

        # rename
        df = df.rename(columns = {"value": "price"})

    else:
        # map: state_id, fuel_id
        df["state_id"]=df.stateShort.map(idMap["statemap"])
        df["fuel_id"]=df.fuelShort.map(idMap["fuelmap"])
        # columns
        df =df[["date_id", "state_id", "fuel_id", "value"]]
        df = df.rename(columns = {"value": "price"})

    return df

def transform_csv():
    dataIn_path = "data/raw"
    dataOut_path = "data/cleaned"

    gen = pd.read_csv(os.path.join(dataIn_path, "gen_data.csv"))
    price = pd.read_csv(os.path.join(dataIn_path, "price_data.csv"))
    fuel = pd.read_csv(os.path.join(dataIn_path, "fuel_data.csv"))

    stateLongToShort = getStateMap_LongToShort(price)
    stateShortToLong = getStateMap_ShortToLong(price)

    gen = transform_gen(gen, stateLongToShort, CSV = True)
    price = transform_prices(price, CSV = True)
    fuel = transform_fuel(fuel, stateShortToLong, CSV = True)

    gen.to_csv(os.path.join(dataOut_path, "gen_data.csv"), index=False)
    price.to_csv(os.path.join(dataOut_path,"price_data.csv"), index=False)
    fuel.to_csv(os.path.join(dataOut_path,"fuel_data.csv"), index=False)


if __name__ == '__main__':
    transform_csv()
