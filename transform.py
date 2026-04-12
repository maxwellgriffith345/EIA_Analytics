import pandas as pd
import os

def getStateMap_ShortToLong(df: pd.DataFrame) -> dict:
    #should verify that column stateDescription is there
    map = pd.Series(df.stateDescription.values, index=price_df.stateid).to_dict()
    return map

def getStateMap_LongToShort(df: pd.DataFrame) -> dict:
    map = pd.Series(df.stateid.values, index=price_df.stateDescription).to_dict()
    return map

def transform_gen(df: pd.DataFrame, stateMap: dict) -> pd.DataFrame:
    #do stuff
    df["stateShort"] = df.stateDescription.map(stateMap)
    return df

def transform_prices(df: pd.DataFrame, statemap: dict) -> pd.DataFrame:
    #do stuff
    #don't need to create state long an short columns just rename

    return df

def transform_fuel(df: pd.DataFrame, stateMap: dict) -> pd.DataFrame:
    #do stuff
    df["stateShort"] = df.duoarea.str[1:]
    df["stateLong"] = df.stateShort.map(stateMap)
    return df


def transform_csv():
    dataIn_path = "data/raw"
    dataOut_path = "data/cleaned"

    gen = pd.read_csv(os.path.join(dataIn_path, "gen_data.csv"))
    price = pd.read_csv(os.path.join(dataIn_path, "price_data.csv"))
    fuel = pd.read_csv(os.path.join(dataIn_path, "fuel_data.csv"))

    stateLongToShort = getStateMap_LongToShort(price)
    stateShortToLong = getStateMap_ShortToLong(price)

    gen = transform_gen(gen, stateLongToShort)
    price = transform_prices(price)
    fuel = transform_fuel(fuel, stateShortToLong)

    gen.to_csv(os.path.join(dataOut_path, "gen_data.csv"), index=False)
    price.to_csv(os.path.join(dataOut_path,"price_data.csv"), index=False)
    fuel.to_csv(os.path.join(dataOut_path,"fuel_data.csv"), index=False)


if __name__ == '__main__':
    transform_csv()
