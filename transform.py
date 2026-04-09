import pandas as pd
import os

def transform_gen(df: pd.DataFrame) -> pd.DataFrame:
    #do stuff

    return df


def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
    #do stuff

    return df


def transform_fuel(df: pd.DataFrame) -> pd.DataFrame:
    #do stuff

    return df


def transform_csv():
    dataIn_path = "data/raw"
    dataOut_path = "data/cleaned"

    gen = pd.read_csv(os.path.join(dataIn_path, "gen_data.csv"))
    price = pd.read_csv(os.path.join(dataIn_path, "price_data.csv"))
    fuel = pd.read_csv(os.path.join(dataIn_path, "fuel_data.csv"))

    gen = transform_gen(gen)
    price = transform_prices(price)
    fuel = transform_fuel(fuel)

    gen.to_csv(os.path.join(dataOut_path, "gen_data.csv"), index=False)
    price.to_csv(os.path.join(dataOut_path,"price_data.csv"), index=False)
    fuel.to_csv(os.path.join(dataOut_path,"fuel_data.csv"), index=False)


if __name__ == '__main__':
    transform_csv()
