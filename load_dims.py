import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# TODO: need to allow functions to except a data frame not just read csv

def get_engine():
    engine = create_engine(
    "postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/eiadb"
    )

    # 2. Test the connection
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
        print("Connection successful:", result.fetchone())
        return engine
    except OperationalError:
        print("connection failed")
        return None


#Price states
def get_states(df):
    state_series = pd.Series(df.stateDescription.values, index=df.stateid).drop_duplicates()
    state_df = state_series.reset_index()
    #df column names must match the schemal names
    state_df = state_df.rename(columns = {"stateid": "state_short", 0: "state_long"})
    return state_df

#Price sectors
def get_price_sectors(df):
    sector_series = df.sectorName.drop_duplicates()
    sector_df = sector_series.to_frame(name = "sector_name")
    return sector_df

def get_gen_sectors(df):
    sector_series = df.sectorDescription.drop_duplicates()
    sector_df = sector_series.to_frame(name = "sector_name")
    return sector_df

#Gen fuels
def get_fuels(df):
    fuel_series = pd.Series(df.fuelTypeDescription.values, index=df.fueltypeid).drop_duplicates()
    fuel_df = fuel_series.reset_index()
    fuel_df = fuel_df.rename(columns = {"fueltypeid": "fuel_short", 0: "fuel_long"})
    return fuel_df #fuel_short, fuel_long

#Price dates
def get_dates(df):
    date_series = df.period.drop_duplicates()
    date_df = date_series.to_frame(name = "period")
    date_df["date_id"] = date_df.period.str.replace("-", "").astype("int32")
    date_df["date"] = pd.to_datetime(date_df["period"], format="%Y-%m")

    # Extract components
    date_df["year"]       = date_df["date"].dt.year
    date_df["month"]      = date_df["date"].dt.month
    date_df["month_name"] = date_df["date"].dt.strftime("%B")  # "January", "February" etc
    date_df["quarter"]    = date_df["date"].dt.quarter


    return date_df[["date_id", "year", "month", "month_name", "quarter"]]

def load_table(engine, tableName, df):
    with engine.connect() as conn:
        df.to_sql(name=tableName, con=conn, if_exists='delete_rows', index=False)
        conn.commit()
        print("Loaded", len(df), f"rows into {tableName}")

def get_map(engine, key_col: str, val_col: str, tablename: str):
    query = f"SELECT {key_col}, {val_col} FROM {tablename}"
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchall()
    result_map = dict(result)
    return result_map


# returns a dictonary of the maps state_id, fuel_id, sector_id
def get_all_maps(engine):
    all_maps = {
        "state": get_map(engine, "state_short", "state_id", "dim_state" ),
        "fuel": get_map(engine, "fuel_short", "fuel_id", "dim_fuel"),
        "price_sector": get_map(engine, "sector_name", "sector_id", "dim_pricesector"),
        "gen_sector": get_map(engine, "sector_name", "sector_id", "dim_gensector")
    }

    return all_maps

if __name__ == '__main__':

    dataIn_path = "data/raw"

    engine = get_engine()

    #check if the engine connected
    if engine is None:
        raise SystemExit("couldn't connect to database")

    price_df = pd.read_csv(os.path.join(dataIn_path, "price_data.csv"))
    gen_df = pd.read_csv(os.path.join(dataIn_path, "gen_data.csv"))

    #cleaner way to write this?
    state_df = get_states(price_df)
    price_sector_df = get_price_sectors(price_df)
    gen_sector_df = get_gen_sectors(gen_df)
    fuel_df = get_fuels(gen_df)
    date_df = get_dates(price_df)

    #load tables
    load_table(engine, "dim_state", state_df)
    load_table(engine, "dim_pricesector", price_sector_df)
    load_table(engine, "dim_gensector", gen_sector_df)
    load_table(engine, "dim_fuel", fuel_df)
    load_table(engine, "dim_date", date_df)

    maps = get_all_maps(engine)
    print("Dimension maps ready:", {k: f"{len(v)} entries" for k, v in maps.items()})
