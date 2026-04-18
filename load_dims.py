import pandas
import os
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def get_engine():
    engine = create_engine(
    "postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/eiadb"
    )

    # 2. Test the connection
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Connection successful:", result.fetchone())
    except OperationalError:
        print("connection failed")

    return engine

def get_states():
    dataIn_path = "data/raw"
    price_df = pd.read_csv(os.path.join(dataIn_path, "price_data.csv"))
    state_series = pd.Series(price_df.stateDescription.values, index=price_df.stateid).drop_duplicates()
    state_df = state_series.reset_index()
    #df column names must match the schemal names
    state_df = state_df.rename(columns = {"stateid": "state_short", 0: "state_long"})
    return state_df


def get_sectors():

def get_fuels():

def get_dates():

def load_table(tableName, engine, df):
    with engine.connect() as conn:
        df.to_sql(name=tableName, con=conn, if_exists='append', index=False)
        conn.commit()
    print("Loaded", len(df), f"rows into {tableName}")

def get_states_map():
    #state_short, stateid
    with engine.connect() as conn:
        result = conn.execute(text("SELECT state_short, state_id FROM dim_state")).fetchall()
    state_map = dict(result)
    return state_map


# returns a dictonary of the maps state_id, fuel_id, sector_id
def get_all_maps():
