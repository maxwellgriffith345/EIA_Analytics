import pandas
import os
import psycopg2



def get_states():
    dataIn_path = "data/raw"
    price_df = pd.read_csv(os.path.join(dataIn_path, "price_data.csv"))
    state_series = pd.Series(price_df.stateDescription.values, index=price_df.stateid).drop_duplicates()
    state_df = state_series.reset_index()
    state_df = state_df.rename(columns = {"stateid": "state_short", 0: "state_long"})

    return state_df






# returns a dictonary of the maps state_id, fuel_id, sector_id
def fetch_all_maps():
