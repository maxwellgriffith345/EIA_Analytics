import pandas as pd
from extract import get_gen_data, get_price_data, get_fuel_data
from load_dims import get_engine, load_table, get_all_maps
from load_dims import get_states, get_price_sectors,get_gen_sectors, get_fuels, get_dates
from transform import get_state_map_long_to_short, get_state_map_short_to_long
from transform import transform_gen, transform_prices, transform_fuel


def run_pipeline():

    #-- Connect to DB ---
    print("connecting to database")
    engine = get_engine()
    if engine is None:
        raise SystemExist("couldn't connect to database")

    #-- API requests ---
    print("\npulling data from EIA API")
    gen_raw = get_gen_data()
    fuel_raw = get_fuel_data()
    price_raw = get_price_data()


    #-- Load dim tables ---
    print("\nloading dimension tables")
    state_df = get_states(price_raw)
    price_sector_df = get_price_sectors(price_raw)
    gen_sector_df = get_gen_sectors(gen_raw)
    fuel_df = get_fuels(gen_raw)
    date_df = get_dates(price_raw)

    load_table(engine, "dim_state", state_df)
    load_table(engine, "dim_pricesector", price_sector_df)
    load_table(engine, "dim_gensector", gen_sector_df)
    load_table(engine, "dim_fuel", fuel_df)
    load_table(engine, "dim_date", date_df)

    #-- pull ID maps
    maps = get_all_maps(engine)
    print("Dimension maps ready:", {k: f"{len(v)} entries" for k, v in maps.items()})

    #-- Transform Data ---
    print("\n transforming data")
    long_to_short = get_state_map_long_to_short(price_raw)
    short_to_long = get_state_map_short_to_long(price_raw)

    gen_clean = transform_gen(gen_raw, long_to_short, maps)
    price_clean = transform_prices(price_raw, maps)
    fuel_clean = transform_fuel(fuel_raw, short_to_long, maps)

    #--data check before loading
    issues = []
    if gen_clean["state_id"].isna().any():   issues.append("gen: missing state_id")
    if gen_clean["fuel_id"].isna().any():    issues.append("gen: missing fuel_id")
    if gen_clean["sector_id"].isna().any():  issues.append("gen: missing sector_id")
    if price_clean["state_id"].isna().any(): issues.append("prices: missing state_id")
    if price_clean["sector_id"].isna().any():issues.append("prices: missing sector_id")
    if fuel_clean["state_id"].isna().any():  issues.append("fuel: missing state_id")
    if fuel_clean["fuel_id"].isna().any():   issues.append("fuel: missing fuel_id")

    if issues:
        for issue in issues:
            print(f"  WARNING: {issue}")
        raise SystemExit("Fix mapping issues before loading fact tables.")
    else:
        print("  All sanity checks passed")

    #-- Load fact tables ---
    print("\n loading facts tables")
    try:
        load_table(engine, "fact_gen", gen_clean)
        load_table(engine, "fact_prices", price_clean)
        load_table(engine, "fact_fuel_prices", fuel_clean)
    except Exception as e:
        print(f"Error lading fact tables: {e}")
        raise

    print("Pipeline finished")


if __name__ == '__main__':
    run_pipeline()
