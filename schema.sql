-- Dimension Tables
CREATE TABLE dim_date(
  date_id INT PRIMARY KEY, -- Format: YYYYMM
  year INT NOT NULL,
  month INT NOT NULL,
  month_name VARCHAR(20) NOT NULL,
  quarter INT NOT NULL
);

CREATE TABLE dim_state(
  state_id SERIAL PRIMARY KEY,
  state_short VARCHAR(5) NOT NULL UNIQUE,
  state_long VARCHAR(50) NOT NULL
);

CREATE TABLE dim_fuel(
  fuel_id SERIAL PRIMARY KEY,
  fuel_short VARCHAR(50) NOT NULL UNIQUE,
  fuel_long VARCHAR(50)
);

-- Price sector, customer sector
CREATE TABLE dim_pricesector(
  sector_id SERIAL PRIMARY KEY,
  sector_name VARCHAR(50) NOT NULL
);

CREATE TABLE dim_gensector(
  sector_id SERIAL PRIMARY KEY,
  sector_name VARCHAR(50) NOT NULL
);

-- Fact Tables
CREATE TABLE fact_gen(
  gen_id SERIAL PRIMARY KEY,
  date_id INT NOT NULL,
  state_id INT NOT NULL,
  sector_id INT NOT NULL,
  fuel_id INT NOT NULL,
  generation DECIMAL(12, 2),
  FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (state_id) REFERENCES dim_state(state_id),
  FOREIGN KEY (fuel_id) REFERENCES dim_fuel(fuel_id),
  FOREIGN KEY (sector_id) REFERENCES dim_gensector(sector_id),
  UNIQUE(date_id, state_id, fuel_id, sector_id)
);

CREATE TABLE fact_prices(
  price_id SERIAL PRIMARY KEY,
  date_id INT NOT NULL,
  state_id INT NOT NULL,
  sector_id INT NOT NULL,
  price_per_kwh DECIMAL(10,4),
  FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (state_id) REFERENCES dim_state(state_id),
  FOREIGN KEY (sector_id) REFERENCES dim_pricesector(sector_id),
  UNIQUE (date_id, state_id, sector_id)
);

CREATE TABLE fact_fuel_prices(
  fuel_price_id SERIAL PRIMARY KEY,
  date_id INT NOT NULL,
  state_id INT NOT NULL,
  fuel_id INT NOT NULL,
  price_per_mmbtu DECIMAL(10,4),
  FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (state_id) REFERENCES dim_state(state_id),
  FOREIGN KEY (fuel_id) REFERENCES dim_fuel(fuel_id),
  UNIQUE (date_id, state_id, fuel_id)
);
