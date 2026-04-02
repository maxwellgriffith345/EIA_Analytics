-- Fact Tables

CREATE TABLE fact_gen(
  date_id INT,
  state_id VARCHAR(50),
  fuel_id VARCHAR(50),
  generation DECIMAL(10, 2),
  FOREIGN KEY (date_id) REFRENCES dim_date(date_id),
  FOREIGN KEY (state_id) REFRENCES dim_state(state_id),
  FOREIGN KEY (fuel_id) REFRENCES dim_fuel(fuel_id)
);


CREATE TABLE fact_prices(

);

CREATE TABLE fact_fuel_prices(

);


-- Dimension Tables
CREATE TABLE dim_date(

);

CREATE TABLE dim_state(

);

CREATE TABLE dim_fuel(

);
