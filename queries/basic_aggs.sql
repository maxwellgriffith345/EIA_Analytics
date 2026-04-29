-- Basic Aggregations


-- Query1: Average electricity prices by state
-- across the enitre date range and all sectors
SELECT ds.state_short, AVG(fp.price_per_kwh) as avg_price
FROM fact_prices fp
JOIN dim_state ds ON fp.state_id = ds.state_id
GROUP BY ds.state_short
ORDER BY avg_price DESC

-- Query2: Total geneartion for each fuel
-- across all states and years
select df.fuel_long, SUM(fg.generation ) as total_gen
from fact_gen fg 
join dim_fuel df on fg.fuel_id  = df.fuel_id 
group by df.fuel_long
order by total_gen DESC