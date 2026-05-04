-- Basic Aggregations

-- Query1: Average electricity prices by state
-- across the enitre date range and all sectors
SELECT ds.state_short, ROUND(AVG(fp.price_per_kwh),2) as avg_price
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
order by total_gen desc

-- Query 3: average price by state and sector
SELECT ds.state_short, dp.sector_name, ROUND(AVG(fp.price_per_kwh),2) as avg_price
FROM fact_prices fp
JOIN dim_state ds ON fp.state_id = ds.state_id
join dim_pricesector dp on fp.sector_id =dp.sector_id
WHERE fp.price_per_kwh  is not NULL
GROUP BY ds.state_short, dp.sector_name
ORDER BY avg_price desc

-- Query 4: Top 5 states by NG price
select ds.state_short , ROUND(avg(ffp.price_per_mmbtu ),2) as avgNG_price
from fact_fuel_prices ffp
join dim_state ds on ffp.state_id = ds.state_id 
join dim_fuel df on ffp.fuel_id = df.fuel_id 
where df.fuel_short = 'NG'
group by ds.state_short 
limit 5

-- Query 5 — States with consistently high prices
SELECT ds.state_short, ROUND(AVG(fp.price_per_kwh),2) as avg_price
FROM fact_prices fp
JOIN dim_state ds ON fp.state_id = ds.state_id
GROUP BY ds.state_short
having AVG(fp.price_per_kwh )> 15
ORDER BY avg_price DESC

-- Query 6 — Generation mix for California (id = 5)
-- what percentage of total generation comes from each fuel type
select ds.state_short, df.fuel_long, SUM(fg.generation ) as total_gen, 
	Round((SUM(fg.generation)*100/(select SUM(fg.generation) 
	from fact_gen fg 
	where fg.state_id = 5)),2) as percent_gen
from fact_gen fg
join dim_state ds  on fg.state_id = ds.state_id 
join dim_fuel df  on fg.fuel_id = df.fuel_id 
where ds.state_short = 'CA'
group by ds.state_short, df.fuel_long




