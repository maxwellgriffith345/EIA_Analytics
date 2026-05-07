-- this is where the window functions go

-- Query 10 Rank states by average electricity price
-- CTE
with state_average as (
	select ds.state_short as state, AVG(fp.price_per_kwh) as average
	from fact_prices fp
	join dim_state ds on fp.state_id  = ds.state_id 
	group by ds.state_short

)

select state, average,
	rank() over (order by average DESC) as price_rank
from state_average

-- Query 10 but with sub query
select state, average,
	rank() over (order by average DESC) as price_rank
from (select ds.state_short as state, AVG(fp.price_per_kwh) as average
	from fact_prices fp
	join dim_state ds on fp.state_id  = ds.state_id 
	group by ds.state_short 
)

--Query 11 — Year over year price change by state
-- CTE average price per state per year
with yearly_state_average as (
	select ds.state_short as state, dd.year as date_year , Round(AVG(fp.price_per_kwh),2) as average
	from fact_prices fp
	join dim_state ds on fp.state_id  = ds.state_id
	join dim_date dd on fp.date_id = dd.date_id
	group by ds.state_short, dd.year 
),
lag_average as ( 
	select state, date_year, average, 
	lag(average,1,0) OVER(partition by state order by date_year) as lag_avg
	from yearly_state_average ysa
)

select state, date_year, average, (average - lag_avg) as Yoy
from lag_average
order by state, date_year

-- Query 12 — Running total of generation by state
-- california and natural gas
with total_gen as ( 
	select fg.date_id , SUM(fg.generation )  as total_gen
	from fact_gen fg
	join dim_state ds on ds.state_id = fg.state_id 
	join dim_fuel df on df.fuel_id = fg.fuel_id 
	where ds.state_short = 'CA' and df.fuel_short = 'NG'
	group by fg.date_id 
)

select date_id, total_gen,
	SUM(total_gen) over (order by date_id) as running_gen
from total_gen as tg

-- Rank fuel types within each state by total generation
-- select top ranked fuel
with state_fuel_gen as(      
	select ds.state_short, df.fuel_short , SUM(fg.generation )  as total_gen
	from fact_gen fg
	join dim_state ds on ds.state_id = fg.state_id 
	join dim_fuel df on df.fuel_id = fg.fuel_id 
	where not fuel_short = 'ALL'
	group by ds.state_short, df.fuel_short

)

select *
from (
	select *, rank () over (partition by stg.state_short order by stg. total_gen DESC) as ranked
	from state_fuel_gen stg 
) as ranked_fuel
where ranked_fuel.ranked = 1


