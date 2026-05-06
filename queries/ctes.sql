-- This is where the ctes go

-- Query 7
-- Average price by state, ranked for states above national average
with cte_avg_price as (
  	SELECT ds.state_short, ROUND(AVG(fp.price_per_kwh), 2) as avg_price
    FROM fact_prices fp
    JOIN dim_state ds ON fp.state_id = ds.state_id
    GROUP BY ds.state_short
)

select * 
from cte_avg_price
where avg_price > (select avg(fp.price_per_kwh) from fact_prices fp)
order by avg_price desc

-- Query 8: FILTER OUT "ALL FUELS"
-- Generation mix percentage by state
--cte generation per state per fuel type
with cte_gen_state_fuel as(
	select ds.state_short, df.fuel_long, SUM(fg.generation ) as fuel_gen 
	from fact_gen fg
	join dim_state ds  on fg.state_id = ds.state_id 
	join dim_fuel df  on fg.fuel_id = df.fuel_id 
	group by ds.state_short, df.fuel_long
),
cte_total_gen_state as(
	select ds.state_short, SUM(fg.generation) as state_gen
	from fact_gen fg 
	join dim_state ds on fg.state_id = ds.state_id
	group by ds.state_short 
)

select gst.state_short, gst.fuel_long, gst.fuel_gen, gs.state_gen,
	Round((gst.fuel_gen *100/gs.state_gen),2) as percentage_gen
from cte_gen_state_fuel gst
join cte_total_gen_state gs on gst.state_short = gs.state_short
order by gst.state_short;


-- Query 9 TODO — Month with peak electricity prices per state
-- CTE average price by state and month
with cte_state_month as(
	SELECT ds.state_short as state, fp.date_id as date, ROUND(AVG(fp.price_per_kwh),2) as avg_price
	FROM fact_prices fp
	JOIN dim_state ds ON fp.state_id = ds.state_id
	GROUP BY ds.state_short, fp.date_id
),
cte_max_price as (
	select csm.state, MAX(csm.avg_price) as max_price
	from cte_state_month csm
	group by csm.state
)

select csm.state, csm.date, cmp.max_price
from cte_state_month csm
join cte_max_price cmp
on csm.state = cmp.state and csm.avg_price = cmp.max_price
order by csm.state
