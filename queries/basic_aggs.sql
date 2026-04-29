-- Basic Aggregations


-- Average fuel price over all years by state sorted by average price
SELECT ds.state_short, AVG(fp.price_per_kwh) as avg_price
FROM fact_prices fp
JOIN dim_state ds ON fp.state_id = ds.state_id
GROUP BY ds.state_short
ORDER BY avg_price DESC
