# EIA_Analytics
Analysis of EIA Open Data


# STEP 1: Extract Data Using EIA API
- Generation and prices by state and fuel
- time, geography and fuel dimensions
- 3 fact tables and shared dimensions

1. Electricty generation by state and fuel (monthly)
   - [Gen API route](https://www.eia.gov/opendata/browser/electricity/electric-power-operational-data)
   - Fields: period, stateid, fueltypeid, geneartion
   - this is the primary fact table

2. Electricty retail prices by state and sectory (monthly)
   - [Price API Route](https://www.eia.gov/opendata/browser/electricity/retail-sales)
   - Fields: period, stateid, sectorid, price
   - this is the second fact table
3. Fuel Cost Data (natural gas monthly)
   - [Fuel API Route](https://www.eia.gov/opendata/browser/natural-gas/pri/sum)
   - fitler by "process-name": "Electric Power Pirce"
   - the state format is slightly different than the other tables and will need cleaning
   - units is $/MCF so will need to convert to mmbtu if desired

challenges: the API only returns 5000 rows per call, when pulling data for multiple years that limit get's hit. Solve was to pagenate the calls and use the offset to return all the rows.

challenges: when using high levels of offsett ie 2000 the API can error out so I needed to add retry functionality

# STEP 2: Database Schema
