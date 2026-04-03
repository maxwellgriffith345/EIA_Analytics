## Questions

Why use a config.py file to store the API key and note a .env file?


If the API return limit is 5,000 records how do I return a full years worth of records without duplicates?


examples
https://medium.com/@laura.marshall.w/using-the-eia-api-python-california-solar-generation-for-2024-e4d4148661fd

https://realpython.com/python-requests/#make-a-get-request


### Star vs Snowflake Schema
https://www.datacamp.com/blog/star-schema-vs-snowflake-schema


## Database Setup
- fact tables should have PRIMARY KEY to avoid duplicate rows and increase efficieny
  - use a surrogate key or a composite key
  - could use the date?
  - it doesn't need to mean anything but can be used as an index and unique constraint
- For date columns you could use the type YYYYMM to make it more readable and naturally sortable
- Key columns should have NOT NULL constraints
- Dimension tables must be created before Facts tables that refrence the dim tables as foreign keys
- Use SERIAL type to create an auto-incementing integer column used for primary keys
- you want all the columns in a dim table to have a NOT NULL constraint
- use UNIQUE constraints in dim tables where appropraite ie state codes
- UNIQUE constraint with multiple columns ie UNIQUE (date_id, state_id, sector_id) ensure that the each comination fo those column values appears only once in the all the rows. so we can have two rows that have the same date, sate and sector
- - IF the fuel_id and sate_id are SERIAL so they are auto incremented then how do input that data when I load those values into the fact tables?
  - create and load the dim table first
  - query the table to pull all of the ids and the name column into a dictornary
  - use that dictornary as a map on the fact data frame to load in the ids and then load the fact data frame into the database
