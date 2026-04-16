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

-Docker stuff
  - differnce between dockerfile and compose file?
  - should I link the init schema to a volume or use a dockerfile to copy it?
  - how to create a volume for the db?
  - the advantage of docker compose is you can spin up and down multipe contrainers for your entire project
  - https://docs.docker.com/guides/pre-seeding/
  - list tables in database: psql \dt


## Extracting the data
- how to deal with the 5000 row limit when pulling a years worth of data?

## Connecting Postgre with Python (SQLAlchemy)
-library psycopg2
- QUESTION: connect via a local unix socket vs connect over the network
  - need to connect over the network when working with a docker container
  - local socker connection vs TCP connection
  - QUESTION: what is a TCP connection
- https://docs.sqlalchemy.org/en/21/core/connections.html#sqlalchemy.engine.CursorResult
- https://docs.sqlalchemy.org/en/21/core/connections.html#basic-usage
- https://docs.sqlalchemy.org/en/21/tutorial/data.html#tutorial-working-with-data
- the engine creates the connection to the database, connection factory
    - do not create per object or per functino call, once is enough
- the connection (created by the enigee) calls SQL statements
    - should be used in a with statement to manage context
- How do I write the dim functions and use them in another file without the eniginee or connection? or do I pass them the connection? maybe I write it as a class?
