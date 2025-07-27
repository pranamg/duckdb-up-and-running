-- Invoke this script in the DuckDB CLI or a compatible environment
duckdb
-- DuckDB workflow: Convert a large flights.csv file into partitioned, GitHub-friendly Parquet files
.mode line
-- 1. Inspect the CSV file (preview first 10 rows)
SELECT * FROM 'flights.csv' LIMIT 10;

-- 2. (Optional) Export the entire CSV to Parquet for reference (not for GitHub upload)
COPY (SELECT * FROM 'flights.csv') TO 'flights.parquet' (FORMAT 'parquet');

-- 3. Examine the inferred schema and data types
DESCRIBE SELECT * FROM 'flights.csv';

-- 4. Check for missing values and type consistency (replace column_name with actual column names as needed)
-- Example: Check for NULLs in a column
-- SELECT COUNT(*) FROM 'flights.csv' WHERE column_name IS NULL OR column_name = '';
-- Example: Check for non-integer values in a column
-- SELECT column_name, COUNT(*) FROM 'flights.csv' WHERE TRY_CAST(column_name AS INTEGER) IS NULL GROUP BY column_name;

-- 5. Get summary statistics for numeric columns (replace column_name as needed)
-- SELECT AVG(column_name), MIN(column_name), MAX(column_name) FROM 'flights.csv';

-- 6. Create a DuckDB database file to persist tables (run DuckDB CLI as: duckdb flights.duckdb)

-- 7. Create the flights table with explicit types based on schema analysis
CREATE TABLE flights (
  YEAR BIGINT,
  MONTH BIGINT,
  DAY BIGINT,
  DAY_OF_WEEK BIGINT,
  AIRLINE VARCHAR,
  FLIGHT_NUMBER BIGINT,
  TAIL_NUMBER VARCHAR,
  ORIGIN_AIRPORT VARCHAR,
  DESTINATION_AIRPORT VARCHAR,
  SCHEDULED_DEPARTURE VARCHAR,
  DEPARTURE_TIME VARCHAR,
  DEPARTURE_DELAY BIGINT,
  TAXI_OUT BIGINT,
  WHEELS_OFF VARCHAR,
  SCHEDULED_TIME BIGINT,
  ELAPSED_TIME BIGINT,
  AIR_TIME BIGINT,
  DISTANCE BIGINT,
  WHEELS_ON VARCHAR,
  TAXI_IN BIGINT,
  SCHEDULED_ARRIVAL VARCHAR,
  ARRIVAL_TIME VARCHAR,
  ARRIVAL_DELAY BIGINT,
  DIVERTED BIGINT,
  CANCELLED BIGINT,
  CANCELLATION_REASON VARCHAR,
  AIR_SYSTEM_DELAY BIGINT,
  SECURITY_DELAY BIGINT,
  AIRLINE_DELAY BIGINT,
  LATE_AIRCRAFT_DELAY BIGINT,
  WEATHER_DELAY BIGINT
);

-- 8. Import the CSV data into the flights table
COPY flights FROM 'flights.csv' (HEADER, DELIMITER ',');

-- 9. Review the table schema
PRAGMA table_info(flights);

-- 10. Check min/max values for numeric columns to validate types and spot outliers
SELECT 
  MIN(YEAR), MAX(YEAR), 
  MIN(MONTH), MAX(MONTH),
  MIN(DAY), MAX(DAY),
  MIN(DAY_OF_WEEK), MAX(DAY_OF_WEEK),
  MIN(FLIGHT_NUMBER), MAX(FLIGHT_NUMBER),
  MIN(DEPARTURE_DELAY), MAX(DEPARTURE_DELAY),
  MIN(TAXI_OUT), MAX(TAXI_OUT),
  MIN(SCHEDULED_TIME), MAX(SCHEDULED_TIME),
  MIN(ELAPSED_TIME), MAX(ELAPSED_TIME),
  MIN(AIR_TIME), MAX(AIR_TIME),
  MIN(DISTANCE), MAX(DISTANCE),
  MIN(TAXI_IN), MAX(TAXI_IN),
  MIN(ARRIVAL_DELAY), MAX(ARRIVAL_DELAY),
  MIN(DIVERTED), MAX(DIVERTED),
  MIN(CANCELLED), MAX(CANCELLED),
  MIN(AIR_SYSTEM_DELAY), MAX(AIR_SYSTEM_DELAY),
  MIN(SECURITY_DELAY), MAX(SECURITY_DELAY),
  MIN(AIRLINE_DELAY), MAX(AIRLINE_DELAY),
  MIN(LATE_AIRCRAFT_DELAY), MAX(LATE_AIRCRAFT_DELAY),
  MIN(WEATHER_DELAY), MAX(WEATHER_DELAY)
FROM flights;

-- 11. Preview some VARCHAR columns to check for unexpected values
SELECT AIRLINE, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME, WHEELS_OFF, WHEELS_ON, SCHEDULED_ARRIVAL, ARRIVAL_TIME, CANCELLATION_REASON
FROM flights
LIMIT 10;

-- 12. Partition the data by month for GitHub-friendly file sizes
-- This creates 12 Parquet files, each for a month, with a logical folder structure
-- Create folders: flights/year=2015/month=01/, ..., flights/year=2015/month=12/
-- Move and rename files after export as needed

COPY (SELECT * FROM flights WHERE MONTH = 1) TO 'flights/year=2015/month=01/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 2) TO 'flights/year=2015/month=02/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 3) TO 'flights/year=2015/month=03/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 4) TO 'flights/year=2015/month=04/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 5) TO 'flights/year=2015/month=05/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 6) TO 'flights/year=2015/month=06/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 7) TO 'flights/year=2015/month=07/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 8) TO 'flights/year=2015/month=08/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 9) TO 'flights/year=2015/month=09/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 10) TO 'flights/year=2015/month=10/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 11) TO 'flights/year=2015/month=11/flights.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM flights WHERE MONTH = 12) TO 'flights/year=2015/month=12/flights.parquet' (FORMAT 'parquet');

-- 13. (Optional) For GitHub, upload only these partitioned files or a small sample.
-- For very large datasets, consider uploading only a sample:
COPY (SELECT * FROM 'flights.csv' LIMIT 1000) TO 'flights_sample.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM 'flights.csv' LIMIT 1000) TO 'flights_sample.parquet' (FORMAT 'parquet');

-- 14. Document in your README that full data is partitioned and available via cloud storage or on request.

-- End



