-- Load Data into Flights DB

-- Load airports table
COPY airports FROM 'airports.csv' (HEADER, DELIMITER ',');

-- Load airlines table
COPY airlines FROM 'airlines.csv' (HEADER, DELIMITER ',');

-- Load airports_location table
COPY airports_location FROM 'airports_location.csv' (HEADER, DELIMITER ',');

-- Load flights table from all 12 months parquet files
COPY flights FROM 'year=2015/month=01/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=02/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=03/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=04/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=05/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=06/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=07/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=08/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=09/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=10/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=11/flights.parquet' (FORMAT 'parquet');
COPY flights FROM 'year=2015/month=12/flights.parquet' (FORMAT 'parquet');
