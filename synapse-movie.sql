-- Synapse Serverless SQL / OnDemand SQL
-- When we run the query, how much data we scan in the query, 1 TB = 5 $
-- Queries are distributed, run across cluster
-- Data Exploration - we have data in data lake, I want to see and explore and learn from there
--   we are not yet ready for modelling (ML not ready, Analytics not ready)

-- we have external table
-- external table - meta data (schema) stored seprately + data stored in data lake

--we have have database with many external table

-- create db
-- use the use data base option on ui or sql statement to change db from master to moviedb
CREATE DATABASE moviedb;

USE moviedb; 


-- create external file format
-- create data source
-- create exteranal table

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)
                )

-- movielens is container name
-- nsgkazurestorage is storage account name
-- linking external data lake [this will work we have established permission]
CREATE EXTERNAL DATA SOURCE [movieset_ds]
  WITH (LOCATION='abfss://movielens@nsgkazurestorage.dfs.core.windows.net')


-- columns are from tables
-- externanl 

CREATE EXTERNAL TABLE movies  (
     id INT,
    title VARCHAR(500),
    genres VARCHAR(250)
) WITH (
    -- location/path within data source
    LOCATION='movies/movies.csv',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    -- data format is delimited text, we skipped first row
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from movies;


CREATE EXTERNAL TABLE ratings (
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp BIGINT 
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='ratings/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * FROM ratings;



-- GROUP BY, HAVING, ORDER BY 
-- external table with in dedicated sql

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
GROUP BY (movieId)
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;


-- join external table from data lake with native dedi sql pool table

SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
JOIN movies 
ON movies.id = ratings.movieId
GROUP BY movieId, title
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;

-- CETAS - Create External Table As Select

-- we create a dedi pool table called popular_movies
-- from the query 

CREATE EXTERNAL TABLE popular_movies 
WITH(
    LOCATION='popular-movies/popular.csv', 
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
     )
AS
SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
    FROM ratings 
    JOIN movies 
    ON movies.id = ratings.movieId
    GROUP BY movieId, title
    HAVING COUNT(userId) >= 100;

SELECT * from popular_movies;

SELECT * FROM sys.external_tables;
SELECT * FROM sys.external_data_sources;
SELECT * FROM sys.external_file_formats;


SELECT LOCATION from  sys.external_tables;
SELECT LOCATION from  sys.external_data_sources;
