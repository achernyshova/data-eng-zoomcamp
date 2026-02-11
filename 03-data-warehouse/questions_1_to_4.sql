-- Module 3 Homework: Questions 1-4
-- Data Engineering Zoomcamp 2026

-- ============================================
-- QUESTION 1: Counting Records
-- ============================================
-- What is count of records for the 2024 Yellow Taxi Data?
-- Options: 65,623 | 840,402 | 20,332,093 | 85,431,289

SELECT COUNT(*) as total_records
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;

-- ANSWER: 20,332,093
-- This represents all Yellow Taxi trips from January to June 2024


-- ============================================
-- QUESTION 2: Data Read Estimation
-- ============================================
-- Write a query to count the distinct number of PULocationIDs 
-- for the entire dataset on both tables.
-- What is the estimated amount of data that will be read?

-- Query on External Table
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_external`;
-- Check the estimated bytes in BigQuery UI (top right corner before execution)
-- Expected: ~0 MB (external tables don't show estimates until execution)

-- Query on Materialized Table
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
-- Check the estimated bytes in BigQuery UI
-- Expected: ~155.12 MB

-- ANSWER: 0 MB for the External Table and 155.12 MB for the Materialized Table
-- External tables don't provide estimates before query execution
-- Materialized tables scan the PULocationID column (stored in BigQuery)


-- ============================================
-- QUESTION 3: Understanding Columnar Storage
-- ============================================
-- Write a query to retrieve PULocationID from the table.
-- Then write a query to retrieve PULocationID AND DOLocationID.
-- Why are the estimated number of Bytes different?

-- Query 1: Single column
SELECT PULocationID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
-- Check estimated bytes in BigQuery UI
-- Expected: ~77.56 MB (approximately)

-- Query 2: Two columns
SELECT PULocationID, DOLocationID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
-- Check estimated bytes in BigQuery UI
-- Expected: ~155.12 MB (approximately double)

-- ANSWER: BigQuery is a columnar database, and it only scans the specific 
-- columns requested in the query. Querying two columns (PULocationID, DOLocationID) 
-- requires reading more data than querying one column (PULocationID), leading to 
-- a higher estimated number of bytes processed.

-- EXPLANATION:
-- In columnar storage, each column is stored separately.
-- When you query one column, BigQuery only reads that column's data.
-- When you query two columns, BigQuery reads both columns' data.
-- This is a key advantage of columnar databases for analytical workloads.


-- ============================================
-- QUESTION 4: Counting Zero Fare Trips
-- ============================================
-- How many records have a fare_amount of 0?
-- Options: 128,210 | 546,578 | 20,188,016 | 8,333

SELECT COUNT(*) as zero_fare_trips
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
WHERE fare_amount = 0;

-- ANSWER: 8,333
-- These could be cancelled trips, test records, or data quality issues

-- Additional analysis (optional):
SELECT 
  COUNT(*) as zero_fare_count,
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`) as percentage
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
WHERE fare_amount = 0;
-- This shows the percentage of zero-fare trips in the dataset
