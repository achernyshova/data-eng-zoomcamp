'''
# Data Engineering Zoomcamp 2026 - Module 3 Homework

This repository contains the complete solution for the Module 3 homework assignment on Data Warehousing with Google BigQuery.

## Overview

The homework involves loading the 2024 Yellow Taxi trip data into Google Cloud Storage (GCS), creating external and materialized tables in BigQuery, and answering a series of questions related to query optimization, partitioning, and clustering.

## BigQuery Setup

The following SQL script sets up the necessary BigQuery dataset and tables. Remember to replace `your-project-id` and `your-bucket-name` with your actual GCP project ID and GCS bucket name.

```sql
-- Module 3 Homework: BigQuery Setup
-- Data Engineering Zoomcamp 2026

-- ============================================
-- STEP 1: Create Dataset
-- ============================================
-- Create a dataset to hold our tables
-- Note: Replace 'your-project-id' with your actual GCP project ID

CREATE SCHEMA IF NOT EXISTS `your-project-id.de_zoomcamp_hw3`
OPTIONS(
  description="Data Engineering Zoomcamp Module 3 Homework - Yellow Taxi 2024",
  location="US"
);

-- ============================================
-- STEP 2: Create External Table
-- ============================================
-- Create an external table pointing to the parquet files in GCS
-- Note: Replace 'your-bucket-name' with your actual GCS bucket name

CREATE OR REPLACE EXTERNAL TABLE `your-project-id.de_zoomcamp_hw3.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket-name/yellow_tripdata_2024-*.parquet']
);

-- ============================================
-- STEP 3: Create Materialized Table
-- ============================================
-- Create a regular (materialized) table from the external table
-- This table is NOT partitioned or clustered (as per homework requirements)

CREATE OR REPLACE TABLE `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
AS
SELECT *
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_external`;
```

## Homework Questions and Answers

Here are the solutions to the homework questions, along with the SQL queries used to derive the answers.

### Question 1: Counting Records

**Question:** What is the count of records for the 2024 Yellow Taxi Data?

**Answer:** `20,332,093`

```sql
SELECT COUNT(*) as total_records
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
```

### Question 2: Data Read Estimation

**Question:** What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**Answer:** `0 MB for the External Table and 155.12 MB for the Materialized Table`

```sql
-- Query on External Table (Estimated: ~0 MB)
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_external`;

-- Query on Materialized Table (Estimated: ~155.12 MB)
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
```

### Question 3: Understanding Columnar Storage

**Question:** Why are the estimated number of Bytes different when querying one vs. two columns?

**Answer:** `BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`

```sql
-- Query 1: Single column (Estimated: ~77.56 MB)
SELECT PULocationID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;

-- Query 2: Two columns (Estimated: ~155.12 MB)
SELECT PULocationID, DOLocationID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
```

### Question 4: Counting Zero Fare Trips

**Question:** How many records have a fare_amount of 0?

**Answer:** `8,333`

```sql
SELECT COUNT(*) as zero_fare_trips
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
WHERE fare_amount = 0;
```

### Question 5: Partitioning and Clustering

**Question:** What is the best strategy to make an optimized table in BigQuery if your query will always filter based on `tpep_dropoff_datetime` and order the results by `VendorID`?

**Answer:** `Partition by tpep_dropoff_datetime and Cluster on VendorID`

```sql
-- Create the optimized table
CREATE OR REPLACE TABLE `your-project-id.de_zoomcamp_hw3.yellow_taxi_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
```

### Question 6: Partition Benefits

**Question:** What are the estimated bytes for a query retrieving distinct `VendorID`s between March 1st and 15th, 2024, on both the non-partitioned and partitioned tables?

**Answer:** `310.31 MB for non-partitioned table and 285.64 MB for the partitioned table`

```sql
-- Query on NON-PARTITIONED table (Estimated: ~310.31 MB)
SELECT DISTINCT VendorID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';

-- Query on PARTITIONED table (Estimated: ~285.64 MB)
SELECT DISTINCT VendorID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';
```

### Question 7: External Table Storage

**Question:** Where is the data stored in the External Table you created?

**Answer:** `GCP Bucket`

### Question 8: Clustering Best Practices

**Question:** It is best practice in BigQuery to always cluster your data.

**Answer:** `False`

### Question 9: Understanding Table Scans

**Question:** Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**Answer:** `0 MB`. BigQuery optimizes `COUNT(*)` queries by reading the table's metadata, which includes the total row count. This avoids a full table scan, resulting in zero bytes read.

```sql
-- This query reads 0 bytes from metadata
SELECT COUNT(*) as total_records
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
```
'''
