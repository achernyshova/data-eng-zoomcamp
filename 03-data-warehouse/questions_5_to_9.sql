-- Module 3 Homework: Questions 5-9
-- Data Engineering Zoomcamp 2026

-- ============================================
-- QUESTION 5: Partitioning and Clustering
-- ============================================
-- What is the best strategy to optimize a table if queries will always:
-- - Filter based on tpep_dropoff_datetime
-- - Order results by VendorID
--
-- Options:
-- 1. Partition by tpep_dropoff_datetime and Cluster on VendorID
-- 2. Cluster on tpep_dropoff_datetime and Cluster on VendorID
-- 3. Cluster on tpep_dropoff_datetime and Partition by VendorID
-- 4. Partition by tpep_dropoff_datetime and Partition by VendorID

-- ANSWER: Partition by tpep_dropoff_datetime and Cluster on VendorID

-- EXPLANATION:
-- - Partitioning is best for columns used in WHERE filters (tpep_dropoff_datetime)
-- - Clustering is best for columns used in ORDER BY or GROUP BY (VendorID)
-- - You can only partition by one column, but cluster by multiple
-- - Partitioning reduces the amount of data scanned
-- - Clustering improves query performance within partitions

-- Create the optimized table:
CREATE OR REPLACE TABLE `your-project-id.de_zoomcamp_hw3.yellow_taxi_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;

-- Verify the partitioned table
SELECT COUNT(*) as total_records
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_optimized`;


-- ============================================
-- QUESTION 6: Partition Benefits
-- ============================================
-- Write a query to retrieve distinct VendorIDs between 
-- tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
-- Compare estimated bytes: non-partitioned vs partitioned table

-- Query on NON-PARTITIONED (materialized) table:
SELECT DISTINCT VendorID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';
-- Check estimated bytes in BigQuery UI
-- Expected: ~310.31 MB (scans entire table)

-- Query on PARTITIONED table:
SELECT DISTINCT VendorID
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';
-- Check estimated bytes in BigQuery UI
-- Expected: ~285.64 MB (scans only relevant partitions - March 1-15)

-- ANSWER: 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

-- EXPLANATION:
-- The partitioned table scans less data because:
-- 1. It only reads partitions for March 1-15, 2024
-- 2. The non-partitioned table must scan the entire dataset
-- 3. For larger date ranges, the difference would be more dramatic
-- 4. Partitioning by date is one of the most effective optimization strategies


-- ============================================
-- QUESTION 7: External Table Storage
-- ============================================
-- Where is the data stored in the External Table you created?
-- Options: Big Query | Container Registry | GCP Bucket | Big Table

-- ANSWER: GCP Bucket

-- EXPLANATION:
-- External tables in BigQuery:
-- - Store metadata in BigQuery (schema, location)
-- - Store actual data in external storage (GCS, Drive, Bigtable, etc.)
-- - In our case, data is in GCS bucket as Parquet files
-- - BigQuery queries the data directly from GCS without importing it
-- - This is useful for:
--   * Data that changes frequently
--   * Data that needs to stay in original location
--   * Exploratory analysis before loading into BigQuery

-- Verify external table definition:
SELECT 
  table_name,
  table_type,
  ddl
FROM `your-project-id.de_zoomcamp_hw3.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'yellow_taxi_external';


-- ============================================
-- QUESTION 8: Clustering Best Practices
-- ============================================
-- It is best practice in BigQuery to always cluster your data:
-- Options: True | False

-- ANSWER: False

-- EXPLANATION:
-- Clustering is NOT always beneficial:
-- 
-- When to USE clustering:
-- - Tables larger than 1 GB
-- - Queries filter or aggregate on specific columns
-- - High cardinality columns (many distinct values)
-- - Queries that benefit from data co-location
--
-- When NOT to use clustering:
-- - Small tables (< 1 GB) - overhead outweighs benefits
-- - Queries that scan entire table
-- - Low cardinality columns (few distinct values)
-- - Tables with frequent DML operations
-- - Ad-hoc queries without consistent patterns
--
-- Best practices:
-- 1. Analyze query patterns first
-- 2. Use partitioning before clustering
-- 3. Cluster on columns used in WHERE, JOIN, GROUP BY
-- 4. Limit to 4 clustering columns (BigQuery maximum)
-- 5. Order clustering columns by cardinality (highest first)


-- ============================================
-- QUESTION 9: Understanding Table Scans
-- ============================================
-- Write a SELECT count(*) query from the materialized table.
-- How many bytes does it estimate will be read? Why?

SELECT COUNT(*) as total_records
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
-- Check estimated bytes in BigQuery UI
-- Expected: 0 MB

-- ANSWER: 0 MB (or very small amount)

-- EXPLANATION:
-- BigQuery optimizes COUNT(*) queries:
-- 1. COUNT(*) doesn't need to read actual column data
-- 2. BigQuery stores metadata about table row counts
-- 3. The query can be answered from table metadata alone
-- 4. No actual data scanning is required
-- 5. This is a significant performance optimization
--
-- However, if you use COUNT(column_name) or COUNT(*) with WHERE:
-- - BigQuery must scan the relevant columns/data
-- - Estimated bytes will be non-zero
--
-- Example that DOES require scanning:
SELECT COUNT(*) 
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
WHERE fare_amount > 0;
-- This would show estimated bytes because it needs to scan fare_amount column

-- Example with COUNT(column):
SELECT COUNT(VendorID)
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;
-- This would scan the VendorID column to count non-null values
