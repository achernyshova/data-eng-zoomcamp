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

-- Verify external table creation
SELECT COUNT(*) as total_records
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_external`;

-- ============================================
-- STEP 3: Create Materialized Table
-- ============================================
-- Create a regular (materialized) table from the external table
-- This table is NOT partitioned or clustered (as per homework requirements)

CREATE OR REPLACE TABLE `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`
AS
SELECT *
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_external`;

-- Verify materialized table creation
SELECT COUNT(*) as total_records
FROM `your-project-id.de_zoomcamp_hw3.yellow_taxi_materialized`;

-- ============================================
-- STEP 4: Verify Data Schema
-- ============================================
-- Check the schema of the materialized table
SELECT 
  column_name,
  data_type,
  is_nullable
FROM `your-project-id.de_zoomcamp_hw3.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'yellow_taxi_materialized'
ORDER BY ordinal_position;
