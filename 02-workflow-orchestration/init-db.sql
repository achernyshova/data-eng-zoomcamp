-- Initialize databases for the homework
-- This script runs automatically when the PostgreSQL container starts

-- Create ny_taxi database for taxi data
CREATE DATABASE ny_taxi;

-- Grant privileges to kestra user
GRANT ALL PRIVILEGES ON DATABASE ny_taxi TO kestra;

-- Connect to ny_taxi database and create schema
\c ny_taxi;

-- Create public schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS public;

-- Grant privileges on schema
GRANT ALL PRIVILEGES ON SCHEMA public TO kestra;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kestra;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kestra;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO kestra;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO kestra;
