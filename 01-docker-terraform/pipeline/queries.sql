-- NYC Green Taxi Data Analysis Queries
-- November 2025 Dataset

-- ============================================================================
-- Question 3: Counting short trips
-- How many trips had a trip_distance <= 1 mile in November 2025?
-- ============================================================================

SELECT COUNT(*) as short_trips_count
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1.0;


-- ============================================================================
-- Question 4: Longest trip for each day
-- Which day had the longest trip distance? (excluding trips >= 100 miles)
-- ============================================================================

SELECT 
    DATE(lpep_pickup_datetime) as pickup_date,
    MAX(trip_distance) as max_distance
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 10;


-- ============================================================================
-- Question 5: Biggest pickup zone on November 18th, 2025
-- Which pickup zone had the largest total_amount?
-- ============================================================================

SELECT 
    z."Zone",
    z."Borough",
    SUM(g.total_amount) as total_amount,
    COUNT(*) as trip_count
FROM green_taxi_data g
JOIN taxi_zones z ON g."PULocationID" = z."LocationID"
WHERE DATE(g.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone", z."Borough"
ORDER BY total_amount DESC
LIMIT 10;


-- ============================================================================
-- Question 6: Largest tip from East Harlem North
-- Which dropoff zone had the largest tip for pickups in East Harlem North?
-- ============================================================================

SELECT 
    z_dropoff."Zone" as dropoff_zone,
    z_dropoff."Borough" as dropoff_borough,
    MAX(g.tip_amount) as max_tip,
    COUNT(*) as trip_count
FROM green_taxi_data g
JOIN taxi_zones z_pickup ON g."PULocationID" = z_pickup."LocationID"
JOIN taxi_zones z_dropoff ON g."DOLocationID" = z_dropoff."LocationID"
WHERE z_pickup."Zone" = 'East Harlem North'
  AND g.lpep_pickup_datetime >= '2025-11-01'
  AND g.lpep_pickup_datetime < '2025-12-01'
GROUP BY z_dropoff."Zone", z_dropoff."Borough"
ORDER BY max_tip DESC
LIMIT 10;
