# Homework

## Ingest Data

Run the script to ingest datasets

```bash
# Ingest taxi data
uv run python ingest_data.py \
  --url https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet \
  --table green_taxi_data

# Ingest zones data
uv run python ingest_data.py \
  --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv \
  --table taxi_zones
```


### Ingestion Options

| Option | Default | Description |
|--------|---------|-------------|
| `--pg-user` | `root` | PostgreSQL username |
| `--pg-pass` | `root` | PostgreSQL password |
| `--pg-host` | `localhost` | PostgreSQL host |
| `--pg-port` | `5432` | PostgreSQL port |
| `--pg-db` | `ny_taxi` | PostgreSQL database name |
| `--url` | *Required* | URL or local file path |
| `--table` | *Required* | Target table name |
| `--format` | Auto-detect | File format: `parquet` or `csv` |
| `--chunksize` | `100000` | Chunk size for ingestion |

## Run Queries

### Run All Queries

```bash
uv run  python run_queries.py
```

### Run Specific Query

```bash
# Run only Question 3
uv run python run_queries.py --query-num 3
```

### Custom Database Connection

```bash
uv run python run_queries.py \
  --pg-user postgres \
  --pg-pass mypassword \
  --pg-db ny_taxi
```


### Query Runner Options

| Option | Default | Description |
|--------|---------|-------------|
| `--pg-user` | `root` | PostgreSQL username |
| `--pg-pass` | `root` | PostgreSQL password |
| `--pg-host` | `localhost` | PostgreSQL host |
| `--pg-port` | `5432` | PostgreSQL port |
| `--pg-db` | `ny_taxi` | PostgreSQL database name |
| `--sql-file` | `queries.sql` | SQL file with queries |
| `--query-num` | `None` | Run specific query (1-4) |

## SQL Queries

The `queries.sql` file contains four analysis queries:

### Question 3: Counting Short Trips
Counts trips in November 2025 with distance ≤ 1 mile.

**Expected Answer:** 8,007 trips

### Question 4: Longest Trip for Each Day
Finds the day with the longest trip distance (excluding trips ≥ 100 miles).

**Expected Answer:** 2025-11-14 (88.03 miles)

### Question 5: Biggest Pickup Zone on November 18th
Identifies the pickup zone with the largest total amount on November 18th, 2025.

**Expected Answer:** East Harlem North ($9,281.92)

### Question 6: Largest Tip from East Harlem North
Finds the dropoff zone that received the largest tip from passengers picked up in East Harlem North.

**Expected Answer:** Yorkville West ($81.89)