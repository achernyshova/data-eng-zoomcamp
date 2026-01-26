Docker workshop link: [https://github.com/alexeygrigorev/workshops/tree/main/dezoomcamp-docker](https://github.com/alexeygrigorev/workshops/tree/main/dezoomcamp-docker) 

## Homework


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


Run All Queries

```bash
uv run python run_queries.py
```