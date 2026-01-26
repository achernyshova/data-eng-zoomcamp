#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def ingest_data(
    url: str,
    engine,
    target_table: str,
    file_format: str = None,
    chunksize: int = 100000,
) -> None:
    """
    Ingest data from URL or local file into PostgreSQL.
    Supports both parquet and CSV formats.
    
    Args:
        url: URL or local file path
        engine: SQLAlchemy engine
        target_table: Target table name in PostgreSQL
        file_format: File format ('parquet' or 'csv'). If None, auto-detect from URL/path
        chunksize: Number of rows per chunk for ingestion
    """
    
    # Auto-detect format if not specified
    if file_format is None:
        if url.endswith('.parquet'):
            file_format = 'parquet'
        elif url.endswith('.csv') or url.endswith('.csv.gz'):
            file_format = 'csv'
        else:
            raise ValueError(f"Cannot auto-detect format for: {url}. Please specify --format")
    
    print(f"\nIngesting {file_format.upper()} from: {url}")
    print(f"Target table: {target_table}")
    
    if file_format == 'parquet':
        # Read parquet file (works with both URL and local path)
        df = pd.read_parquet(url)
        
        print(f"Total records: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Create table
        df.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace",
            index=False
        )
        
        print(f"Table {target_table} created")
        
        # Insert data in chunks
        total_rows = len(df)
        for i in tqdm(range(0, total_rows, chunksize), desc="Inserting chunks"):
            chunk = df.iloc[i:i+chunksize]
            chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append",
                index=False
            )
        
        print(f'Done ingesting {total_rows} rows to {target_table}')
    
    elif file_format == 'csv':
        # Check if file is gzipped
        compression = 'gzip' if url.endswith('.gz') else None
        
        # First, read a small sample to check columns
        sample_df = pd.read_csv(url, nrows=5, compression=compression)
        columns = sample_df.columns.tolist()
        
        # Check if this is taxi data (has pickup/dropoff datetime columns)
        has_taxi_columns = any(col in columns for col in parse_dates)
        
        # Prepare kwargs for reading CSV
        read_kwargs = {
            'iterator': True,
            'chunksize': chunksize,
            'compression': compression
        }
        
        # Apply dtype and parse_dates only if taxi columns exist
        if has_taxi_columns:
            # Filter dtype to only include columns that exist
            filtered_dtype = {k: v for k, v in dtype.items() if k in columns}
            # Filter parse_dates to only include columns that exist
            filtered_parse_dates = [col for col in parse_dates if col in columns]
            
            if filtered_dtype:
                read_kwargs['dtype'] = filtered_dtype
            if filtered_parse_dates:
                read_kwargs['parse_dates'] = filtered_parse_dates
        
        # Read CSV file in chunks (works with both URL and local path)
        df_iter = pd.read_csv(url, **read_kwargs)
        
        # Get first chunk
        first_chunk = next(df_iter)
        
        print(f"Columns: {first_chunk.columns.tolist()}")
        
        # Create table
        first_chunk.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace",
            index=False
        )
        
        print(f"Table {target_table} created")
        
        # Insert first chunk
        first_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )
        
        print(f"Inserted first chunk: {len(first_chunk)}")
        
        # Insert remaining chunks
        total_rows = len(first_chunk)
        for df_chunk in tqdm(df_iter, desc="Inserting chunks"):
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append",
                index=False
            )
            total_rows += len(df_chunk)
            print(f"Inserted chunk: {len(df_chunk)}")
        
        print(f'Done ingesting {total_rows} rows to {target_table}')
    
    else:
        raise ValueError(f"Unsupported format: {file_format}. Use 'parquet' or 'csv'")

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--url', 'data_url', required=True, help='URL or local file path')
@click.option('--table', 'table_name', required=True, help='Target table name')
@click.option('--format', 'file_format', type=click.Choice(['parquet', 'csv'], case_sensitive=False), help='File format (auto-detect if not specified)')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, data_url, table_name, file_format, chunksize):

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Ingest data
    ingest_data(
        url=data_url,
        engine=engine,
        target_table=table_name,
        file_format=file_format,
        chunksize=chunksize
    )

    print("\n" + "="*80)
    print("INGESTION COMPLETE")
    print("="*80)
    print(f"\nTable created: {table_name}")
    

if __name__ == '__main__':
    main()