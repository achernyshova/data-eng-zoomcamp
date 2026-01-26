#!/usr/bin/env python
# coding: utf-8

"""
Run SQL queries from queries.sql file against PostgreSQL database.
"""

import click
from sqlalchemy import create_engine, text


def run_query(engine, query_name: str, query: str):
    """Execute a single query and display results"""
    print(f"\n{'='*80}")
    print(f"{query_name}")
    print('='*80)
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        
        # Get column names
        columns = result.keys()
        
        # Print header
        header = " | ".join(str(col) for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in result:
            row_str = " | ".join(str(val) for val in row)
            print(row_str)


def parse_sql_file(file_path: str):
    """Parse SQL file and extract individual queries with their names"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    queries = []
    current_query = []
    current_name = None
    
    for line in content.split('\n'):
        # Check if it's a question comment
        if line.strip().startswith('-- Question'):
            if current_query and current_name:
                # Save previous query
                queries.append((current_name, '\n'.join(current_query)))
                current_query = []
            current_name = line.strip('-- ').strip()
        elif line.strip() and not line.strip().startswith('--') and not line.strip().startswith('='):
            # Add SQL line to current query
            current_query.append(line)
    
    # Add last query
    if current_query and current_name:
        queries.append((current_name, '\n'.join(current_query)))
    
    return queries


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--sql-file', default='queries.sql', help='SQL file with queries')
@click.option('--query-num', type=int, help='Run specific query number (1-4)')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, sql_file, query_num):
    """
    Run SQL queries from file against PostgreSQL database.
    
    Example usage:
        # Run all queries
        python run_queries.py
        
        # Run specific query
        python run_queries.py --query-num 3
        
        # Custom connection
        python run_queries.py --pg-user postgres --pg-pass mypass --pg-db ny_taxi
    """
    # Create database engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Parse SQL file
    queries = parse_sql_file(sql_file)
    
    if not queries:
        print(f"No queries found in {sql_file}")
        return
    
    print(f"Found {len(queries)} queries in {sql_file}")
    
    # Run queries
    if query_num:
        # Run specific query
        if 1 <= query_num <= len(queries):
            name, query = queries[query_num - 1]
            run_query(engine, name, query)
        else:
            print(f"Error: Query number must be between 1 and {len(queries)}")
    else:
        # Run all queries
        for name, query in queries:
            run_query(engine, name, query)
    
    print("\n" + "="*80)
    print("QUERIES COMPLETE")
    print("="*80)


if __name__ == '__main__':
    main()
