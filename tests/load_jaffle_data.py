#!/usr/bin/env python3
"""
Script to load Jaffle data CSV files into DuckDB tables.
This creates the raw source tables that correspond to SQLMesh external models.
"""

import os
import duckdb
import pandas as pd
from pathlib import Path

def load_csv_to_duckdb(csv_path: str, table_name: str, db_path: str = "tests/sqlmesh_project/jaffle_test.db"):
    """
    Load a CSV file into a DuckDB table.
    
    Args:
        csv_path: Path to the CSV file
        table_name: Name of the table to create in DuckDB
        db_path: Path to the DuckDB database file
    """
    print(f"Loading {csv_path} into table {table_name}...")
    
    # Read CSV with pandas
    df = pd.read_csv(csv_path)
    
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    # Create table from DataFrame
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    
    # Get row count
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"✅ Loaded {count} rows into {table_name}")
    
    con.close()

def main():
    """Load all Jaffle data CSV files into DuckDB tables."""
    
    # Define the mapping of CSV files to table names
    csv_to_table_mapping = {
        "tests/jaffle-data/raw_source_customers.csv": "raw_source_customers",
        "tests/jaffle-data/raw_source_products.csv": "raw_source_products", 
        "tests/jaffle-data/raw_source_orders.csv": "raw_source_orders",
        "tests/jaffle-data/raw_source_items.csv": "raw_source_items",
        "tests/jaffle-data/raw_source_stores.csv": "raw_source_stores",
        "tests/jaffle-data/raw_source_supplies.csv": "raw_source_supplies",
        "tests/jaffle-data/raw_source_tweets.csv": "raw_source_tweets",
    }
    
    # Database path
    db_path = "tests/sqlmesh_project/jaffle_test.db"
    
    # Remove existing database file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"🗑️ Removed existing database: {db_path}")
    
    print("🚀 Starting data load...")
    
    # Load each CSV file
    for csv_path, table_name in csv_to_table_mapping.items():
        if os.path.exists(csv_path):
            load_csv_to_duckdb(csv_path, table_name, db_path)
        else:
            print(f"⚠️ Warning: CSV file not found: {csv_path}")
    
    # Verify tables were created
    con = duckdb.connect(db_path)
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"\n📊 Created tables: {[table[0] for table in tables]}")
    
    # Show sample data from each table
    for table_name in csv_to_table_mapping.values():
        try:
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
            print(f"\n📋 Sample from {table_name}:")
            for row in sample:
                print(f"  {row}")
        except Exception as e:
            print(f"⚠️ Could not read from {table_name}: {e}")
    
    con.close()
    print(f"\n✅ Data loading complete! Database: {db_path}")

if __name__ == "__main__":
    main() 