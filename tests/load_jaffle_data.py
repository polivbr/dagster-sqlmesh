#!/usr/bin/env python3
"""
Script to load Jaffle data CSV files into DuckDB tables.
This creates the raw source tables that correspond to SQLMesh external models.
"""

import os
import duckdb
import pandas as pd
from pathlib import Path

def load_csv_to_duckdb(csv_path: str, table_name: str, db_path: str = "tests/fixtures/sqlmesh_project/jaffle_test.db"):
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
    
    # Create schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    
    # Create table from DataFrame in the main schema
    # SQLMesh expects tables in the main schema within the jaffle_test catalog
    full_table_name = f"main.{table_name}"
    con.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    con.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM df")
    
    # Get row count
    count = con.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]
    print(f"✅ Loaded {count} rows into {full_table_name}")
    
    con.close()

def main():
    """Load all Jaffle data CSV files into DuckDB tables."""
    
    # Define the mapping of CSV files to table names
    csv_to_table_mapping = {
        "tests/fixtures/raw_source_customers.csv": "raw_source_customers",
        "tests/fixtures/raw_source_products.csv": "raw_source_products", 
        "tests/fixtures/raw_source_orders.csv": "raw_source_orders",
        "tests/fixtures/raw_source_items.csv": "raw_source_items",
        "tests/fixtures/raw_source_stores.csv": "raw_source_stores",
        "tests/fixtures/raw_source_supplies.csv": "raw_source_supplies",
        "tests/fixtures/raw_source_tweets.csv": "raw_source_tweets",
    }
    
    # Generate test data if CSV files don't exist
    import subprocess
    import os
    
    csv_files_exist = all(os.path.exists(csv_path) for csv_path in csv_to_table_mapping.keys())
    if not csv_files_exist:
        print("📊 Generating test data files...")
        subprocess.run(["python", "tests/generate_test_data.py"], check=True)
    
    # Database path - use the same name as in SQLMesh config
    db_path = "tests/fixtures/sqlmesh_project/jaffle_test.db"
    
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
            full_table_name = f"main.{table_name}"
            sample = con.execute(f"SELECT * FROM {full_table_name} LIMIT 3").fetchall()
            print(f"\n📋 Sample from {full_table_name}:")
            for row in sample:
                print(f"  {row}")
        except Exception as e:
            print(f"⚠️ Could not read from {full_table_name}: {e}")
    
    con.close()
    print(f"\n✅ Data loading complete! Database: {db_path}")

if __name__ == "__main__":
    main() 