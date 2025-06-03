#!/usr/bin/env python3

import subprocess
import sys
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fleet_update.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Configuration
CLICKHOUSE_HOST = "10.6.155.14"
CLICKHOUSE_USER = "default"
CSV_FILE = "output/26-05-2025/fleet-device-mapping-updated_header_fixed_v2.csv"


def run_clickhouse_query(query):
    """Execute a ClickHouse query and return the result."""
    try:
        cmd = [
            "clickhouse-client",
            f"-h{CLICKHOUSE_HOST}",
            f"-u{CLICKHOUSE_USER}",
            "--ask-password",
            f"--query={query}"
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing query: {e}")
        logging.error(f"Error output: {e.stderr}")
        raise


def main():
    try:
        # Step 1: Create new table
        logging.info("Creating new table fleet_device_mapping_v7...")
        create_table_query = """
        CREATE TABLE atlas_kafka.fleet_device_mapping_v7 ON CLUSTER '{cluster}' 
        AS atlas_kafka.fleet_device_mapping 
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/fleet_device_mapping_v7', '{replica}') 
        ORDER BY (fleet)
        """
        run_clickhouse_query(create_table_query)
        logging.info("Table created successfully")

        # Step 2: Insert data from CSV
        if not os.path.exists(CSV_FILE):
            raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

        logging.info(f"Inserting data from {CSV_FILE}...")
        insert_cmd = [
            "clickhouse-client",
            f"-h{CLICKHOUSE_HOST}",
            f"-u{CLICKHOUSE_USER}",
            "--ask-password",
            "--query=INSERT INTO atlas_kafka.fleet_device_mapping_v7 FORMAT CSVWithNames"
        ]

        with open(CSV_FILE, 'r') as f:
            subprocess.run(insert_cmd, stdin=f, check=True)
        logging.info("Data inserted successfully")

        # Step 3: Verify data
        logging.info("Verifying inserted data...")
        verify_query = "SELECT * FROM atlas_kafka.fleet_device_mapping_v7 LIMIT 10 FORMAT Pretty"
        result = run_clickhouse_query(verify_query)
        logging.info("Sample data:\n%s", result)

        # Step 4: Rename tables
        logging.info("Renaming tables...")
        rename_query = """
        RENAME TABLE 
        atlas_kafka.fleet_device_mapping TO atlas_kafka.fleet_device_mapping_bak_26_05_2025_v2,
        atlas_kafka.fleet_device_mapping_v7 TO atlas_kafka.fleet_device_mapping 
        ON CLUSTER '{cluster}'
        """
        run_clickhouse_query(rename_query)
        logging.info("Tables renamed successfully")

        logging.info("Fleet mapping update completed successfully!")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
