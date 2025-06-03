20th May 2025
=============
1. Remove I2903 fleet
2. Update amnex mapping
3. Update chalo mapping


If you have all the table data in csv then follow the below steps:
```bash
# Create table
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="CREATE TABLE atlas_kafka.fleet_device_mapping_v8_v3 ON CLUSTER '{cluster}' AS atlas_kafka.fleet_device_mapping ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/fleet_device_mapping_v8_v3', '{replica}') ORDER BY (fleet)"

# Insert data
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="INSERT INTO atlas_kafka.fleet_device_mapping_v8_v3 FORMAT CSVWithNames" < output/30-05-2025/fleet-device-mapping-updated_header_fixed.csv

# Check data
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="SELECT * FROM atlas_kafka.fleet_device_mapping_v8_v3 LIMIT 10 FORMAT Pretty"
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="SELECT * FROM atlas_kafka.fleet_device_mapping_v8_v3 LIMIT 10 OFFSET 3000 FORMAT Pretty"

# Rename tables
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="RENAME TABLE atlas_kafka.fleet_device_mapping TO atlas_kafka.fleet_device_mapping_bak_30_05_2025_v3, atlas_kafka.fleet_device_mapping_v8_v3 TO atlas_kafka.fleet_device_mapping ON CLUSTER '{cluster}';"


# !!!!!!!! Revert
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="RENAME TABLE atlas_kafka.fleet_device_mapping TO atlas_kafka.fleet_device_mapping_v8_v3, atlas_kafka.fleet_device_mapping_bak_30_05_2025_v3 TO atlas_kafka.fleet_device_mapping ON CLUSTER '{cluster}';"
```