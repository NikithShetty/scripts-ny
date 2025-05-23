20th May 2025
=============
1. Remove I2903 fleet
2. Update amnex mapping
3. Update chalo mapping


If you have all the table data in csv then follow the below steps:
```bash
# Create table
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="CREATE TABLE atlas_kafka.fleet_device_mapping_v5 ON CLUSTER '{cluster}' AS atlas_kafka.fleet_device_mapping ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/fleet_device_mapping_v5', '{replica}') ORDER BY (fleet)"

# Insert data
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="INSERT INTO atlas_kafka.fleet_device_mapping_v5 FORMAT CSVWithNames" < output/23-05-2025/filtered-fleet-device-mapping.csv

# Rename tables
clickhouse-client -h 10.6.155.14 -u default --ask-password --query="RENAME TABLE atlas_kafka.fleet_device_mapping TO atlas_kafka.fleet_device_mapping_bak_23_05_2025, atlas_kafka.fleet_device_mapping_v5 TO atlas_kafka.fleet_device_mapping ON CLUSTER '{cluster}';"
```
RENAME TABLE fleet_device_mapping TO fleet_device_mapping_v5, fleet_device_mapping_bak_23_05_2025 TO fleet_device_mapping ON CLUSTER '{cluster}';