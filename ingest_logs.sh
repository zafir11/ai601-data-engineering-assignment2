#!/bin/bash

# Check if the date parameter is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Define local directories
LOCAL_LOG_FILE="raw_data/user_activity_logs.csv"
LOCAL_METADATA_FILE="raw_data/content_metadata.csv"

# Define HDFS directories
HDFS_LOG_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

# Create HDFS directories
hdfs dfs -mkdir -p $HDFS_LOG_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Copy logs into HDFS
hdfs dfs -put -f $LOCAL_LOG_FILE $HDFS_LOG_DIR/
hdfs dfs -put -f $LOCAL_METADATA_FILE $HDFS_METADATA_DIR/

echo "Data for $DATE successfully ingested into HDFS!"
