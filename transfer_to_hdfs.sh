#!/bin/bash

# This script transfers CSV files from the local triData directory
# to the specified HDFS directory /tmp/greyxu/tri
# Loop through CSV files from the year 2000 to 2022
for year in {2000..2022}
do

  # Define the file name
  filename="tri_data_$year.csv"

  # Check if the file exists in the local triData directory
  if [[ -f "/home/hadoop/greyxu/triData/$filename" ]]; then
    # Transfer the file to HDFS
    hdfs dfs -put "/home/hadoop/greyxu/triData/$filename" "/tmp/greyxu/tri/$filename"
  else
    echo "File $filename does not exist in triData directory."
  fi
done
