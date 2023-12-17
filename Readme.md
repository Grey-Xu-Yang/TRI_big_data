# TRI Data Processing and Management System

This repository contains a set of scripts and HQL files used to manage and process data from the Toxics Release Inventory (TRI) website, transferring it to a Hadoop-based system, and performing various data organization and retrieval tasks.

## Files Description

- **download_data.sh**
  - **Purpose**: Downloads data from the Toxics Release Inventory website.
  - **Usage**: Execute this shell script to retrieve the latest TRI data.
  
- **transfer_to_hdfs.sh**
  - **Purpose**: Transfers the downloaded TRI data to the Hadoop Distributed File System (HDFS).
  - **Usage**: Run this script after downloading the TRI data to move it into HDFS.

- **hive_tri_data.hql**
  - **Purpose**: Stores and serializes the TRI data into Hive.
  - **Usage**: Use this Hive query language script to create the necessary tables and serialize CSV files into a format that Hive can store and query.

- **hbase_tri_data.hql**
  - **Purpose**: Connects Hive to HBase, inserts the data, and groups the data by year and sector code.
  - **Usage**: Execute this script to perform data insertion into HBase via Hive and to organize the data accordingly.

- **kafka_topic.hql**
  - **Purpose**: Creates a Kafka topic to store the speed layer submit data.
  - **Usage**: This script should be run to set up a Kafka topic for real-time data streaming purposes.

- **tri_speed_layer**
  - **Purpose**: A folder containing scripts and resources to handle the storage of data from Kafka to HBase.
  - **Usage**: Refer to individual files within this folder for specific usage instructions.

- **tri_with_sector_form**
  - **Purpose**: Contains resources to build the website interface that displays the batch layer view, submission of speed data, and the speed layer view.
  - **Usage**: Access this folder to update or modify the web interface components.
  
  - **website_showcase.mp4**
  - **Purpose**: Contains the video go through the web application.
  - **Usage**: Click on this file and open to with a video-player software to view the video record.

### Web Interface URLs

- **Batch View**: [http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3056/](http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3056/)
  - Displays yearly statistics of toxic release information from the historical data.

- **Submit Data (Speed Layer)**: [http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3056/submit-tri.html](http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3056/submit-tri.html)
  - Interface for submitting real-time toxic release information.

- **Speed Layer View**: [http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3056/latest-tri.html](http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3056/latest-tri.html)
  - Shows the most recent toxic release data as it is processed.

### Running the App

To start the application and begin interacting with the web interfaces, follow these steps:

1. Start the Node.js application server:

```bash
node app.js 3056 ec2-3-131-137-149.us-east-2.compute.amazonaws.com 8070 b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
```

2. Submit the Spark job for processing real-time data:
```bash
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamToxics uber-tri_speed_layer-1.0-SNAPSHOT.jar b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
```

Note: 

1. I have ensured the `tri_with_sector_form` code is deployed on the cluster as it is required for the web application to function.

2. I also deployed the uber JAR file required by Spark. This step is completed prior to submitting the Spark job.
