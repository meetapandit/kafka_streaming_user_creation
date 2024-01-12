# Real-time data Streaming! User Profile Automation

Introduction
- This project is developed for real-time stream processing of events from API for creating user dimension table in the database.
- It is an end-to-end data engineering project involving data ingestion, processing and storage in real-time for down stream analysis.
- It utilizes a robust tech stack including Apache Airflow, Apache Kafka, Apache Spark and Cassandra. 
- Finally, all components are containerized using Docker for ease of deployment and scalability.

## System Architecture

![system_architecture_diagram](https://github.com/meetapandit/kafka_streaming_user_creation/assets/15186489/0060c7d6-7ceb-4e27-9407-ad2fee5d94ea)

## Step-by-Step Workflow
- Data Source: Random users are generated through randomuser.me API
- Extraction: Data is extracted in real-time and scheduled in Apache Airflow
- Staging layer: Events are streamed continuously in intervals of 1 min into Kafka cluster (topics) running on Zookeeper
- Data processing: Events from Kafka topics are streamed to Spark cluster for data processing
- Persistent Storage: Chose columnar storage that is Cassandra for storage as user data is write-once read-many

## Snapshots showing user data updated to Cassandra table after processing in Spark:

- Logged into Kafka cluster and selected topic users_created to display messages streaming to Kafka topic
 <img width="1353" alt="Screenshot 2024-01-10 at 11 13 27 PM" src="https://github.com/meetapandit/kafka_streaming_user_creation/assets/15186489/d29f3084-57a4-4ad1-8829-4976095a811c">


- Screenshot showing the count of messages stored in Kafka topic
 <img width="1353" alt="Screenshot 2024-01-10 at 11 14 04 PM" src="https://github.com/meetapandit/kafka_streaming_user_creation/assets/15186489/9ae20fc1-df26-49a7-b63b-aec9dea81e53">


- Logged into Cassandra keyspace spark_streams and selecting records from created_users table
 <img width="1353" alt="Screenshot 2024-01-10 at 11 04 05 PM" src="https://github.com/meetapandit/kafka_streaming_user_creation/assets/15186489/b4515579-8361-4412-bcf9-79344befc3a0">


- The below snapshot shows that all 530 records are stored in the table
 <img width="1353" alt="Screenshot 2024-01-10 at 11 05 48 PM" src="https://github.com/meetapandit/kafka_streaming_user_creation/assets/15186489/09712c3d-2f99-4291-9c11-2d1737e8c2e4">

