# Real-time data Streaming! User Profile Automation

Introduction
- This project is developed for real-time stream processing of events from API for creating user dimension table in the database.
- It is an end-to-end data engineering project involving data ingestion, processing and storage in real-time for down stream analysis.
- It utilizes a robust tech stack including Apache Airflow, Apache Kafka, Apache Spark and Cassandra. 
- Finally all components are containerized using Docker for ease of deployment and scalability.

## System Architecture

![system_architecture_diagram](https://github.com/meetapandit/kafka_streaming_user_creation/assets/15186489/0060c7d6-7ceb-4e27-9407-ad2fee5d94ea)

## Step-by-Step Workflow
- Data Source: Random users are generated through randomuser.me API
- Extraction: Data is extracted in real-time and scheduled in Apache Airflow
- Staging layer: Events are streamed continuously in intervals of 1 min into Kafka cluster (topics) running on Zookeeper
- Data processing: Events from kafka topics are streamed to Spark cluster for data processing
- Persistent Storage: Chose columnar storage that is Cassandra for storage as user data is write-once read-many

