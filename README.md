# P02 - Fraud Detection

## Project Description

This project focuses on detecting potentially fraudulent financial transactions using Big Data technologies and a Lambda Architecture.

The project uses the PaySim synthetic financial dataset and combines historical batch processing with simulated real-time transaction processing.

## Objectives

- Analyze financial transactions using Hadoop HDFS.
- Implement two Java Hadoop MapReduce jobs (MR1 and MR2).
- Generate historical profiles and transaction features.
- Simulate real-time transaction processing using Apache Kafka.
- Develop a classical Machine Learning model for fraud detection.
- Integrate historical profiles and real-time predictions into a service.

## Technologies

- **Big Data:** Hadoop, HDFS, YARN, MapReduce
- **Streaming:** Apache Kafka
- **Machine Learning:** Python, Scikit-learn
- **Data Processing:** Pandas
- **Version Control:** Git and GitHub

## Dataset

The project uses the PaySim dataset:

https://www.kaggle.com/datasets/ealaxi/paysim1

The complete dataset is not included in this repository because of its size.

See [data/README.md](data/README.md) for more information.

## Project Architecture

The system follows a Lambda Architecture composed of:

- Batch Layer: Hadoop MapReduce (MR1 and MR2).
- Speed Layer: Kafka and a transaction consumer.
- Service Layer: Historical profiles, ML scores and fraud alerts.

## Project Structure

The repository is organized into the following main components:

- `docs/`: Project documentation and architecture.
- `data/`: Dataset documentation and samples.
- `hadoop/`: Hadoop MapReduce jobs and scripts.
- `ml/`: Machine Learning implementation.
- `kafka/`: Kafka producer and consumer.
- `service/`: Fraud detection service.
- `tests/`: Project tests.
- `screenshots/`: Execution evidence.

## Team

Project P02 – Big Data  
Academic year: 2026/2027  
ISIMa – Master 1 Artificial Intelligence and Data Science