# Airflow Football ETL Project

This project uses Apache Airflow for orchestrating an ETL (Extract, Transform, Load) process related to football data. The ETL process retrieves football-related information, uploads it to an S3 bucket, and triggers a Glue job for further processing.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Overview](#dag-overview)
- [Contributing](#contributing)
- [License](#license)

## Prerequisites

Before you begin, ensure you have the following dependencies installed:

- [Apache Airflow](https://airflow.apache.org/)
- [AWS CLI](https://aws.amazon.com/cli/)
- [Python](https://www.python.org/)

## Getting Started

To get started with this project, follow these steps:

1. Clone the repository to your local machine:

   ```bash
   git clone [(https://github.com/JawaharRamis/PL-Football-ETL-with-Airflow-AWS.git)]
   ```

2. Change to the project directory:

3. Run the Docker Compose file to set up the project environment:

   ```bash
   docker-compose up -d

## Project Workflow

The project follows a streamlined workflow to retrieve, transform, and visualize football data. The process is orchestrated through Apache Airflow, leveraging custom operators and AWS Glue services. Here's a detailed breakdown of the workflow:

### 1. Retrieve Football Data
The retrieve_group_tasks task group is responsible for fetching raw football data from an API. This information, which includes details on top goal scorers and other relevant statistics, is then pushed onto Airflow's XCom system. This ensures seamless data sharing between tasks.

### 2. Upload to S3
The custom operator, UploadXComToS3Operator, takes the data from the XCom system and efficiently uploads it to an S3 bucket. The data is stored within the 'raw' object, maintaining a clear and organized structure in the S3 storage.

### 3. Data Processing with AWS Glue
#### 3.1 Glue Crawler
A Glue Crawler is executed on the raw football data stored in the S3 bucket. This automatically discovers the schema of the raw data, creating a catalog that will be used in subsequent processing steps.

#### 3.2 Glue Job Execution
A Glue job is triggered, utilizing the schema obtained from the Glue Crawler. This job processes the raw football data, transforming it into a structured format. The transformed data is then uploaded back to the same S3 bucket but under the 'transformed' object, maintaining a clear distinction between raw and processed data.

### 4. Visualization with Quicksight
The processed data is fed into Amazon Quicksight, a robust business intelligence tool. Quicksight enables intuitive and interactive data visualization, allowing stakeholders to gain valuable insights and make informed decisions based on the football statistics.
