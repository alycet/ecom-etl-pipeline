# Ecommerce ETL Pipeline



## Table of Contents
- [Introduction]()
- [System Architecture]()
- [About the Data]()
- [Services/Tools Used]()
- [Packages]()
- [Project Execution Flow]()


## Introduction
This project demonstrates the development of an end-to-end ETL pipeline for e-commerce data, leveraging the power of Microsoft Azure and Databricks. The pipeline is designed to process data sourced from data.world, providing a seamless workflow for extracting, transforming, and loading data into a Delta Lake for advanced analytics.

The ETL pipeline utilizes Azure Data Factory for data ingestion, transferring raw data into Azure Data Lake Storage. The data is then transformed in Databricks using Apache Spark, where the Medallion Architecture is applied to organize the data into layers: Bronze (raw data), Silver (cleaned and enriched data), and Gold (aggregated, ready-to-query data). Finally, the transformed data is stored in a Databricks Delta Lake, enabling efficient querying and analysis. This project provides a scalable and structured solution for managing and analyzing large datasets.



## System Architecture
![Architecture Diagram](https://github.com/alycet/ecom-etl-pipeline/blob/main/Ecom-Azure-ETL-Architecture.png)
## About the Data



## Sevices/Tools Used
1. **Apache Spark**:
2. **Azure DataFactory**:
3. **Azure Data Lake Storage**:
4. **Azure Databricks**:


## Packages

```
pip install pandas
```

## Project Execution Flow
1. Environment Setup:

    - Clone the repository and set up the required dependencies
    - Configure Azure credentials and connect the Data Factoru, Data Lake Storage, and Databricks for seamless data integration.

2. Data Ingestion:

    - Use Azure Data Factory to extract raw e-commerce data from data.world and ingest it into Azure Data Lake Storage.
    - Store the data in the Bronze Layer of the data lake to preserve its raw, unprocessed state.

3. Data Transformation:

    - Process the raw data in Databricks using Apache Spark, performing data cleaning and enrichment.
    - Apply the Medallion Architecture:
      - Bronze Layer: Store raw data.
      - Silver Layer: Clean and structure the data, resolving duplicates and handling missing values.
      - Gold Layer: Aggregate and prepare the data for analytics, creating business-focused tables.

5. Data Loading:

    - Store the transformed data in a Databricks Delta Lake, leveraging its ACID properties for reliable storage and fast querying.
    - Organize the Delta Lake into Gold-layer tables optimized for analytics use cases.

6. Testing and Validation:

    - Validate the data at each stage of the pipeline (Bronze, Silver, Gold) to ensure accuracy and integrity.
    - Run queries on the Delta Lake to confirm the data aligns with business requirements.

7. Usage:

    - Use Databricks SQL to explore and analyze the data in the Gold Layer.
    - Integrate with business intelligence tools to generate visualizations and insights for e-commerce performance.

8. Pipeline Automation:

     - Schedule the pipeline in Azure Data Factory or Databricks Workflows to handle periodic updates and ensure continuous data refresh.

9. Monitoring and Optimization:

    - Implement logging and monitoring in Azure and Databricks to track pipeline performance and troubleshoot issues.
    - Optimize Spark jobs and Delta Lake configurations for improved efficiency and scalability.
    - 
[Back to table of contents](https://github.com/alycet/ecom-etl-pipeline/tree/main?tab=readme-ov-file#ecommerce-etl-pipeline)



