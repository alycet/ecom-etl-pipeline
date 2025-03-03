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
2. Prepare Data:
   	- execute script to chuck user data into 10 csv files.
	- user data is large and may change so we split into 10 chunks that will be uploaded to landing zone manually to simulate real word data ingestion. 
3. Create Landing Zones in Data Lake Storage:
    - Create landing-zone-1 container where raw csv files will be manually uploaded.(represents rdms of other external data storage in real world)
	- Create landing-zone-2 that will act as the sink raw storage location for the azure data pipelines.
	- Within landing-zone-1 and landing-zone-2 create separate folders for each data set
5. Build Azure Pipeline for Data Ingestion:

	- Create one pipeline for users folder (batch pipeline triggers as user data is added)
	- Create one for the rest of the dataset folders (ran as required since this data does not continuously updated)
	- The pipelines copy the data from the first landing zone (source) to the second landing zone and converts data to parquet file format

6. Data Transformation using Azure Databricks:

    - Process the raw data in Databricks using Apache Spark, performing data cleaning and enrichment.
    - Apply the Medallion Architecture:
      - Bronze Layer: Store raw data.
      - Silver Layer: Clean and structure the data, resolving duplicates, handling missing values, and deriving additional columns for data enrichment
      - Gold Layer: Join and prepare the data for analytics, creating one big table..

7. Data Loading and Automation:

    - Store the transformed data in a Databricks Delta Lake, leveraging its ACID properties for reliable storage and fast querying.
    - Organize the Delta Lake into Gold-layer tables optimized for analytics use cases.

8. Testing and Validation:

    - Validate the data at each stage of the pipeline (Bronze, Silver, Gold) to ensure accuracy and integrity.
    - Run queries on the Delta Lake to confirm the data aligns with business requirements.

9. Usage:

    - Use Databricks SQL to explore and analyze the data in the Gold Layer.
    - Integrate with business intelligence tools to generate visualizations and insights for e-commerce performance.


[Back to table of contents](https://github.com/alycet/ecom-etl-pipeline/tree/main?tab=readme-ov-file#ecommerce-etl-pipeline)



