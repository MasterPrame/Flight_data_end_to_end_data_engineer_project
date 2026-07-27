# Flight_data_end_to_end_data_engineer_project

Databricks Flight Data Pipeline using Lakeflow Pipelines, PySpark Streaming, and Unity Catalog. Developing a dynamic Medallion Architecture with automated data quality gates and incremental loading.

## Technologies

* **Cloud Data Platform:** Databricks (PySpark, Databricks SQL)
* **Data Transformation:** dbt Cloud (SQL)
* **Version Control:** GitHub

# Process Breakdown 

Source Data ➔ Bronze (Raw) ➔ Silver (Cleansed) ➔ Gold (Star Schema) ➔ dbt Transformation ➔ Warehouse

## Source Data
The source data is the flight data has been divide into 4 parts
1. fact_bookings
2. dim_airports
3. dim_flights
4. dim_passengers

Each of the file have 3 different variant for testing the dynamic approach
- Normal
- Incremental (end with _incremental)
- Slowly Changing Dimension (end with _scd)

## Bronze Layer
Use only 2 notebook for the Incremental Data Ingestion process to bronze layer
1. BronzeLayer - Setup for load / append multiple data source in the bronze layer to the delta table.
2. SrcParamenter - Use to keep the parameter for the BronzeLayer multiple data ingestion process in the Bronze_Ingestion Job with Loop over function. 

## Silver Layer
Using Silver_DLT.py as the script for cleaning the dirty data in the delta table and support slowly changing dimension (type I) with **dlt** package and add modified date for data integrity.

This script is use in the pipeline runs feature in the Databricks to be able to watch the process with the mapping of the process.

## Gold Layer Part Dimension
Using 1 Notebook for all of the dimension delta table by using the parameter for each table 
- Call a last load date
- Detect Initial Load --> Pseudo Table
- Creating Join Condition from key_col parameter
- Seperate between Old Data and New Data by detect the surrogate_key
- Add a new surrogate key to the new data table
- Join and Upsert

## Gold Layer Part Fact
Also use 1 notebook in this time but need to simplify the surrogate key in the fact table that need to connect with the dimensions for create star schema structure.

## Apply DBT
After the gold layer data have been loaded, Everyone can use DBT for query the gold layer table to a specific query result for the Business Question and send the result to Warehouse.
To Warehouse

## Acknowledgements

 - [Reference Video](https://www.youtube.com/watch?v=vT7Oeu7WqHg)

## Possible Future Approach
- Connect to Cloud Platform (AWS, Azure)
- Learning Apache Airflow to understanding the Batch / Streaming Processing

## Note
This project is use the reference from video on youtube as the guideline of this project.

The main purpose of this project is to learning the method that Data Engineer use to achieve the purpose of prepare the data to the data team (Data Science, Data Analysis, etc.)

After this project is achieved, I have learn a lot about the approach that Data Engineering have been used in the present after the previous project that I working on the Simple Lakehouse that didn't have a challenge on the incremental data and SCD Type I that appear on this project. In addition, I have learned the new tools called **DBT** that will be the transition to create endpoint for Analysis purpose.

