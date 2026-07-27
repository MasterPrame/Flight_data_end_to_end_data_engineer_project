# Flight_data_end_to_end_data_engineer_project

Databricks Flight Data Pipeline using Lakeflow Pipelines, PySpark Streaming, and Unity Catalog. Developing a dynamic Medallion Architecture with automated data quality gates and incremental loading.

## Technologies

* **Cloud Data Platform:** Databricks (PySpark, Databricks SQL)
* **Data Transformation & Modeling:** dbt Cloud (SQL)
* **Version Control:** GitHub

# Process Breakdown 

Source Data ➔ Bronze (Raw) ➔ Silver (Cleansed) ➔ Gold (Star Schema) ➔ dbt Transformation ➔ Warehouse

## Source Data
The source data is the flight data has been divide into 4 portions
1. fact_bookings
2. dim_airports
3. dim_flights
4. dim_passengers

Each of the file have 3 different variant
- Normal
- Incremental
- Slowly Changing Dimension

## Bronze Layer
- Create the notebook
- Using Autoloader method to pull the data from raw 

## Silver Layer
- Clean the data, upsert
- Prepare for the first time data coming
- Using Python Pipeline

## Gold Layer Part Dimension
- Make the create the surrogate key specifically
- Update Date
- Lastload Date

## Gold Layer Part Fact
- Connect every dim and upsert, Star Schema
- Lastload Date

## Apply DBT
After the gold layer data have been loaded, Everyone can use DBT for query the gold layer table to a specific query result for the Business Question and send the result to Warehouse.
To Warehouse

##

## Acknowledgements

 - [Reference Video](https://www.youtube.com/watch?v=vT7Oeu7WqHg)

## Possible Future Approach
- Connect to Cloud Platform (AWS, Azure)
- Learning Apache Airflow to understanding the Batch / Streaming Processing

## Note
This project is use the reference from video on youtube as the guideline of this project.

The main purpose of this project is to learning the method that Data Engineer use to achieve the purpose of prepare the data to the data team (Data Science, Data Analysis, etc.)

After this project is achieved, I have learn a lot about the approach that Data Engineering have been used in the present after the previous project that I working on the Simple Lakehouse that didn't have a challenge on the incremental data and SCD Type I that appear on this project. In addition, I have learned the new tools called **DBT** that will be the transition to create endpoint for Analysis purpose.

