# Flight_data_end_to_end_data_engineer_project **Draft 
Databricks Flight Data Pipeline using Lakeflow Pipelines, PySpark Streaming, and Unity Catalog. Developing a dynamic Medallion Architecture with automated data quality gates and incremental loading.

## Technologies

* **Cloud Data Platform:** Databricks (PySpark, Databricks SQL)
* **Data Transformation & Modeling:** dbt Cloud (SQL, Jinja)
* **Version Control:** GitHub

* **Storage / Lakehouse:** Delta Lake (Bronze, Silver, Gold Layers)
* **Data Architecture:** Medallion Architecture, Kimball Star Schema

# Process Breakdown 

[Source Data] ➔ [Bronze (Raw)] ➔ [Silver (Cleansed)] ➔ [Gold (Star Schema)] ➔ [dbt Transformation]

## Source Data

## Bronze Layer

## Silver Layer

## Gold Layer Part Dimension

## Gold Layer Part Fact

## Apply DBT

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

