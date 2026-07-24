
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH parent_query AS
(SELECT F.amount, D.country
FROM 
    dataengineerflightproject.gold.fact_bookings AS F
LEFT JOIN
    dataengineerflightproject.gold.dim_airports AS D
ON 
    F.dim_airports_key = D.dim_airports_key)

SELECT country, sum(amount) AS total_amount 
FROM parent_query
GROUP BY country


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
