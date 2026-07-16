import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Booking Table
@dlt.table(
    name = "stage_bookings" #give a table name
)
def stage_bookings():
    df = spark.readStream.format("delta")\
    .load("/Volumes/dataengineerflightproject/bronze/bronzevolume/bookings/data/")
    ##Streaming Table
    return df
#Decorator dlt.table - tell to databricks that this trying to create table

# 1.ระบบจะเข้ามาสร้างตารางชื่อ stage_bookings สแตนด์บายรอไว้
# 2.มันจะวิ่งไปดูดข้อมูลที่อยู่ในโฟลเดอร์ bookings/data ของเลเยอร์ Bronze เข้ามาเก็บไว้ในตารางนี้
# 3.หากในอนาคตมีข้อมูลใหม่ถูกส่งเข้ามาต่อท้ายโฟลเดอร์ Bronze อีก โค้ดชุดนี้จะฉลาดพอที่จะดึงเฉพาะข้อมูลแถวที่งอกใหม่เหล่านั้นเข้ามาเติมในตาราง stage_bookings ให้เองโดยอัตโนมัติ โดยไม่เกิดปัญหาข้อมูลซ้ำ

@dlt.view(
    name = "transformed_bookings"
)
def transformed_bookings():
    df = dlt.read_stream("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modified_date", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd"))\
        .drop("_rescued_data")

    ### 1.Make "amount" change from string to double (float)
    ### 2. Add "modified_date" column with current timestamp
    ### 3. Drop "_rescued_data"
    ### 4. Change "booking_date" from string to date
    return df


rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
#@expect_all(rules) = ต้องตรงตามกฏ
def silver_bookings():
    df = dlt.read_stream("transformed_bookings")
    return df


#############################################################################################
#flights
#Dimenstion --> Slow Changing Dimension Problem --> USE Auto CDC


@dlt.view(
    name = "transformed_flights"
)
def transformed_flights():
    df = spark.readStream.format("delta")\
            .load("/Volumes/dataengineerflightproject/bronze/bronzevolume/flights/data/")

    df = df.withColumn("flight_date", to_date(col("flight_date"), "yyyy-MM-dd"))\
    .drop("_rescued_data")\
    .withColumn("modified_date", current_timestamp())

    return df

#Create Target Table - to put in the data upserted
dlt.create_streaming_table("silver_flights")

#Function สำหรับ Upsert ได้อัตโนมัติ
# Parameters -----
# target = table that is the one get saved
# source = table that want to upsert
# keys = primary key of the table that connected
# sequence_by = to not make the old data replace new data
# apply_as_deletes - find this in column = delete
# apply_as_truncates - find this in column = truncates
# except_column_list - not put this column in the final table
# stored_as_scd_type = Type of Slowly changing dimension

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "transformed_flights",
    keys = ["flight_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


#############################################################################################
#passenger data

@dlt.view(
    name = "transformed_passengers"
)
def transformed_passengers():
    df = spark.readStream.format("delta")\
            .load("/Volumes/dataengineerflightproject/bronze/bronzevolume/customers/data/")

    df = df.drop("_rescued_data")\
    .withColumn("modified_date", current_timestamp())

    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "transformed_passengers",
    keys = ["passenger_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


#############################################################################################
#airport data

@dlt.view(
    name = "transformed_airports"
)
def transformed_airports():
    df = spark.readStream.format("delta")\
    .load("/Volumes/dataengineerflightproject/bronze/bronzevolume/airports/data/")

    df = df.drop("_rescued_data")\
    .withColumn("modified_date", current_timestamp())

    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "transformed_airports",
    keys = ["airport_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


##############################################
#Silver Business View
# Combine all data table (relational database)

@dlt.table(
    name = "silver_business_view"
)
def silver_business_view():
    df = dlt.readStream("silver_bookings")\
        .join(dlt.readStream("silver_flights").drop("modified_date"), ["flight_id"])\
        .join(dlt.readStream("silver_passengers").drop("modified_date"), ["passenger_id"])\
        .join(dlt.readStream("silver_airports").drop("modified_date"), ["airport_id"])

    return df

