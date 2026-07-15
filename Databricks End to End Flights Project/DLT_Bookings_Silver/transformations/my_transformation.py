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



#flights
#Dimenstion --> Slow Changing Dimension Problem --> USE Auto CDC


@dlt.view(
    name = "transformed_flights"
)
def transformed_flights():
    df = df.readStream.format("delta")\
            .load("/Volumes/dataengineerflightproject/bronze/bronzevolume/flights/data/")
    return df

@dlt.table(
    name = "silver_flights"
)





















