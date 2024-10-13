# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data
# MAGIC Pull data from all csv files and hold them in a dataframe

# COMMAND ----------

holding_df = spark.read.load('/Volumes/landing/landed_files/vol_colibri/data_group_*.csv',format='csv',header=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC add original file name as column

# COMMAND ----------

from pyspark.sql.functions import col, row_number, rank, avg, sum, min, max, round, stddev,when, count, lit,current_timestamp
from pyspark.sql import Window
landing_df = holding_df.withColumn("file_name", col("_metadata.file_path"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks
# MAGIC In this pipeline, data quality issues trigger a failure of the pipeline. We don't impute or infer data; we try to keep downstream datasets clean. Users are alerted to issues in the data via an external mechanism; typically I use a function app in Azure to handle this

# COMMAND ----------

# MAGIC %md
# MAGIC define a function that calls an external API for alerting stakeholders to pipeline issues. this function would ideally be stored as a UDF somewhere in databricks

# COMMAND ----------

import requests
#define reporting function
def call_orchestration_alert(alert_message):
    print(f"I am the fucntion call: {alert_message}")
    api_url = "http://my-supernifty-alerting-mechanism.com/api/v1/alert"
    payload = {"message": alert_message}
    # response = requests.post(api_url, json=payload) #commenting this line out as the URL doesn't exist

# COMMAND ----------

# MAGIC %md
# MAGIC ## check each file has at least one record per turbine per file

# COMMAND ----------

#this mapping is hard-coded, in a real-world scenario, this would be read from a config table based on current system knowlewdge
file_turbine_mapping = {
    'dbfs:/Volumes/landing/landed_files/vol_colibri/data_group_3.csv': range(11, 16),
    'dbfs:/Volumes/landing/landed_files/vol_colibri/data_group_2.csv': range(6, 11),
    'dbfs:/Volumes/landing/landed_files/vol_colibri/data_group_1.csv': range(1, 6)
}

# Iterate through the mapping to check the existance of at least one record per turbine per file
for file_name, turbine_ids in file_turbine_mapping.items():
    for turbine_id in turbine_ids:
        record_exists = landing_df \
            .filter((col("file_name") == file_name) & (col("turbine_id") == turbine_id)) \
            .count() > 0
        assert record_exists, f"No records found for turbine_id {turbine_id} in file {file_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check that there is a record for every tubrine for every hour in every day

# COMMAND ----------

from pyspark.sql.functions import expr, col, date_format, hour

hours_df = spark.range(24).selectExpr("id as hour")

# Extract the date from the timestamp column and get distinct turbine and date combinations
turbine_dates_df = landing_df.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .select("turbine_id", "date").distinct()

# Cross join with hours_df to ensure every turbine has a recreord for each hour of each day
turbine_hours_df = turbine_dates_df.crossJoin(hours_df)

# Extract hour and date from the timestmap column 
landing_with_hour_df = landing_df.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
                                 .withColumn("hour", hour(col("timestamp")))

# Join to find missing hourly records per turbine per day
missing_hourly_records_df = turbine_hours_df.join(landing_with_hour_df,
    (turbine_hours_df.turbine_id == landing_with_hour_df.turbine_id) &
    (turbine_hours_df.date == landing_with_hour_df.date) &
    (turbine_hours_df.hour == landing_with_hour_df.hour),
    "left_anti"
)

# run assert to fail if missing hours
is_missing_hours = missing_hourly_records_df.count() == 0
assert is_missing_hours, call_orchestration_alert("Missing hourly records")



# COMMAND ----------

# MAGIC %md
# MAGIC ## check quality of metrics in files, fail if NULL

# COMMAND ----------

#generate list of assertions based on documented validation rules
no_missing_wind_speed = landing_df.filter(col("wind_speed").isNull()).count() == 0
no_missing_wind_direction = landing_df.filter(col("wind_direction").isNull()).count() == 0
no_missing_power_output = landing_df.filter(col("power_output").isNull()).count() == 0

assert no_missing_power_output, call_orchestration_alert("Missing power_output in file")
assert no_missing_wind_direction, call_orchestration_alert("Missing wind_direction in file")
assert no_missing_wind_speed, call_orchestration_alert("Missing wind_speed in file")


# COMMAND ----------

# #JUST FOR TESTING

# # Update landing_df setting power_output as NULL for a few records
# landing_df = landing_df.withColumn("power_output", when(col("turbine_id").isin([1, 2, 3]), lit(None)).otherwise(col("power_output")))

# COMMAND ----------

# MAGIC %md
# MAGIC #   Statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric aggregations

# COMMAND ----------


aggregate_df = landing_with_hour_df.groupBy("turbine_id", "date").agg(
    min("power_output").alias("min_power_output"),
    max("power_output").alias("max_power_output"),
    avg("power_output").alias("avg_power_output"),
    stddev("power_output").alias("stddev_power_output")
)

aggregate_df = aggregate_df.withColumn("anomaly_upper_threshold", round((col("stddev_power_output") * 2) + col("avg_power_output"),2))

display(aggregate_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## anomolies (anomalies? Spelling)

# COMMAND ----------

#get upper threshold for each date/turbine

anomaly_threshold_df = landing_with_hour_df.join(aggregate_df, ["turbine_id", "date"], "inner")
anomaly_threshold_df = anomaly_threshold_df.withColumn("is_anomaly", col("power_output") > col("anomaly_upper_threshold"))
filtered_anomaly = anomaly_threshold_df.filter("is_anomaly")
display(filtered_anomaly) 

# COMMAND ----------

# MAGIC %md
# MAGIC # Load into Database

# COMMAND ----------

# MAGIC %md
# MAGIC ## idempotence
# MAGIC make sure our bronze and silver schemas are built if this is the first time this code runs

# COMMAND ----------

spark.sql("""
create schema if not exists bronze.colibiri_interview;

""")
spark.sql("""
          create schema if not exists silver.colibiri_interview;
""")



# COMMAND ----------

# MAGIC %md
# MAGIC ## load into bronze 

# COMMAND ----------

landing_df = landing_df.withColumn("record_inserted_timestamp", current_timestamp()) \
   .withColumnRenamed("timestamp","turbine_datetime") \
   .write.mode("overwrite").saveAsTable("bronze.colibiri_interview.turbine_raw") 


# COMMAND ----------

# MAGIC %md
# MAGIC ## load into silver

# COMMAND ----------


#write summary statistics to Silver layer
aggregate_df.withColumn("record_added_timestamp", current_timestamp()) \
    .select("turbine_id", "date", "avg_power_output", "min_power_output", "max_power_output", "record_added_timestamp") \
    .write.mode("overwrite") \
    .saveAsTable("silver.colibiri_interview.turbine_statistics")


# COMMAND ----------


#write anomaly list to silver layer
filtered_anomaly.withColumn("record_added_timestamp", current_timestamp()) \
    .write.mode("overwrite") \
    .saveAsTable("silver.colibiri_interview.turbine_anomalies")
