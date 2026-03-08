# Databricks notebook source
#Raw Data Ingestion
import requests

csv_url = "https://docs.google.com/spreadsheets/d/1qNAa2jrpEXSEAmw37rWd2nLA6ZyLSQc05cthFZYpBlE/export?format=csv"

volume_path = "/Volumes/training_details/bronze_layer/raw_data/training_data.csv"

response = requests.get(csv_url)
data_to_write = response.text
dbutils.fs.put(volume_path, data_to_write, overwrite=True)
print("File saved to databricks")

# COMMAND ----------

#Removing Spaces from Column Headers
from pyspark.sql.functions import col
raw_df=spark.read.csv(volume_path, header=True, inferSchema=True)

old_columns = raw_df.columns

new_columns = [c.replace(" ", "_").replace("\n", "").replace("\t", "").replace("(", "").replace(")", "").lower() for c in old_columns]

bronze_df = raw_df.toDF(*new_columns)

print(f"Old Columns: {old_columns}")
print(f"New Columns: {new_columns}")
bronze_df.printSchema()

# COMMAND ----------

bronze_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("training_details.bronze_layer.bronze_training_tracker")