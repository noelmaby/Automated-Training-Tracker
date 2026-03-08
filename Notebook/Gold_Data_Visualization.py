# Databricks notebook source
from pyspark.sql.functions import (
    count,round,date_format,when,col
)
silver_df = spark.table("training_details.silver_layer.silver_training_tracker")

gold_course_summary = (silver_df
    .groupBy("course_name")
    .agg(
        count("*").alias("Total_Enrolled"),
        count(when(col("status") == "Completed", True)).alias("Total_Completed"),
        count(when(col("status") != "Completed", True)).alias("Total_Pending")
    )

    .withColumn("Completion_Rate_Percent", 
                round((col("Total_Completed") / col("Total_Enrolled")) * 100, 2))
)

(gold_course_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("training_details.gold_layer.course_summary"))

display(gold_course_summary)

# COMMAND ----------


gold_associate = (silver_df
    .select(
        col("associate_name"),
        col("course_name"), 
        col("status").alias("Status"),
        when(col("completion_date").isNull(), "Pending")
        .otherwise(date_format(col("completion_date"), "dd/MM/yyyy")).alias("Completion_Date")
    )
    
    .orderBy("associate_name", "course_name") 
)

(gold_associate.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("training_details.gold_layer.associate_status"))

display(gold_associate)