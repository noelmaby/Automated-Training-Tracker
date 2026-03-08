# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_replace, try_to_date, 
    coalesce, lit, split, element_at, concat_ws, initcap
)
bronze_df = spark.table("training_details.bronze_layer.bronze_training_tracker")

clean_date_str = regexp_replace(col("completion_date"), "\n", "")
associate_name_split=split(col("associate_name"), ",")
#Transformations
silver_df = (
    bronze_df
        .filter(col("associate_name").isNotNull())
        .select(
            col("associate_id"),
            when(col("associate_name").contains(","), 
                 concat_ws(" ", 
                           trim(element_at(associate_name_split, 2)), 
                           trim(element_at(associate_name_split, 1))
                 )
            ).otherwise(col("associate_name")).alias("associate_name"),

            coalesce(
                initcap(
                    trim(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(col("course_name"), "\n", ""), 
                            "-", " "), 
                        " +", " ")
                    )
                ),
                lit("Pending Assignment")
            ).alias("course_name"),

            when(col("status").isNull(), "Not Started")
            .when(lower(col("status")).contains("progress"), "In Progress")
            .when(lower(col("status")).contains("completed"), "Completed")
            .otherwise("Not Started").alias("status"),

            coalesce(
                try_to_date(clean_date_str, "dd/MM/yyyy"), 
                try_to_date(clean_date_str, "dd/MM/yy"),
                try_to_date(clean_date_str,"dd/M/yy"),
                try_to_date(clean_date_str,"dd/M/yyyy"),
                try_to_date(clean_date_str,"d/M/yy"),
                try_to_date(clean_date_str,"d/M/yyyy"),
                try_to_date(clean_date_str,"d/MM/yy"),
                try_to_date(clean_date_str,"d/MM/yyyy")
            ).alias("completion_date")
        )
)



(silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("training_details.silver_layer.silver_training_tracker")
)
display(spark.table("training_details.silver_layer.silver_training_tracker"))