# Training Insights Automation
## Project Overview
This project automates the processing of manual training records. It replaces slow, error-prone spreadsheets with a scalable Medallion Architecture pipeline in Databricks.

## The Problem
Before this project, training data was scattered across manual spreadsheets. This caused:

Operational Inefficiency: Coordinators spent hours manually checking rows.

Data Inconsistency: Names and dates were entered in multiple formats.

Hidden Insights: It was difficult for managers to see who had finished or who was lagging.

## The Solution: Medallion Architecture
The data moves through three automated stages:

Bronze (Raw): Ingests raw data from web sources and saves it as Managed Delta Tables.

Silver (Refined): Uses PySpark to clean data.

Flipping names (e.g., "Aby, Noel" to "Noel Aby").

Using Regexp to fix course names (removing dashes and extra spaces).

Using Coalesce and try_to_date to handle 8 different date formats.

Gold (Curated): Aggregates data into business metrics for the final dashboard.

## Tech Stack
Platform: Databricks

Language: Python (PySpark)

Storage: Delta Lake (ACID Compliant)

Automation: Databricks Jobs
