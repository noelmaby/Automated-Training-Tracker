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

##Architecture
<img width="1536" height="1024" alt="Designer" src="https://github.com/user-attachments/assets/b9d05e28-284b-4006-8ad3-b9ec7899a388" />

##Dashboard
<img width="1581" height="808" alt="Screenshot 2026-03-08 143015" src="https://github.com/user-attachments/assets/5d0b5692-c012-4cd5-9acb-cd510fdaf9c7" />
<img width="1588" height="809" alt="Screenshot 2026-03-08 142758" src="https://github.com/user-attachments/assets/32f126f0-1404-4ff2-8b17-f3b45f468950" />

