# ETL_Snowflake

ETL Snowflake + SampleSuperstore Project
This project uses the Sample Superstore dataset to imitate a real-world ETL use case using Snowflake. The goal is to showcase my skills in loading data into Snowflake, cleaning and transforming it to be ready for analytics.

Tech and Tools:
Data Source: Public SampleSuperstore dataset (CSV format)
Cloud Data Warehouse: Snowflake
ETL Tool: Python with pandas and snowflake-connector
Storage Layer: Snowflake stage (internal)
Target Tables: Snowflake

Data structure:
orders.csv
products.csv
customers.csv
regions.csv
order_details.csv

Step-by-step process:
1) Created Database, Schema, and Warehouse
2) Created Snowflake internal Stage
3) Uploaded raw SQLs to internal Stage using SnowSQL Command Line Interface
4) Created Target Tables in Snowflake
5) Copied data from Stage to Target tables
6) Created a transformed final consumption table

Why This Project?
This simulates ETL task commonly performed in BI Developer roles:
