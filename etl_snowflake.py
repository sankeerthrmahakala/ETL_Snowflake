import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# Load credentials
load_dotenv()
sf_user = os.getenv("SNOWFLAKE_USER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD")
sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_database = os.getenv("SNOWFLAKE_DATABASE")
sf_schema = os.getenv("SNOWFLAKE_SCHEMA")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)
cs = conn.cursor()

# Helper function to load dataframe to Snowflake
def upload_dataframe(df, table_name):
    create_temp_stage = f"""
    CREATE OR REPLACE TEMPORARY STAGE temp_stage_{table_name};
    """
    cs.execute(create_temp_stage)

    df.to_csv(f"/tmp/{table_name}.csv", index=False)
    put_cmd = f"PUT file:///tmp/{table_name}.csv @temp_stage_{table_name} OVERWRITE = TRUE"
    cs.execute(put_cmd)

    copy_cmd = f"""
    COPY INTO {table_name}
    FROM @temp_stage_{table_name}/{table_name}.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """
    cs.execute(copy_cmd)

# Load CSVs
df_orders = pd.read_csv("/Users/sankeerthreddymahakala/Downloads/orders.csv") 
df_customers = pd.read_csv("/Users/sankeerthreddymahakala/Downloads/customers.csv")
df_products = pd.read_csv("/Users/sankeerthreddymahakala/Downloads/products.csv")
df_regions = pd.read_csv("/Users/sankeerthreddymahakala/Downloads/regions.csv")
df_order_details = pd.read_csv("/Users/sankeerthreddymahakala/Downloads/order_details.csv")

# Upload raw data
print("Uploading raw tables")
upload_dataframe(df_orders, "orders")
upload_dataframe(df_customers, "customers")
upload_dataframe(df_products, "products")
upload_dataframe(df_regions, "regions")
upload_dataframe(df_order_details, "order_details")


# Run transformation in Snowflake
print("transforming table")
transform_sql = """
CREATE OR REPLACE TABLE orders_cleaned AS
SELECT
	o.ORDER_ID,
	o.ORDER_DATE,
	o.SHIP_DATE,
	o.SHIP_MODE,
	o.REGION,
	c.CUSTOMER_ID,
	c.CUSTOMER_NAME,
	c.SEGMENT,
	r.COUNTRY,
	r.CITY,
	r.STATE,
	r.POSTAL_CODE,
	p.PRODUCT_ID,
	p.CATEGORY,
	p.SUB_CATEGORY,
	p.PRODUCT_NAME,
	od.SALES,
	od.QUANTITY, 
	od.DISCOUNT,
	od.PROFIT
FROM PORTFOLIO_DB.ANALYTICS.ORDERS o
JOIN PORTFOLIO_DB.ANALYTICS.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
JOIN PORTFOLIO_DB.ANALYTICS.ORDER_DETAILS od ON o.ORDER_ID = od.ORDER_ID 
JOIN PORTFOLIO_DB.ANALYTICS.PRODUCTS p ON od.PRODUCT_ID = p.PRODUCT_ID
JOIN PORTFOLIO_DB.ANALYTICS.REGIONS r ON o.REGION = r.REGION
WHERE od.Sales IS NOT NULL AND od.Quantity > 0;
"""
cs.execute(transform_sql)

print("ETL successfull.")

cs.close()
conn.close()
