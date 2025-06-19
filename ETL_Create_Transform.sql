
CREATE WAREHOUSE etl_wh WITH WAREHOUSE_SIZE = 'XSMALL';
CREATE DATABASE retail_db;
USE DATABASE retail_db;
CREATE SCHEMA sales_data;

--------------------------------------------------------------------------------------
--Creating internal stage
CREATE OR REPLACE STAGE retail_stage;



--------------------------------------------------------------------------------------
--Creating Tables

create or replace TABLE RETAIL_DB.SALES_DATA.ORDERS (
	ORDER_ID VARCHAR NOT NULL,
	ORDER_DATE DATE,
	SHIP_DATE DATE,
	SHIP_MODE VARCHAR,
	CUSTOMER_ID VARCHAR,
	REGION VARCHAR,
	primary key (ORDER_ID)
);


create or replace TABLE RETAIL_DB.SALES_DATA.CUSTOMERS (
	CUSTOMER_ID VARCHAR NOT NULL,
	CUSTOMER_NAME VARCHAR,
	SEGMENT VARCHAR,
	primary key (CUSTOMER_ID)
);

create or replace TABLE RETAIL_DB.SALES_DATA.ORDER_DETAILS (
	ORDER_ID VARCHAR NOT NULL,
	PRODUCT_ID VARCHAR NOT NULL,
	SALES FLOAT,
	QUANTITY INT,
	DISCOUNT FLOAT,
	PROFIT FLOAT,
	primary key (ORDER_ID, PRODUCT_ID),
	foreign key (ORDER_ID) references PORTFOLIO_DB.ANALYTICS.ORDERS(ORDER_ID),
	foreign key (PRODUCT_ID) references PORTFOLIO_DB.ANALYTICS.PRODUCTS(PRODUCT_ID)
);

create or replace TABLE RETAIL_DB.SALES_DATA.PRODUCTS (
	PRODUCT_ID VARCHAR NOT NULL,
	PRODUCT_NAME VARCHAR,
	SUB_CATEGORY VARCHAR,
	CATEGORY VARCHAR,
	primary key (PRODUCT_ID)
);


create or replace TABLE RETAIL_DB.SALES_DATA.REGIONS (
	REGION VARCHAR NOT NULL,
	COUNTRY VARCHAR NOT NULL,
	STATE VARCHAR NOT NULL,
	CITY VARCHAR NOT NULL,
	POSTAL_CODE VARCHAR NOT NULL,
	primary key (REGION, COUNTRY, STATE, CITY, POSTAL_CODE)
);




--------------------------------------------------------------------------------------
--Loading data into stage files

COPY INTO sales_data.orders
FROM @retail_stage/orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO sales_data.products
FROM @retail_stage/products.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO sales_data.customers
FROM @retail_stage/customers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO sales_data.regions
FROM @retail_stage/regions.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);


COPY INTO sales_data.order_details
FROM @retail_stage/order_details.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);




--------------------------------------------------------------------------------------
--Creating and trasforming Table 
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
INNER JOIN PORTFOLIO_DB.ANALYTICS.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
INNER JOIN PORTFOLIO_DB.ANALYTICS.ORDER_DETAILS od ON o.ORDER_ID = od.ORDER_ID 
INNER JOIN PORTFOLIO_DB.ANALYTICS.PRODUCTS p ON od.PRODUCT_ID = p.PRODUCT_ID
INNER JOIN PORTFOLIO_DB.ANALYTICS.REGIONS r ON o.REGION = r.REGION
WHERE od.Sales IS NOT NULL AND od.Quantity > 0;



