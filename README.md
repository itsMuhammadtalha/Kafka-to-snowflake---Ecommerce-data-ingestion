


# 🛒 Real-Time E-Commerce Analytics Pipeline

This project demonstrates an **end-to-end data ingestion pipeline** using **Kafka, Python, and Snowflake** for a small e-commerce store. It simulates real-time order events, processes them through Kafka, and stores them in a Snowflake star schema for analytics and reporting.


## ⚙️ Setup Instructions

- install the requirements.txt file -> 
  ```$ pip install -r requirements.txt


---

### 🏗️ Snowflake Setup

1. Sign up or log in to your Snowflake account.
2. Create a database and schema:
   ```sql
   CREATE OR REPLACE DATABASE E_COMMERCE;
   USE DATABASE E_COMMERCE;
   USE SCHEMA PUBLIC;
   ```
3. Run the SQL from the file `snowflake_star_schema.sql`.

---

## ❄️ Star Schema Design

The star schema includes:

- **Fact Table:** `FACT_ORDERS`
- **Dimension Tables:** `DIM_CUSTOMER`, `DIM_PRODUCT`, `DIM_DATE`

### 🌟 Class Diagram
![Class diagrma ](https://github.com/itsMuhammadtalha/Kafka-to-snowflake---Ecommerce-data-ingestion/blob/master/starschema-ecommerce.drawio.png)



---

## 🔧 DDL Script (snowflake_star_schema.sql)

Paste your full Snowflake schema DDL here:

<details>
<summary>Click to expand</summary>

```sql
CREATE OR REPLACE DATABASE E_COMMERCE;
USE DATABASE E_COMMERCE;

CREATE OR REPLACE SCHEMA E_COMMERCE;


-- DIM_CUSTOMER
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_ID VARCHAR PRIMARY KEY,
    CUSTOMER_NAME VARCHAR,
    CUSTOMER_REGION VARCHAR
);

-- DIM_PRODUCT
CREATE OR REPLACE TABLE DIM_PRODUCT (
    PRODUCT_ID VARCHAR PRIMARY KEY,
    PRODUCT_NAME VARCHAR,
    CATEGORY VARCHAR
);

-- DIM_DATE
CREATE OR REPLACE TABLE DIM_DATE (
    ORDER_DATE DATE PRIMARY KEY,
    DAY INT,
    MONTH INT,
    YEAR INT,
    WEEKDAY VARCHAR
);

-- FACT_ORDERS
CREATE OR REPLACE TABLE FACT_ORDERS (
    ORDER_ID VARCHAR PRIMARY KEY,
    ORDER_TIMESTAMP TIMESTAMP,
    CUSTOMER_ID VARCHAR REFERENCES DIM_CUSTOMER(CUSTOMER_ID),
    PRODUCT_ID VARCHAR REFERENCES DIM_PRODUCT(PRODUCT_ID),
    ORDER_DATE DATE REFERENCES DIM_DATE(ORDER_DATE),
    PRICE NUMBER,
    QUANTITY NUMBER,
    TOTAL_AMOUNT NUMBER
);


select * from fact_orders;

select * from dim_product;


-- minthly sales 

SELECT 
    d.YEAR,
    d.MONTH,
    p.CATEGORY,
    c.CUSTOMER_REGION,
    SUM(f.TOTAL_AMOUNT) AS TOTAL_SALES
FROM FACT_ORDERS f
JOIN DIM_DATE d ON f.ORDER_DATE = d.ORDER_DATE
JOIN DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY d.YEAR, d.MONTH, p.CATEGORY, c.CUSTOMER_REGION
ORDER BY d.YEAR, d.MONTH;


-- mosty repeated customer 
SELECT 
    f.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    COUNT(*) AS ORDER_COUNT
FROM FACT_ORDERS f
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
WHERE f.ORDER_TIMESTAMP >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)
GROUP BY f.CUSTOMER_ID, c.CUSTOMER_NAME
LIMIT 5;

-- qualntity sold 
SELECT 
    f.PRODUCT_ID,
    p.PRODUCT_NAME,
    SUM(f.QUANTITY) AS TOTAL_QUANTITY_SOLD,
    AVG(f.PRICE) AS AVERAGE_PRICE
FROM FACT_ORDERS f
JOIN DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
GROUP BY f.PRODUCT_ID, p.PRODUCT_NAME;

select * from dim_customer;

-- customer place order multip region
SELECT 
    c.CUSTOMER_ID,
    COUNT(DISTINCT CUSTOMER_REGION) AS REGION_COUNT
FROM FACT_ORDERS f
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY CUSTOMER_ID
HAVING REGION_COUNT > 1;


-- slowly changing dimensions can be handled using surrogate keys or tracking record of the CreateDate and UpdateDate in this way 
-- if any of the column is changed/updated we will have a version or a record of it at the time of creation 






```

</details>

---

## 🐍 Kafka Producer (kafka_producer.py)

<details>
<summary>Click to expand</summary>

```python
from kafka import KafkaProducer
import json, time, random
from faker import Faker

fake = Faker()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

products = [
    {"product_id": "P123", "product_name": "Wireless Mouse", "category": "Accessories", "price": 2500},
    {"product_id": "P124", "product_name": "Keyboard", "category": "Accessories", "price": 3000},
    {"product_id": "P125", "product_name": "Monitor", "category": "Electronics", "price": 20000}
]

for i in range(100):
    product = random.choice(products)
    message = {
        "order_id": f"O{1000+i}",
        "order_timestamp": fake.iso8601(),
        "customer_id": f"C{random.randint(100, 999)}",
        "customer_name": fake.name(),
        "customer_region": fake.state(),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 5)
    }
    producer.send("orders", message)
    print(f"Sent: {message}")
    time.sleep(1)

```

</details>

---

## 🐍 Kafka Consumer + Snowflake Ingestion (kafka_consumer_snowflake.py)

<details>
<summary>Click to expand</summary>

```python
from kafka import KafkaConsumer
import json
import snowflake.connector
from datetime import datetime
#2J5FUKRvmVqaPr3
# Snowflake connection
conn = snowflake.connector.connect(
    user='MUHAMMADTALHA1',  # ← from "User Name"
    password='2J5FUKRvmVqaPr3',  # ← set this securely (don't hardcode if deploying)
    account='PCEFAXP-HK41377',  # ← from "Account Identifier"
    warehouse='E_COMMERCE',  # ← default or the one you created
    database='E_COMMERCE',  # ← the one you create
    schema='E_COMMERCE',    # ← can match your DB name
    role='ACCOUNTADMIN'     # ← from "Role"
)
cs = conn.cursor()

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def insert_dim_date(order_date):
    cs.execute("""
        MERGE INTO DIM_DATE t USING (SELECT %s AS ORDER_DATE) s
        ON t.ORDER_DATE = s.ORDER_DATE
        WHEN NOT MATCHED THEN INSERT (
            ORDER_DATE, DAY, MONTH, YEAR, WEEKDAY
        ) VALUES (%s, %s, %s, %s, %s)
    """, (order_date, order_date, order_date.day, order_date.month, order_date.year, order_date.strftime("%A")))

for msg in consumer:
    data = msg.value
    order_ts = datetime.fromisoformat(data['order_timestamp'].replace("Z", "+00:00"))
    order_date = order_ts.date()

    # Insert DIM_CUSTOMER
    cs.execute("""
        MERGE INTO DIM_CUSTOMER USING (SELECT %s AS ID) s ON DIM_CUSTOMER.CUSTOMER_ID = s.ID
        WHEN NOT MATCHED THEN INSERT (CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION)
        VALUES (%s, %s, %s)
    """, (data['customer_id'], data['customer_id'], data['customer_name'], data['customer_region']))

    # Insert DIM_PRODUCT
    cs.execute("""
        MERGE INTO DIM_PRODUCT USING (SELECT %s AS ID) s ON DIM_PRODUCT.PRODUCT_ID = s.ID
        WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY)
        VALUES (%s, %s, %s)
    """, (data['product_id'], data['product_id'], data['product_name'], data['category']))

    # Insert DIM_DATE
    insert_dim_date(order_date)

    # Insert FACT_ORDERS
    total_amount = data['price'] * data['quantity']
    cs.execute("""
        INSERT INTO FACT_ORDERS (ORDER_ID, ORDER_TIMESTAMP, CUSTOMER_ID, PRODUCT_ID, ORDER_DATE, PRICE, QUANTITY, TOTAL_AMOUNT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (data['order_id'], order_ts, data['customer_id'], data['product_id'], order_date,
          data['price'], data['quantity'], total_amount))

    print(f"Inserted order {data['order_id']} into Snowflake")

cs.close()
conn.close()

```

</details>

---

## 📊 SQL Queries (sql_queries.sql)

Paste your analytics SQL queries here:

<details>
<summary>Click to expand</summary>

```sql
-- minthly sales 

SELECT 
    d.YEAR,
    d.MONTH,
    p.CATEGORY,
    c.CUSTOMER_REGION,
    SUM(f.TOTAL_AMOUNT) AS TOTAL_SALES
FROM FACT_ORDERS f
JOIN DIM_DATE d ON f.ORDER_DATE = d.ORDER_DATE
JOIN DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY d.YEAR, d.MONTH, p.CATEGORY, c.CUSTOMER_REGION
ORDER BY d.YEAR, d.MONTH;


-- mosty repeated customer 
SELECT 
    f.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    COUNT(*) AS ORDER_COUNT
FROM FACT_ORDERS f
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
WHERE f.ORDER_TIMESTAMP >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)
GROUP BY f.CUSTOMER_ID, c.CUSTOMER_NAME
LIMIT 5;

-- qualntity sold 
SELECT 
    f.PRODUCT_ID,
    p.PRODUCT_NAME,
    SUM(f.QUANTITY) AS TOTAL_QUANTITY_SOLD,
    AVG(f.PRICE) AS AVERAGE_PRICE
FROM FACT_ORDERS f
JOIN DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
GROUP BY f.PRODUCT_ID, p.PRODUCT_NAME;

select * from dim_customer;

-- customer place order multip region
SELECT 
    c.CUSTOMER_ID,
    COUNT(DISTINCT CUSTOMER_REGION) AS REGION_COUNT
FROM FACT_ORDERS f
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY CUSTOMER_ID
HAVING REGION_COUNT > 1;


</details>

---


## 📝 Assumptions

- Order IDs are unique.
- Customer and product data are consistent across orders.
- Real-time ingestion assumes dimension data is either known or derived from order data.
- No historical tracking yet — can be extended using SCD Type 2 in the future.

---

