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
