import psycopg2
from psycopg2 import pool
import numpy as np
from datetime import datetime, timedelta
import pandas as pd

# Define the connection pool
connection_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=5,
    maxconn=20,
    dbname="postgres",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)

def generate_and_insert(nrows: int):
    names = np.asarray(
        [
            "Laptop",
            "Smartphone",
            "Desk",
            "Chair",
            "Monitor",
            "Printer",
            "Paper",
            "Pen",
            "Notebook",
            "Coffee Maker",
            "Cabinet",
            "Plastic Cups",
        ]
    )

    categories = np.asarray(
        [
            "Electronics",
            "Electronics",
            "Office",
            "Office",
            "Electronics",
            "Electronics",
            "Stationery",
            "Stationery",
            "Stationery",
            "Electronics",
            "Office",
            "Sundry",
        ]
    )

    product_id = np.random.randint(len(names), size=nrows)
    quantity = np.random.randint(1, 11, size=nrows)
    price = np.random.randint(199, 10000, size=nrows) / 100

    # Generate random dates between 2010-01-01 and 2023-12-31
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2023, 12, 31)
    date_range = (end_date - start_date).days

    # Create random dates as np.array and convert to string format
    order_dates = np.array([(start_date + timedelta(days=np.random.randint(0, date_range))).strftime('%Y-%m-%d') for _ in range(nrows)])

    # Define columns
    columns = {
        "order_id": np.arange(nrows),
        "order_date": order_dates,
        "customer_id": np.random.randint(100, 1000, size=nrows),
        "customer_name": [f"Customer_{i}" for i in np.random.randint(2**15, size=nrows)],
        "product_id": product_id + 200,
        "product_names": names[product_id],
        "categories": categories[product_id],
        "quantity": quantity,
        "price": price,
        "total": price * quantity,
    }

    # Create DataFrame from generated data
    df = pd.DataFrame(columns)

    # Insert data into the PostgreSQL database
    try:
        # Get a connection from the pool
        conn = connection_pool.getconn()
        cursor = conn.cursor()

        # Insert each row into the sales_data table
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO public.sales_data (
                    order_id, order_date, customer_id, customer_name, product_id, 
                    product_names, categories, quantity, price, total
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['order_id'], row['order_date'], row['customer_id'], row['customer_name'], 
                row['product_id'], row['product_names'], row['categories'], 
                row['quantity'], row['price'], row['total']
            ))

        # Commit the transaction
        conn.commit()
        print(f"{nrows} rows have been inserted successfully.")

    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        # Release the connection back to the pool
        if conn:
            connection_pool.putconn(conn)

# Generate and insert 100,000 rows of data directly into the database
generate_and_insert(100_000)
