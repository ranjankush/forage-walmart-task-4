import pandas as pd
import sqlite3
import os

# Set the working directory
os.chdir(r"C:\Users\ranja\OneDrive\Desktop\Internship Project\Forage\file\forage-walmart-task-4")

# Connect to the SQLite database
conn = sqlite3.connect('shipment_database.db')
cursor = conn.cursor()

# Function to insert data into a table
def insert_data(query, data):
    cursor.executemany(query, data)
    conn.commit()

# Part 1: Populate shipping_data_0 (self-contained product data)
def populate_shipping_data_0():
    df = pd.read_csv('data/shipping_data_0.csv')
    
    # Insert products into product table
    products = df['product'].unique()  # Get unique product names
    product_data = [(name,) for name in products]
    insert_query = "INSERT OR IGNORE INTO product (name) VALUES (?)"
    insert_data(insert_query, product_data)

    # Map product names to IDs
    cursor.execute("SELECT id, name FROM product")
    product_ids = {name: id for id, name in cursor.fetchall()}

    # Insert shipments into shipment table
    shipments = []
    for _, row in df.iterrows():
        product_id = product_ids[row['product']]
        quantity = row['product_quantity']
        origin = row['origin_warehouse']
        destination = row['destination_store']
        shipments.append((product_id, quantity, origin, destination))

    insert_query = "INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)"
    insert_data(insert_query, shipments)
    print("Inserted data from shipping_data_0.csv into product and shipment tables.")

# Part 2: Populate shipping_data_1 and shipping_data_2 (dependent data)
def populate_shipping_data_1_and_2():
    df1 = pd.read_csv('data/shipping_data_1.csv')  # Products per shipment
    df2 = pd.read_csv('data/shipping_data_2.csv')  # Shipment details

    # Insert products into product table
    products = df1['product'].unique()
    product_data = [(name,) for name in products]
    insert_query = "INSERT OR IGNORE INTO product (name) VALUES (?)"
    insert_data(insert_query, product_data)

    # Map product names to IDs
    cursor.execute("SELECT id, name FROM product")
    product_ids = {name: id for id, name in cursor.fetchall()}

    # Combine df1 and df2
    df_merged = pd.merge(
        df1.groupby('shipment_identifier')['product'].value_counts().reset_index(name='quantity'),
        df2[['shipment_identifier', 'origin_warehouse', 'destination_store']],
        on='shipment_identifier'
    )

    # Insert shipments into shipment table
    shipments = []
    for _, row in df_merged.iterrows():
        product_id = product_ids[row['product']]
        quantity = row['quantity']
        origin = row['origin_warehouse']
        destination = row['destination_store']
        shipments.append((product_id, quantity, origin, destination))

    insert_query = "INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)"
    insert_data(insert_query, shipments)
    print("Inserted data from shipping_data_1.csv and shipping_data_2.csv into product and shipment tables.")

# Main execution
try:
    # Print database schema for verification
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in database:", tables)
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table[0]})")
        print(f"Schema for {table[0]}:", cursor.fetchall())

    populate_shipping_data_0()
    populate_shipping_data_1_and_2()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    conn.close()
    print("Database connection closed.")