import pandas as pd
import mysql.connector

# -------------------------------
# 1️ EXTRACT - Read data from CSV
# -------------------------------
print("Step 1: Extracting data from CSV...")
data = pd.read_csv('sales_data.csv')
print("\nRaw Data from CSV:")
print(data)

# -------------------------------
# 2️ TRANSFORM - Clean and summarize data
# -------------------------------
print("\nStep 2: Transforming data...")

# Fill missing sale_amount values (if any)
data['sale_amount'] = data['sale_amount'].fillna(0)

# Group by employee and department, then sum sale_amount
summary = data.groupby(['employee_name', 'department'], as_index=False)['sale_amount'].sum()

# Rename column to total_sales
summary.rename(columns={'sale_amount': 'total_sales'}, inplace=True)

print("\nTransformed Data:")
print(summary)

# -------------------------------
# 3️ LOAD - Load data into MySQL table
# -------------------------------
print("\nStep 3: Loading data into MySQL database...")

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",            
    password="*****",  
    database="etl_db"       
)

cursor = conn.cursor()

# (Optional) Clear old data before inserting new
cursor.execute("DELETE FROM sales_summary")

# Insert each record row by row
for _, row in summary.iterrows():
    cursor.execute(
        "INSERT INTO sales_summary (employee_name, department, total_sales) VALUES (%s, %s, %s)",
        (row['employee_name'], row['department'], int(row['total_sales']))
    )

# Commit changes to save data
conn.commit()

# Close connections
cursor.close()
conn.close()

print("\nETL Process Completed Successfully! ")


