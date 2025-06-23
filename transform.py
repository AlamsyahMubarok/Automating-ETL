import pandas as pd
from sqlalchemy import create_engine, text

# Koneksi ke PostgreSQL
engine = create_engine('postgresql://postgres:alamsyah25@localhost:5432/db_staging')

with engine.begin() as conn:
    # Kosongkan data terlebih dahulu
    conn.execute(text("DELETE FROM fact_sales"))
    conn.execute(text("DELETE FROM dim_product"))
    conn.execute(text("DELETE FROM dim_salesperson"))
    conn.execute(text("DELETE FROM dim_time"))

# --- 1. Transformasi ke dim_product ---
df_product = pd.read_sql('SELECT productid, name AS product_name, modifieddate FROM product', engine)
df_product = df_product.drop_duplicates(subset=['productid'])
df_product.to_sql('dim_product', engine, if_exists='append', index=False)

# --- 2. Transformasi ke dim_salesperson ---
query_salesperson = """
SELECT DISTINCT soh.salespersonid,
       sp.businessentityid AS employeeid,
       CONCAT(p.firstname, ' ', p.lastname) AS fullname,
       sp.modifieddate
FROM sales_order_header soh
JOIN sales_person sp ON soh.salespersonid = sp.businessentityid
JOIN person p ON sp.businessentityid = p.businessentityid
WHERE soh.salespersonid IS NOT NULL
"""
df_salesperson = pd.read_sql(query_salesperson, engine)
df_salesperson = df_salesperson.drop_duplicates(subset=['salespersonid'])
df_salesperson.to_sql('dim_salesperson', engine, if_exists='append', index=False)

# --- 3. Transformasi ke dim_time ---
df_order_header = pd.read_sql("SELECT orderdate FROM sales_order_header", engine)
df_order_header['time_date'] = pd.to_datetime(df_order_header['orderdate'])
df_order_header['year'] = df_order_header['time_date'].dt.year
df_order_header['month'] = df_order_header['time_date'].dt.month
df_order_header['day'] = df_order_header['time_date'].dt.day
df_order_header['quarter'] = df_order_header['time_date'].dt.quarter
df_time = df_order_header[['time_date', 'year', 'month', 'day', 'quarter']].drop_duplicates()
df_time.to_sql('dim_time', engine, if_exists='append', index=False)

# --- 4. Transformasi ke fact_sales ---
query_fact_sales = """
SELECT soh.salesorderid,
       soh.orderdate,
       sod.productid,
       sod.orderqty,
       sod.unitprice,
       (sod.orderqty * sod.unitprice) AS totalline,
       soh.totaldue,
       soh.salespersonid,
       soh.modifieddate
FROM sales_order_detail sod
JOIN sales_order_header soh ON sod.salesorderid = soh.salesorderid
"""
df_fact_sales = pd.read_sql(query_fact_sales, engine)
df_fact_sales.to_sql('fact_sales', engine, if_exists='append', index=False)

print("✅ Transformasi selesai. Semua tabel dim dan fact telah diperbarui di dalam database staging.")
