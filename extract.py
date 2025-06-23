import pandas as pd
from sqlalchemy import create_engine

# Koneksi ke database OLTP (AdventureWorks)
oltp_engine = create_engine('postgresql+psycopg2://postgres:alamsyah25@localhost:5432/adventureworks')

# Koneksi ke database staging
staging_engine = create_engine('postgresql+psycopg2://postgres:alamsyah25@localhost:5432/db_staging')

# Ekstrak data pakai query eksplisit agar lebih aman
sales_order_header = pd.read_sql_query("SELECT * FROM sales.salesorderheader", oltp_engine)
sales_order_detail = pd.read_sql_query("SELECT * FROM sales.salesorderdetail", oltp_engine)
product = pd.read_sql_query("SELECT * FROM production.product", oltp_engine)
person = pd.read_sql_query("SELECT * FROM person.person", oltp_engine)
customer = pd.read_sql_query("SELECT * FROM sales.customer", oltp_engine)
sales_person = pd.read_sql_query("""
    SELECT businessentityid, territoryid, salesquota, bonus, commissionpct, 
           salesytd, saleslastyear, rowguid, modifieddate
    FROM sales.salesperson
""", oltp_engine)

# Simpan ke database staging
sales_order_header.to_sql('sales_order_header', staging_engine, if_exists='replace', index=False)
sales_order_detail.to_sql('sales_order_detail', staging_engine, if_exists='replace', index=False)
product.to_sql('product', staging_engine, if_exists='replace', index=False)
person.to_sql('person', staging_engine, if_exists='replace', index=False)
customer.to_sql('customer', staging_engine, if_exists='replace', index=False)
sales_person.to_sql('sales_person', staging_engine, if_exists='replace', index=False)

print("Data berhasil diekstrak dan dimasukkan ke staging.")
