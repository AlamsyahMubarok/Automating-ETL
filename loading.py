import pandas as pd
from sqlalchemy import create_engine, text

# --- KONFIGURASI KONEKSI DATABASE ---
staging_engine = create_engine('postgresql+psycopg2://postgres:alamsyah25@localhost/db_staging')
dw_engine = create_engine('postgresql+psycopg2://postgres:alamsyah25@localhost/adventureworks_dw')  # Ganti jika nama DB DW beda

# --- HAPUS DATA LAMA DI DATA WAREHOUSE ---
with dw_engine.begin() as connection:
    connection.execute(text('DELETE FROM fact_sales;'))
    connection.execute(text('DELETE FROM dim_product;'))
    connection.execute(text('DELETE FROM dim_salesperson;'))
    connection.execute(text('DELETE FROM dim_time;'))

# --- LOAD DIM PRODUCT ---
df_product = pd.read_sql_table('dim_product', staging_engine)
df_product.to_sql('dim_product', dw_engine, if_exists='append', index=False)

# --- LOAD DIM SALESPERSON ---
df_salesperson = pd.read_sql_table('dim_salesperson', staging_engine)
df_salesperson.to_sql('dim_salesperson', dw_engine, if_exists='append', index=False)

# --- LOAD DIM TIME ---
df_time = pd.read_sql_table('dim_time', staging_engine)
df_time.to_sql('dim_time', dw_engine, if_exists='append', index=False)

# --- LOAD FACT SALES ---
df_fact_sales = pd.read_sql_table('fact_sales', staging_engine)
df_fact_sales.to_sql('fact_sales', dw_engine, if_exists='append', index=False)

print("✅ Loading dari staging ke data warehouse selesai.")
