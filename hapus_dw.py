# cleardw.py
from sqlalchemy import create_engine, text

# Koneksi ke Data Warehouse kamu
dw_engine = create_engine('postgresql+psycopg2://postgres:alamsyah25@localhost/adventureworks_dw')

with dw_engine.begin() as connection:
    # Hapus data dari tabel fakta terlebih dahulu
    connection.execute(text('DELETE FROM fact_sales;'))

    # Hapus data dari dimensi
    connection.execute(text('DELETE FROM dim_product;'))
    connection.execute(text('DELETE FROM dim_salesperson;'))
    connection.execute(text('DELETE FROM dim_time;'))

print("✅ Data di Data Warehouse berhasil dibersihkan tanpa menghapus struktur tabel!")
