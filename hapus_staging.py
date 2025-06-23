from sqlalchemy import create_engine, text

# Koneksi ke database staging
staging_engine = create_engine('postgresql+psycopg2://postgres:alamsyah25@localhost/db_staging')

# --- Clear old data in correct order ---
with staging_engine.begin() as connection:
    # Hapus data dari tabel fakta terlebih dahulu (jika ada)
    connection.execute(text('DELETE FROM fact_sales;'))

    # Hapus data dari tabel dimensi
    connection.execute(text('DELETE FROM dim_product;'))
    connection.execute(text('DELETE FROM dim_salesperson;'))
    connection.execute(text('DELETE FROM dim_time;'))

    # Hapus data dari tabel staging mentah (raw staging)
    connection.execute(text('DELETE FROM customer;'))
    connection.execute(text('DELETE FROM person;'))
    connection.execute(text('DELETE FROM product;'))
    connection.execute(text('DELETE FROM sales_order_detail;'))
    connection.execute(text('DELETE FROM sales_order_header;'))
    connection.execute(text('DELETE FROM sales_person;'))

print("✅ Data di database staging berhasil dibersihkan tanpa menghapus tabel dan relasi!")
