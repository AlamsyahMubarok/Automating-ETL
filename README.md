# Automating ETL Process for AdventureWorks Sales Data

This project focuses on building an automated ETL (Extract, Transform, Load) pipeline using **SQL** and **Python** to process and analyze sales data from the AdventureWorks database. The goal is to efficiently extract raw transactional data, transform it into a clean analytical format, and load it into a data warehouse for further business intelligence and reporting purposes.

---

## Project Objectives

- Automate the extraction of sales data from a relational database (AdventureWorks)
- Perform transformation such as data cleaning, aggregation, and formatting
- Load the transformed data into a **data warehouse schema** (fact and dimension tables)
- Analyze **average sales**, **annual revenue trends**, and product performance
- Enable future integration with business dashboards or marketing tools

---

## Tools & Technologies

- **Python 3.x**
- **SQL (PostgreSQL / SQL Server)**
- **Pandas**, **SQLAlchemy**, **psycopg2**
- **DBMS:** AdventureWorks sample database
- **DBeaver** for database visualization
- **Jupyter Notebook** / Google Colab for experimentation

---

## ETL Workflow

1. **Extract**  
   - Connect to the AdventureWorks database  
   - Query sales-related tables such as `Sales.SalesOrderHeader`, `Sales.SalesOrderDetail`, `Production.Product`, and `Person.Customer`

2. **Transform**  
   - Join relevant tables to form meaningful relationships  
   - Convert raw timestamps, normalize text, and calculate derived fields (e.g. revenue = quantity * unit price)  
   - Handle missing values and apply filters based on business rules

3. **Load**  
   - Insert cleaned data into a structured **data warehouse schema**, including:
     - `fact_sales`
     - `dim_customer`
     - `dim_product`
     - `dim_date`
   - Ensure referential integrity with primary & foreign keys

---

## Key Insights

- Identified top-selling products by category and year
- Visualized yearly revenue trends and seasonality patterns
- Highlighted anomalies and outliers in sales regions or customer segments

---

## Future Enhancements

- Add support for **incremental loads** (e.g. using timestamps)
- Integrate ETL pipeline with **Airflow** or **Cron scheduler**
- Build interactive dashboard using **Tableau** or **Power BI**

---

## Author

Sultan Alamsyah Lintang Mubarok – Information Systems Student at Institute Technology Sepuluh Nopember  
*Focus on data engineering, analytics, and end-to-end automation*
