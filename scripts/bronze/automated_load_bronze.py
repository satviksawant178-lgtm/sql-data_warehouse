import pyodbc
import time

# Connection to sql-server
conn = pyodbc.connect(
    "DRIVER={SQL Server};"
    "SERVER=LAPTOP-FRTLHIV7\SQLEXPRESS;"
    "DATABASE=DataWarehouse;"
    "Trusted_Connection=yes;"  # Windows authentication
)

cursor = conn.cursor()

# Map each table to its CSV file

tables = [
    ("bronze.crm_cust_info",  r"C:\Users\SATVIK\Satvik Course Work\CoreSQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"),
    ("bronze.crm_prd_info",  r"C:\Users\SATVIK\Satvik Course Work\CoreSQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"),
    ("bronze.crm_sales_details",  r"C:\Users\SATVIK\Satvik Course Work\CoreSQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"),
    ("bronze.erp_cust_az12",  r"C:\Users\SATVIK\Satvik Course Work\CoreSQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"),
    ("bronze.erp_loc_a101",  r"C:\Users\SATVIK\Satvik Course Work\CoreSQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"),
    ("bronze.erp_px_cat_g1v2",  r"C:\Users\SATVIK\Satvik Course Work\CoreSQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"),

]

start = time.time()

for table, csv_path in tables:
    print(f"Loading {table}...")
    cursor.execute(f"TRUNCATE TABLE {table}")
    cursor.execute(f"""
        BULK INSERT {table}
        FROM '{csv_path}'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK)
    """)
    conn.commit()
    print(f" {table}  Loading Completed.")

print(f"All tables loaded in {round(time.time() - start, 2)}s")
cursor.close()
conn.close()
