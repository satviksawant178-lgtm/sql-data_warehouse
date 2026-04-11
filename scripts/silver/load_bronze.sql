USE DataWarehouse;

BULK INSERT [bronze].[crm_cust_info]
FROM 'C:\Users\SATVIK\Satvik Course Work\CoreSQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
)

SELECT * FROM bronze.crm_cust_info;

SELECT COUNT(1) FROM bronze.crm_cust_info;

