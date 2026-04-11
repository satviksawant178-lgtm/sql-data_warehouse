USE DataWarehouse;

/*
Transformation performed on the basis of following points
    1. Check Unwanted spaces
    2. Check for Nulls 
    3. Check for duplicates in key columns
    4. If Date columns, check if end_date is greater than start_date
        or should start-end period of any two record should not be overlapping
*/

----------------------------- crm_cust_info ------------------------------------

SELECT count(1) FROM bronze.crm_cust_info;
SELECT count(1) FROM silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cust_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT *
FROM
(
    SELECT 
        cust_id
        , cst_key
        , TRIM(cst_firstname) as cst_firstname 
        , TRIM(cst_lastname) as cst_lastname
        , CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'N/A'
            END AS cst_marital_status
        , CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'N/A'
            END AS cst_gndr
        , cst_create_date
    FROM
    (   
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC) as cst_rw
        FROM bronze.crm_cust_info
    ) cst
    WHERE cst_rw = 1
    AND cust_id IS NOT NULL
) cst_silver



----------------------------- crm_prd_info ------------------------------------

SELECT COUNT(1) as bronze_cnt FROM bronze.crm_prd_info;
SELECT COUNT(1) as silver_cnt FROM silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT *
FROM
(
    SELECT  
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
        prd_start_dt,
        derived_prd_end_dt as prd_end_date
    FROM (
        SELECT *,
            -- we can directly do this change on column level where 
            -- we are defining 'derived_prd_end_dt' as 'prd_end_date'  
            DATEADD(DAY, -1, nxt_row_start_date) as derived_prd_end_dt
        FROM
        (
            SELECT *,
                    LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) as nxt_row_start_date
            FROM bronze.crm_prd_info
        ) prd
    ) prd_date_rectified
) prd_silver


----------------------------- crm_sales_details ------------------------------------

SELECT COUNT(1) as bronze_cnt FROM bronze.crm_sales_details;
SELECT COUNT(1) as silver_cnt FROM silver.crm_sales_details;


INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN LEN(sls_order_dt) < 8 -- This logic can be applied to other 2 date columns as well
        THEN NULL
        ELSE CAST(CONCAT(SUBSTRING(sls_order_dt, 1, 4), '-',
            SUBSTRING(sls_order_dt, 5, 2), '-', 
            SUBSTRING(sls_order_dt, 7, 2))as DATE) 
    END AS sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM
(
    SELECT sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        CAST(CONCAT(SUBSTRING(sls_ship_dt, 1, 4), '-',
                SUBSTRING(sls_ship_dt, 5, 2), '-', 
                SUBSTRING(sls_ship_dt, 7, 2))as DATE) as sls_ship_dt,

        CAST(CONCAT(SUBSTRING(sls_due_dt, 1, 4), '-',
                SUBSTRING(sls_due_dt, 5, 2), '-', 
                SUBSTRING(sls_due_dt, 7, 2))as DATE) as sls_due_dt,
        CASE 
            WHEN sls_sales <= 0 OR sls_sales IS NULL 
                OR sls_sales = ABS(sls_quantity*sls_price)
            THEN sls_price
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price <= 0 OR sls_price IS NULL
            THEN sls_sales/ NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details
) sls_dt;

/*
    Observation i.e. order_dt + 7 = ship_dt
SELECT *,
    DATEDIFF(DAY, sls_order_dt, sls_ship_dt) AS 'shipMinorder'
FROM 
(
    SELECT DISTINCT 
            CAST(CONCAT(SUBSTRING(sls_order_dt, 1, 4), '-',
            SUBSTRING(sls_order_dt, 5, 2), '-', 
            SUBSTRING(sls_order_dt, 7, 2))as DATE) as sls_order_dt,

            CAST(CONCAT(SUBSTRING(sls_ship_dt, 1, 4), '-',
            SUBSTRING(sls_ship_dt, 5, 2), '-', 
            SUBSTRING(sls_ship_dt, 7, 2))as DATE) as sls_ship_dt,
            
            CAST(CONCAT(SUBSTRING(sls_due_dt, 1, 4), '-',
            SUBSTRING(sls_due_dt, 5, 2), '-', 
            SUBSTRING(sls_due_dt, 7, 2))as DATE) as sls_due_dt
    FROM bronze.crm_sales_details
    WHERE LEN(sls_order_dt) = 8
) dts

*/

----------------------------- erp_cust_az12 ------------------------------

SELECT COUNT(1) FROM bronze.erp_cust_az12;
SELECT COUNT(1) FROM silver.erp_cust_az12;
SELECT * FROM silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12(
    cid,
    bdate,
    gen
)
SELECT cid,
    bdate,
    gen
FROM
(
    SELECT 
        CASE 
            WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
            ELSE CID
        END AS cid,
        CASE 
            WHEN BDATE > CAST(GETDATE() AS DATE) THEN NULL
            ELSE BDATE
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(GEN)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(GEN)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(GEN)) IS NULL OR GEN = '' THEN 'N/A'
            ELSE GEN
        END AS gen
    FROM bronze.erp_cust_az12
) cst12;


----------------------------- erp_cust_az12 ------------------------------

SELECT COUNT(1) FROM bronze.erp_loc_a101;
SELECT COUNT(1) FROM silver.erp_loc_a101;


INSERT INTO silver.erp_loc_a101 (
    CID, CNTRY
)
SELECT REPLACE(CID, '-', '') as CID , 
    CASE  
        WHEN CNTRY = 'DE' THEN 'Germany'
        WHEN CNTRY IN ('USA', 'United States', 'US') THEN 'United States of America'
        WHEN CNTRY = '' THEN NULL
        ELSE CNTRY
    END AS CNTRY
FROM bronze.erp_loc_a101;


-------------------------- erp_px_cat_g1v2 ----------------------

SELECT COUNT(1) FROM bronze.erp_px_cat_g1v2;
SELECT COUNT(1) FROM silver.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance
)
SELECT *
FROM bronze.erp_px_cat_g1v2


