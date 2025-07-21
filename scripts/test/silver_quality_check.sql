/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


select * from silver.crm_prd_info

select prd_id, count(*) from silver.crm_prd_info 
group by prd_id
having count(*) > 1

select prd_nm from silver.crm_prd_info where trim(prd_nm) != prd_nm
select prd_cost from silver.crm_prd_info where prd_cost < 0 or prd_cost is null
select distinct(prd_line) from  silver.crm_prd_info
select * from silver.crm_prd_info where prd_start_dt > prd_end_dt
select prd_id, prd_key, prd_nm, prd_start_dt, prd_end_dt, 
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test 
from silver.crm_prd_info where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

SELECT 
	prd_id, 
	prd_key, 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm, 
	ISNULL(prd_cost, 0) as prd_cost, 
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'Unknown'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt , 
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt 
 
FROM silver.crm_prd_info

select * from silver.crm_prd_info
SELECT * from silver.erp_px_cat_g1v2
SELECT sls_prd_key from silver.crm_sales_details



select * from bronze.crm_sales_details
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id, 
	sls_ord_dt, 
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cust_id from silver.crm_cust_info)


select nullif(sls_due_dt, 0) as sls_due_dt from silver.crm_sales_details 
where 
	sls_due_dt <= 0 or
	len(sls_due_dt) != 8 or
	sls_due_dt > 20500101 or
	sls_due_dt < 19000101

SELECT sls_sales FROM  silver.crm_sales_details WHERE sls_sales <0 OR sls_sales IS NULL
SELECT sls_sales FROM  silver.crm_sales_details WHERE sls_sales != sls_quantity * sls_price

SELECT * FROM  silver.crm_sales_details WHERE sls_ord_dt > sls_DUE_dt



SELECT 
cid, 
CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
	ELSE cid
END AS cst_key,
CASE 
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'Unknown'
END AS gen
FROM bronze.erp_cust_az12

SELECT cst_key FROM bronze.crm_cust_info

select distinct bdate from  silver.erp_cust_az12 where 
bdate < '1924-01-01' or bdate > getdate();

select distinct GEN, CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'Unknown'
END AS gen from  silver.erp_cust_az12  



SELECT 
TRIM(REPLACE(cid, '-', '')) AS cst_key,
CASE
	WHEN cntry ='DE' THEN 'Germany'
	WHEN cntry IN ('US', 'USA') THEN 'United States'
	WHEN cntry = '' OR  cntry IS NULL THEN 'UNKNOWN'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

select trim(replace(cid, '-', '')), cntry from bronze.erp_loc_a101 
where trim(replace(cid, '-', '')) not in (select cst_key from silver.crm_cust_info)

SELECT DISTINCT cntry,CASE
	WHEN cntry ='DE' THEN 'Germany'
	WHEN cntry IN ('US', 'USA') THEN 'United States'
	WHEN cntry = '' OR  cntry IS NULL THEN 'UNKNOWN'
	ELSE TRIM(cntry)
END AS cntry
 from bronze.erp_loc_a101


 SELECT * FROM BRONZE.ERP_PX_CAT_G1V2
 SELECT * FROM SILVER.CRM_PRD_INFO

 SELECT cat FROM BRONZE.ERP_PX_CAT_G1V2 WHERE TRIM(cat) != cat

 SELECT DISTINCT(MAINTENANCE) FROM BRONZE.ERP_PX_CAT_G1V2

