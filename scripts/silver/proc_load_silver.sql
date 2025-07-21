EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================='
		PRINT 'TRANSFORMING DATA START'
		PRINT '-----------------------'

		PRINT '======================='
		PRINT 'TRANSFORMING CRM TABLES...'
		PRINT '======================='

		PRINT '>>  TRUNCATING DATA INTO: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info

		SET @start_time = GETDATE();
		PRINT '>>  INSERTING DATA INTO: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cust_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gender, cst_create_date
		)
		SELECT 
		cust_id, 
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			ELSE 'Unknown'
		END AS cst_material_status,
		CASE
			WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
			ELSE 'Unknown'
		END AS cst_gender,
		cst_create_date
		FROM (
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cust_id,cst_key ORDER BY cst_create_date desc) AS FLAG_LAST
			FROM bronze.crm_cust_info WHERE cust_id IS NOT NULL
		)
		AS CLEANED_CST_INFO
		WHERE FLAG_LAST = 1;
		SET @end_time = GETDATE();
		PRINT '>> TRANSFORM DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>  TRUNCATING DATA INTO: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info

		PRINT '>>  INSERTING DATA INTO: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

		)
		SELECT 
			prd_id, 
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
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> TRANSFORM DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_ord_dt,
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
			CASE 
				WHEN sls_ord_dt = 0 OR LEN(sls_ord_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ord_dt AS VARCHAR) AS DATE)
			END AS sls_ord_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> TRANSFORM DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------------------------------'
		
		
		PRINT '======================='
		PRINT 'TRANSFORMING ERP TABLES...'
		PRINT '======================='


		SET @start_time = GETDATE();
		PRINT '>>  TRUNCATING DATA INTO: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12

		PRINT '>>  INSERTING DATA INTO: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
			cid, cst_key, bdate, gen
		)

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
		SET @end_time = GETDATE();
		PRINT '>> TRANSFORM DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>>  TRUNCATING DATA INTO: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101

		PRINT '>>  INSERTING DATA INTO: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(
			cst_key, cntry
		)

		SELECT 
		TRIM(REPLACE(cid, '-', '')) AS cst_key,
		CASE
			WHEN cntry ='DE' THEN 'Germany'
			WHEN cntry IN ('US', 'USA') THEN 'United States'
			WHEN cntry = '' OR  cntry IS NULL THEN 'UNKNOWN'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> TRANSFORM DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>>  TRUNCATING DATA INTO: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2

		PRINT '>>  INSERTING DATA INTO: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id, cat, subcat, maintenance
		)

		SELECT * 
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> TRANSFORM DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------------------------------'
	
	SET @batch_end_time = GETDATE();
    PRINT '================================'
    PRINT 'LOADING BRONZE LAYER IS DONE';
    PRINT 'TOTAL LOAD DURATION: ' + CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' miliseconds';
    PRINT '================================'
	END	TRY

	BEGIN CATCH
	PRINT '============================'
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT 'Error Message' + ERROR_MESSAGE();
    PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '============================'
	END CATCH
END

