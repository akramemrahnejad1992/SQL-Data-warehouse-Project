
/*
	===============================================================================
	DDL Script: Create Bronze Tables
	===============================================================================
	Script Purpose:
		This script creates tables in the 'bronze' schema, dropping existing tables 
		if they already exist.
		  Run this script to re-define the DDL structure of 'bronze' Tables
	===============================================================================
*/
IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'crm_cust_info' AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    CREATE TABLE bronze.crm_cust_info (
        cust_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_material_status NVARCHAR(50),
        cst_gender NVARCHAR(50),
        cst_create_date DATE
    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'crm_prd_info' AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    CREATE TABLE bronze.crm_prd_info (
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost int,
        prd_line NVARCHAR(50),
        prd_start_dt DATETIME,
        prd_end_dt DATETIME
    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'crm_sales_details' AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_ord_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT
    );
END


----
IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'erp_cust_az12' AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    CREATE TABLE bronze.erp_cust_az12 (
        cid NVARCHAR(50),
        bdate DATE,
        gen  NVARCHAR(50)
    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'erm_loc_a101' AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    CREATE TABLE bronze.erm_loc_a101 (
        cid NVARCHAR(50),
        cntry NVARCHAR(50)
        
    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'erm_px_cat_g1v2' AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    CREATE TABLE bronze.erm_px_cat_g1v2 (
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(50)
    );
END
