
/*
	===============================================================================
	DDL Script: Create silver Tables
	===============================================================================
	Script Purpose:
		This script creates tables in the 'silver' schema, dropping existing tables 
		if they already exist.
		  Run this script to re-define the DDL structure of 'silver' Tables
	===============================================================================
*/
IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'crm_cust_info' AND schema_id = SCHEMA_ID('silver')
)
BEGIN
    CREATE TABLE silver.crm_cust_info (
        cust_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_material_status NVARCHAR(50),
        cst_gender NVARCHAR(50),
        cst_create_date DATE,
        dwh_create_date DATETIME2 DEFAULT GETDATE()

    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'crm_prd_info' AND schema_id = SCHEMA_ID('silver')
)
BEGIN
    CREATE TABLE silver.crm_prd_info (
        prd_id INT,
		cat_id NVARCHAR(50),
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost int,
        prd_line NVARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'crm_sales_details' AND schema_id = SCHEMA_ID('silver')
)
BEGIN
    CREATE TABLE silver.crm_sales_details (
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_ord_dt DATE,
        sls_ship_dt DATE,
        sls_due_dt DATE,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END


----
IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'erp_cust_az12' AND schema_id = SCHEMA_ID('silver')
)
BEGIN
    CREATE TABLE silver.erp_cust_az12 (
        cid NVARCHAR(50),
		cst_key NVARCHAR(50),
        bdate DATE,
        gen  NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'erp_cust_az12' AND schema_id = SCHEMA_ID('silver')
)
BEGIN
    CREATE TABLE silver.erp_cust_az12 (
        cid NVARCHAR(50),
		cst_key NVARCHAR(50),
        bdate DATE,
        gen  NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'erp_loc_a101' AND schema_id = SCHEMA_ID('silver')
)
BEGIN
    CREATE TABLE silver.erp_loc_a101 (
        cst_key NVARCHAR(50),
        cntry NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
        
    );
END


IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'erp_px_cat_g1v2' AND schema_id = SCHEMA_ID('silver')
)
BEGIN
    CREATE TABLE silver.erp_px_cat_g1v2 (
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
