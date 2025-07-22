/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
SELECT 
	ci.cust_id AS customer_id, 
	ci.cst_key AS customer_number, 
	ci.cst_firstname AS first_name, 
	ci.cst_lastname AS last_name, 
	la.cntry AS country,
	ci.cst_material_status AS material_status, 
	CASE 
		WHEN ci.cst_gender != 'Unknown' THEN ci.cst_gender
		ELSE ISNULL(ca.gen, 'Unknown')
	END AS gender,
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date
	
	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cst_key
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cst_key

