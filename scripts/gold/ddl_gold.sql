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

DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW
    gold.dim_customers AS
    ---- We have to collect all the customer information from the tables present in the silver layer
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            cst_id
    ) "customer_key",
    cci.cst_id "customer_id",
    cci.cst_key "customer_number",
    cci.cst_firstname "first_name",
    cci.cst_lastname "last_name",
    eloc.cntry "country",
    cci.cst_marital_status "marital_status",
    ---- Integrating two different sources into ONE with the correct information
    CASE
    ---- If there is data in the cst_gndr then use it otherwise opt for eci.gen
        WHEN cci.cst_gndr != 'N/A' THEN cci.cst_gndr
        ---- Make sure that if there's NULL in eci.gen then handle it properly
        ELSE COALESCE(eci.gen, 'N/A')
    END "gender",
    eci.bdate "birthdate",
    cci.cst_create_date "create_date"
FROM
    ---- Getting the customer id, full name, marital status and gender from the crm_cust_info table
    silver.crm_cust_info cci
    ---- Getting the birth date from erp_cust_az10 table
    LEFT JOIN silver.erp_cust_az12 eci ON cci.cst_key = eci.cid
    ---- Getting the location of the customer from erp_loc_a101 table
    LEFT JOIN silver.erp_loc_a101 eloc ON cci.cst_key = eloc.cid;

/* 
---- After Joining table, check if any duplicates were introduced by the join logic
SELECT cst_id, COUNT(*)
FROM (
SELECT
cci.cst_id,
cci.cst_key,
cci.cst_firstname,
cci.cst_lastname,
cci.cst_marital_status,
cci.cst_gndr,
cci.cst_create_date,
eci.bdate,
-- eci.gen, ---- Customer gender is already provided in the crm_cust_info table
eloc.cntry
FROM
---- Getting the customer id, full name, marital status and gender from the crm_cust_info table
silver.crm_cust_info cci
---- Getting the birth date from erp_cust_az10 table
LEFT JOIN silver.erp_cust_az12 eci ON cci.cst_key = eci.cid
---- Getting the location of the customer from erp_loc_a101 table
LEFT JOIN silver.erp_loc_a101 eloc ON cci.cst_key = eloc.cid
)
GROUP BY 1 HAVING COUNT(*) > 1;
 */
/* 
If some columns are getting repeated, then make sure they are giving same results.
Here, cci.cst_gndr and eci.gen both represents gender column.
Validate if they are giving the same result, if not correct it and then integrate the data.


SELECT
DISTINCT
cci.cst_gndr,
eci.gen,
CASE 
WHEN cci.cst_gndr != 'N/A' THEN cci.cst_gndr
ELSE COALESCE(eci.gen, 'N/A')
END "GENDER"
FROM
silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eci ON cci.cst_key = eci.cid
 */
DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW
    gold.dim_products AS
    ---- We want the current information of the product and not the historical information
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            prd_key,
            prd_start_dt
    ) "product_key",
    cpi.prd_id "product_id",
    cpi.prd_key "product_number",
    cpi.prd_nm "product_name",
    cpi.cat_id "category_id",
    epi.cat "category",
    epi.subcat "sub-category",
    epi.maintenance "maintainence",
    cpi.prd_cost "cost",
    cpi.prd_line "product_line",
    cpi.prd_start_dt "start_date"
    -- cpi.prd_end_dt "end_date",
    -- epi.id "id",
FROM
    silver.crm_prd_info cpi
    LEFT JOIN silver.erp_px_cat_g1v2 epi ON cpi.cat_id = epi.id
WHERE
    cpi.prd_end_dt IS NULL;

DROP VIEW IF EXISTS gold.fact_sales;

---- Filters out all historical data
CREATE VIEW
    gold.fact_sales AS
SELECT
    csd.sls_ord_num "order_number",
    pr.product_key,
    cu.customer_id,
    csd.sls_order_dt "order_date",
    csd.sls_ship_dt "shipping_date",
    csd.sls_due_dt "dur_date",
    csd.sls_sales "sales_amount",
    csd.sls_quantity "sales_quantity",
    csd.sls_price "price"
FROM
    silver.crm_sales_details csd
    LEFT JOIN gold.dim_products pr ON csd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu ON csd.sls_cust_id = cu.customer_id;