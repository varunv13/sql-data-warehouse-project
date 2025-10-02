---- Working with silver.crm_cust_info
--- Removing the data from the table
TRUNCATE Table silver.crm_cust_info;
---- Inserting only the clean data with in the table
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
--- Only getting those cst_id whose rank is 1
SELECT
    cst_id,
    cst_key,
    --- Triming down the extra spaces from the first and last name
    TRIM(cst_firstname) "cst_firstname",
    TRIM(cst_lastname) "cst_lastname",
    ---- We want to maintain a proper naming convention and will not use any short abbreviations
    CASE
    ---- We use UPPER(), since we don't know if the cst_gndr will contain data in small or capital
    ---- We use TRIM(), incase the data contains extra spaces
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Bachelor'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END "cst_marital_status",
    CASE
    ---- We use UPPER(), since we don't know if the cst_gndr will contain data in small or capital
    ---- We use TRIM(), incase the data contains extra spaces
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END "cst_gndr",
    cst_create_date
FROM
    (
        SELECT
            *,
            --- Giving rank to the cst_id, such that the last create date of the cst_id is rank 1
            ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY
                    cst_create_date DESC
            ) "Flag_Last"
        FROM
            bronze.crm_cust_info
    ) t
WHERE
    "Flag_Last" = 1;



---- Working with silver.crm_prd_info
--- Removing the data from the table
TRUNCATE TABLE silver.crm_prd_info;
SELECT * FROM silver.crm_prd_info;

INSERT INTO
    silver.crm_prd_info (
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
    ---- We seggregated the prd_key in two halves
    ---- One half will give the category id which will help in joining the ERP table
    ---- Other will help in joining the CRM table
    UPPER(
        TRIM(REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_'))
    ) "cat_id",
    UPPER(TRIM(SUBSTRING(prd_key, 7, LENGTH (prd_key)))) "prd_key",
    prd_nm,
    ---- Replace NULL values with in prd_cost
    COALESCE(prd_cost, 0) "prd_cost",
    ---- Instead of writing Full When Case statements, we can opt for 'Quick Case'
    ---- Implementation of 'Quick Case'
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END prd_line,
    prd_start_dt,
    ---- We want the end date to be in correct order, such that no overlapping takes place
    ---- To access values from the next row within a window, we use lead
    ---- To access values from the prev row within a window, we use lag
    ---- We are dividing the data by product key, and not prd_id
    LEAD (prd_start_dt) OVER (
        PARTITION BY
            prd_key
        ORDER BY
            prd_start_dt ASC
    ) -1 "prd_lst_dt"
FROM
    bronze.crm_prd_info;

