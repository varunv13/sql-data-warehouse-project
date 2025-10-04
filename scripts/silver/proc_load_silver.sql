CALL silver.load_silver();
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    load_count INTEGER;

    BEGIN
        batch_start_time := clock_timestamp();
        start_time := clock_timestamp();

        BEGIN
            ---- We will create a procedure so that the process of loading the data within all the tables
            ---- occurs in a single execution call

            /*Working with silver.crm_cust_info */ 
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
            GET DIAGNOSTICS load_count = ROW_COUNT;
            
            -- Get end time and log duration for this block
            end_time := clock_timestamp();
            RAISE NOTICE 'crm_cust_info loaded: % rows. Duration: % seconds', 
                load_count, 
                EXTRACT(EPOCH FROM (end_time - start_time));
            
            EXCEPTION 
                WHEN OTHERS THEN
                    RAISE NOTICE 'Error in crm_cust_info load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;

        END; 
        ---------------------------------------------------
        ---------------------------------------------------
        start_time := clock_timestamp();
        /* Working with silver.crm_prd_info */
        BEGIN
            --- Removing the data from the table
            TRUNCATE TABLE silver.crm_prd_info;

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
            end_time := clock_timestamp();
            RAISE NOTICE 'crm_prd_info loaded: % rows. Duration: % seconds', 
                load_count, 
                EXTRACT(EPOCH FROM (end_time - start_time));
            EXCEPTION 
                WHEN OTHERS THEN
                    RAISE NOTICE 'Error in crm_prd_info load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;
        END;
        ---------------------------------------------------
        ---------------------------------------------------
        start_time := clock_timestamp();
        BEGIN
            /* Working with silver.crm_sales_details
            

            ---- Validating the data quality is an important aspect while performing TRANSFORMATION
            ---- Especially with the Dates.
            ---- Things one should keep in mind while working with dates are:
            ---- 1. Dates should be valid,i.e., Dates should never be less than or 0
            ---- 2. Dates should be in YYYYMMDD or DDMMYYYY format, if not convert it
            ---- 3. If a date is given in Integer format (eg: 20130801), the length of such integers should match with YYYYMMDD format

            Validation of Date data with in the table
            SELECT
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                sls_order_dt,
                sls_ship_dt,
                sls_due_dt,
                sls_sales,
                sls_quantity,
                sls_price
            FROM
                bronze.crm_sales_details
            WHERE
                sls_order_dt <= 0
                OR LENGTH (CAST(sls_order_dt AS TEXT)) <> 8
                OR sls_order_dt > 20500101
                OR sls_order_dt < 19000101;

            Validating for the other fields if they're valid.
            A field to be valid must be:
            1. Not a null
            2. >= 0
            3. Calculation is correct for every the fields (Basically correct data)

            SELECT * 
            FROM 
                bronze.crm_sales_details
            WHERE 
                sls_sales <= 0 
                OR sls_quantity <= 0 
                OR sls_price <= 0 
                OR sls_sales IS NULL
                OR sls_quantity IS NULL
                OR sls_price IS NULL
                OR sls_sales <> sls_price * ABS(sls_price);
            */
            ---- Clear the data if present in the 
            TRUNCATE TABLE silver.crm_sales_details;
            ---- Loading the clean data into the silver.crm_sales_details table
            INSERT INTO silver.crm_sales_details (
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
            ---- Transforming (Cleaning) the Data as per requirements and improvements
            SELECT
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                ---- If the data for the Data is bad, then make it NULL else convert it into Date format (if it's not)
                CASE 
                    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt :: text) <> 8 THEN NULL
                    ---- We cannot directly convert an Integer to Date, we first convert it to Varchar or Text then to Date
                    ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
                END "sls_order_dt",
                ---- Applying the same rules, to make sure data remains consistent
                CASE 
                    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt :: text) <> 8 THEN NULL
                    ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
                END "sls_ship_dt",
                CASE 
                    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt :: text) <> 8 THEN NULL
                    ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
                END "sls_due_dt",
                ---- Ensure Data consistency throughout the fields
                ---- Sales = Price * Quantity
                CASE 
                    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price) THEN  sls_price * sls_quantity
                    ELSE sls_sales
                END "sls_sales",
                sls_quantity,
                ---- Price = Sales / Quantity
                CASE 
                    ---- We want to ensure that while dividing we don't want any 0 as denominator
                    ---- If we do have 0, then make it NULL
                    WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
                    ELSE sls_price
                END "sls_price"
            FROM
                bronze.crm_sales_details;

            /* Final Check
            SELECT * 
            FROM 
                silver.crm_sales_details
            WHERE 
                sls_sales != sls_quantity * ABS(sls_price)
                OR sls_sales < 0 
                OR sls_quantity <= 0 
                OR sls_price < 0 
                OR sls_sales IS NULL
                OR sls_quantity IS NULL
                OR sls_price IS NULL;
            */
            GET DIAGNOSTICS load_count = ROW_COUNT;
            -- Get end time and log duration for this block
            end_time := clock_timestamp();
            RAISE NOTICE 'crm_sales_details loaded: % rows. Duration: % seconds', 
                load_count, 
                EXTRACT(EPOCH FROM (end_time - start_time));
            EXCEPTION 
                WHEN OTHERS THEN
                    RAISE NOTICE 'Error in crm_sales_details load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;
        END;
        ---------------------------------------------------
        ---------------------------------------------------
        start_time := clock_timestamp();
        BEGIN

            /*
            Working with silver.erp_cust_az12
            ---- Validate if the user is valid or not.
            ---- The user is not valid if it's birthdate is in Future
            SELECT
                *
            FROM
                bronze.erp_cust_az12
            WHERE
                ---- If the user is like 100+ years old or it's birthday further in future
                ---- NOW() :- Current date and time
                bdate < '1925-01-01' OR bdate > NOW();

            ---- Validate the gender field to ensure consistency between the data is there
            SELECT
                DISTINCT gen
            FROM
                bronze.erp_cust_az12;

            */
            
        ---- Removing the data if present from the table
            TRUNCATE TABLE silver.erp_cust_az12;
            ---- Inserting the clean data into silver.erp_cust_az12
            INSERT INTO silver.erp_cust_az12 (
                cid,
                bdate,
                gen
            )
            SELECT
                ---- Making changes in the cid as per the requirement
                CASE
                    WHEN cid ILIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH (cid))
                    ELSE cid
                END "cid",
                CASE 
                    ---- We will only consider users that we are certain do not exist.
                    ---- If bdate is in future, then they are fake.
                    WHEN bdate > NOW() THEN NULL
                    ELSE bdate
                END "bdate",
                ---- Ensuring data remains consistent and correct through out the table
                CASE
                    WHEN gen ILIKE 'M%' THEN 'Male'
                    WHEN gen ILIKE 'F%' THEN 'Female'
                    ELSE 'N/A'
                END "gen"
            FROM
                bronze.erp_cust_az12;

            GET DIAGNOSTICS load_count = ROW_COUNT;
            -- Get end time and log duration for this block
            end_time := clock_timestamp();
            RAISE NOTICE 'erp_cust_az12 loaded: % rows. Duration: % seconds', 
                load_count, 
                EXTRACT(EPOCH FROM (end_time - start_time));
        EXCEPTION 
            WHEN OTHERS THEN
                RAISE NOTICE 'Error in erp_cust_az12 load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;
        END;

        ---------------------------------------------------
        ---------------------------------------------------
        start_time := clock_timestamp();
        BEGIN
            /* Working with silver.erp_loc_a101 */
            ---- Removing the data if present from the table
            TRUNCATE TABLE silver.erp_loc_a101;
            ---- Inserting the clean data into the silver.erp_loc_a101
            INSERT INTO silver.erp_loc_a101 (
                cid,
                cntry
            )
            SELECT
                ---- Making changes as per the requirements
                REPLACE (cid, '-', '') "cid",
                ---- Ensuring Data consistency
                CASE
                    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM(cntry) IN ('US', 'USA', 'United States') THEN 'United States Of America'
                    WHEN TRIM(cntry) IS NULL
                    OR TRIM(cntry) = '' THEN 'N/A'
                    ELSE cntry
                END "cntry"
            FROM
                bronze.erp_loc_a101;

            GET DIAGNOSTICS load_count = ROW_COUNT;
            end_time := clock_timestamp();
            RAISE NOTICE 'erp_loc_a101 loaded: % rows. Duration: % seconds', 
                load_count, 
                EXTRACT(EPOCH FROM (end_time - start_time));
            EXCEPTION 
                WHEN OTHERS THEN
                    RAISE NOTICE 'Error in erp_loc_a101 load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;
        END;

        -------------------------------------------------
        -------------------------------------------------
        start_time := clock_timestamp();
        BEGIN

            /* Working with silver.erp_px_cat_g1v2 table */

            ---- If data is already present in the table, then remove it
            TRUNCATE TABLE silver.erp_px_cat_g1v2;

            ---- Data is clean, but make sure to validate before Inserting anything from bronze table to silver table
            ---- Inserting the data into the silver.erp_px_cat_g1v2
            INSERT INTO
                silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
            SELECT
                id,
                cat,
                subcat,
                maintenance
            FROM
                bronze.erp_px_cat_g1v2;

            GET DIAGNOSTICS load_count = ROW_COUNT;
            end_time := clock_timestamp();
            RAISE NOTICE 'erp_px_cat_g1v2 loaded: % rows. Duration: % seconds', 
                load_count, 
                EXTRACT(EPOCH FROM (end_time - start_time));
            EXCEPTION 
                WHEN OTHERS THEN
                    RAISE NOTICE 'Error in erp_px_cat_g1v2 load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;
        END;
        batch_end_time := clock_timestamp();
        RAISE NOTICE 'Silver Layer Loading Completed, Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    END;
$$;