/*
===================================================================
Stored ProcedureL Load Broze Layer (Source -> Bronze)
===================================================================
Script Purpose:
    This stored procedures loads the data into the 'broze' schema
    from external CSV files.
    It performs the following functions:
    - Truncates the bronze tables before loading the data
    - Uses the 'COPY' command to load data from CSV files to the
    bronze tables.
===================================================================
*/

CALL bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    load_count INTEGER;
    base_path CONSTANT TEXT := 'D:/PROJECT/DataEng/sql-data-warehouse-project/datasets/';
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;


BEGIN

    batch_start_time := clock_timestamp();

    start_time := clock_timestamp();

    -------------------------------------------------------
    -- Load crm_cust_info 
    -------------------------------------------------------
    BEGIN 
        TRUNCATE TABLE bronze.crm_cust_info;

        EXECUTE format(
            $f$
            COPY bronze.crm_cust_info (
                cst_id, cst_key, cst_firstname, cst_lastname,
                cst_marital_status, cst_gndr, cst_create_date
            )
            FROM '%s'
            DELIMITER ','
            CSV HEADER;
            $f$,
            base_path || 'bronze.crm_cust_info.csv'
        );
        
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
    
    -------------------------------------------------------
    -- Load crm_prd_info
    -------------------------------------------------------
    start_time := clock_timestamp();

    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;

        EXECUTE format(
            $f$
            COPY bronze.crm_prd_info (
                prd_id, prd_key, prd_nm, prd_cost,
                prd_line, prd_start_dt, prd_end_dt
            )
            FROM '%s'
            DELIMITER ','
            CSV HEADER;
            $f$,
            base_path || 'bronze.crm_prd_info.csv'
        );

        -- Get end time and log duration for this block
        end_time := clock_timestamp();
        RAISE NOTICE 'crm_prd_info loaded: % rows. Duration: % seconds', 
            load_count, 
            EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE 'Error in crm_prd_info load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;
    END;

    -------------------------------------------------------
    -- Load crm_sales_details
    -------------------------------------------------------
    start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details;

        EXECUTE format(
            $f$
            COPY bronze.crm_sales_details (
                sls_ord_num, sls_prd_key, sls_cust_id,
                sls_order_dt, sls_ship_dt, sls_due_dt,
                sls_sales, sls_quantity, sls_price
            )
            FROM '%s'
            DELIMITER ','
            CSV HEADER;
            $f$,
            base_path || 'bronze.crm_sales_details.csv'
        );

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

    -------------------------------------------------------
    -- Load erp_cust_az12
    -------------------------------------------------------
    start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;

        EXECUTE format(
            $f$
            COPY bronze.erp_cust_az12 (cid, bdate, gen)
            FROM '%s'
            DELIMITER ','
            CSV HEADER;
            $f$,
            base_path || 'bronze.erp_cust_az12.csv'
        );

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

    -------------------------------------------------------
    -- Load erp_loc_a101
    -------------------------------------------------------
    start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;

        EXECUTE format(
            $f$
            COPY bronze.erp_loc_a101 (cid, cntry)
            FROM '%s'
            DELIMITER ','
            CSV HEADER;
            $f$,
            base_path || 'bronze.erp_loc_a101.csv'
        );

        GET DIAGNOSTICS load_count = ROW_COUNT;
        end_time := clock_timestamp();
        RAISE NOTICE 'erp_loc_a101 loaded: % rows. Duration: % seconds', 
            load_count, 
            EXTRACT(EPOCH FROM (end_time - start_time));
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE 'Error in erp_loc_a101 load: SQLSTATE % - SQLERRM %', SQLSTATE, SQLERRM;
    END;

    -------------------------------------------------------
    -- Load erp_px_cat_g1v2
    -------------------------------------------------------
    start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        EXECUTE format(
            $f$
            COPY bronze.erp_px_cat_g1v2
            FROM '%s'
            DELIMITER ','
            CSV HEADER;
            $f$,
            base_path || 'bronze.erp_px_cat_g1v2.csv'
        );

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
    RAISE NOTICE 'Bronze Layer Loading Completed, Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
END;
$$;