/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time      TIMESTAMP;
    end_time        TIMESTAMP;
    batch_start     TIMESTAMP;
    batch_end       TIMESTAMP;
BEGIN
    batch_start := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -------------------------------------------------------------------------
    -- CRM TABLES
    -------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -------------------------------------------------------------------------
    -- crm_cust_info
    -------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE bronze.crm_cust_info;

    RAISE NOTICE '>> Loading: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'C:/Users/pheni/OneDrive/Desktop/data modeling course/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

    -------------------------------------------------------------------------
    -- crm_prd_info
    -------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE bronze.crm_prd_info;

    RAISE NOTICE '>> Loading: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'C:/Users/pheni/OneDrive/Desktop/data modeling course/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

    -------------------------------------------------------------------------
    -- crm_sales_details
    -------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE bronze.crm_sales_details;

    RAISE NOTICE '>> Loading: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'C:/Users/pheni/OneDrive/Desktop/data modeling course/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

    -------------------------------------------------------------------------
    -- ERP TABLES
    -------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -------------------------------------------------------------------------
    -- erp_loc_a101
    -------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE bronze.erp_loc_a101;

    RAISE NOTICE '>> Loading: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'C:/Users/pheni/OneDrive/Desktop/data modeling course/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

    -------------------------------------------------------------------------
    -- erp_cust_az12
    -------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE bronze.erp_cust_az12;

    RAISE NOTICE '>> Loading: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'C:/Users/pheni/OneDrive/Desktop/data modeling course/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

    -------------------------------------------------------------------------
    -- erp_px_cat_g1v2
    -------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Loading: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:/Users/pheni/OneDrive/Desktop/data modeling course/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

    -------------------------------------------------------------------------
    -- COMPLETED
    -------------------------------------------------------------------------
    batch_end := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end - batch_start));
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========================================';
END;
$$;
