/*
=====================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================
Script Purpose:
        This procedure loads data into the Bronze schema from external csv files
        It truncates, then loads the tables with BULK INSERT
=====================================================
*/

create or alter procedure bronze.load_bronze as
begin
    declare @start_time datetime, @end_time datetime;
    begin try
        print '======================';
        print 'Loading Bronze Layer';
        print '======================';
        print '----------------------';
        print 'Loading CRM Tables';
        print '----------------------';

        set @start_time = getdate();
        print 'truncating crm_cust_info';
        truncate table bronze.crm_cust_info;
        print 'inserting crm_cust_info';
        bulk insert bronze.crm_cust_info
        from 'C:\Users\Michael\OneDrive\Documents\data engineer\cust_info.csv'
        with ( firstrow = 2,
               fieldterminator = ',',
               tablock
        );
        set @end_time = getdate();
        print ' Load duration: ' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
        print '-------------------------';
        set @start_time = getdate();
        print 'truncating crm_prd_info';
        truncate table bronze.crm_prd_info;
        print 'inserting crm_prd_info';
        bulk insert bronze.crm_prd_info
        from 'C:\Users\Michael\OneDrive\Documents\data engineer\prd_info.csv'
        with ( firstrow = 2,
               fieldterminator = ',',
               tablock
        );

        set @end_time = getdate();
        print ' Load duration: ' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
        print '------------------------';
        
        set @start_time = getdate();
        print 'truncating crm_sales_details';
        truncate table bronze.crm_sales_details;
        print 'inserting crm_sales_details';
        bulk insert bronze.crm_sales_details
        from 'C:\Users\Michael\OneDrive\Documents\data engineer\sales_details.csv'
        with ( firstrow = 2,
               fieldterminator = ',',
               tablock
        );
        set @end_time = getdate();
        print ' Load duration: ' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';

        print '----------------------';
        print 'Loading ERP Tables'
        print '----------------------';

        set @start_time = getdate();
        print 'truncating erp_cust_az12';
        truncate table bronze.erp_cust_az12;
        print 'inserting erp_cust_az12';
        bulk insert bronze.erp_cust_az12
        from 'C:\Users\Michael\OneDrive\Documents\data engineer\CUST_AZ12.csv'
        with ( firstrow = 2,
               fieldterminator = ',',
               tablock
        );
        set @end_time = getdate();
        print ' Load duration: ' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
        print '----------------------';

        set @start_time = getdate();
        print 'truncating erp_loc_a101';
        truncate table bronze.erp_loc_a101;
        print 'inserting erp_loc_a101';
        bulk insert bronze.erp_loc_a101
        from 'C:\Users\Michael\OneDrive\Documents\data engineer\LOC_A101.csv'
        with ( firstrow = 2,
               fieldterminator = ',',
               tablock
        );
        set @end_time = getdate();
        print ' Load duration: ' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
        print '----------------------';

        set @start_time = getdate();
        print 'truncating erp_px_cat_g1v2';
        truncate table bronze.erp_px_cat_g1v2;
        print 'inserting erp_px_cat_g1v2';
        bulk insert bronze.erp_px_cat_g1v2
        from 'C:\Users\Michael\OneDrive\Documents\data engineer\PX_CAT_G1V2.csv'
        with ( firstrow = 2,
               fieldterminator = ',',
               tablock
        );
        set @end_time = getdate();
        print ' Load duration: ' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
        print '----------------------';
    end try
    begin catch
    print '============================';
    print ' Error occured during loading bronze layer ';
    print 'Error message' + error_message();
    print 'Error message' + cast(error_number() as nvarchar);
    print '============================';
    end catch
end
