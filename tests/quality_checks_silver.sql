/*
=============================================
Quality checks
=============================================
*/


-- ===========================================
-- checking silver.crm_cust_info
-- ===========================================

-- check for nulls or duplicates in primary key
select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id = null;


-- check for unwanted spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr);


-- data standardization and consistancy 
select distinct cst_material_status
from bronze.crm_cust_info;



-- ===========================================
-- checking silver.crm_prd_info
-- ===========================================

-- check for nulls or duplicates in primary key 

select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is Null;


-- check for unwanted spaces

select prd_nm 
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);

-- check for nulls and negative numbers 
select prd_cost
from bronze.crm_prd_info
where prd_cost is null or prd_cost < 0;


-- Data standardization and consistency
select distinct prd_line
from bronze.crm_prd_info;

-- Check for invalid date orders
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

select 
prd_id,
prd_key,
prd_start_dt,
prd_end_dt,
dateadd(day, -1,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt_test 
-- lead() lets us access next record. We are making sure end dates are before the start date of the next product.
from bronze.crm_prd_info 
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');




-- ===========================================
-- checking silver.crm_sales_details
-- ===========================================


-- check for invalid dates
select nullif(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where len(sls_due_dt) != 8 or sls_due_dt > 20270101 or sls_due_dt < 19000101 or sls_due_dt <= 0;

-- check for invalid order dates
select *
from bronze.crm_sales_details
where sls_order_dt > sls_due_dt;


-- check for sales:
-- Sales = Quantity * Price
-- Sales values cannot be 0, null, or negative
select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,

case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
    then sls_quantity * abs(sls_price)
    else sls_sales
end as sls_sales,

case when sls_price is null or sls_price <=0 
    then sls_sales / nullif(sls_quantity, 0)
    else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null
or sls_quantity is null
or sls_price is null
or sls_sales <=0
or sls_quantity <=0
or sls_price <=0;


-- ===========================================
-- checking silver.erp_cust_az12
-- ===========================================

-- getting cid ready for joins with customer info
select
case when cid like 'NAS%'
    then substring(cid, 4, len(cid)) 
    else cid
end as cid
from bronze.erp_cust_az12;


-- checking for out of range dates 

select distinct 
bdate 
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

-- cheking for consistency in data

select distinct gen,
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
     when upper(trim(gen)) in ('M', 'MALE') then 'Male'
     when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
     else 'no information'
end as gen
from bronze.erp_cust_az12;



-- ===========================================
-- checking silver.erp_loc_a101
-- ===========================================



-- data standardization and consistency 
select distinct cntry from bronze.erp_loc_a101;

select 
replace(cid,'-', '') as cid,
case when trim(cntry) = 'DE' then 'Genrmany'
     when trim(cntry) in ('US', 'USA') then 'United States'
     when trim(cntry) = '' or cntry is null then 'no info'
     else trim(cntry)
end as cntry
from bronze.erp_loc_a101;

-- ===========================================
-- checking silver.erp_px_cat_g1v2
-- ===========================================



-- check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or MAINTENANCE != trim(MAINTENANCE);

-- check for data standardization and consistency
select distinct maintenance
from bronze.erp_px_cat_g1v2;




