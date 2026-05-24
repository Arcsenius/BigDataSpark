select 'report_product_sales' as table_name, count(*) as rows from report_product_sales
union all
select 'report_customer_sales', count(*) from report_customer_sales
union all
select 'report_time_sales', count(*) from report_time_sales
union all
select 'report_store_sales', count(*) from report_store_sales
union all
select 'report_supplier_sales', count(*) from report_supplier_sales
union all
select 'report_product_quality', count(*) from report_product_quality;

select * from report_product_sales where metric = 'top_10_products' order by rank limit 10;
select * from report_customer_sales where metric = 'top_10_customers' order by rank limit 10;
select * from report_time_sales order by sale_year, sale_month limit 12;
select * from report_store_sales where metric = 'top_5_stores' order by rank limit 5;
select * from report_supplier_sales where metric = 'top_5_suppliers' order by rank limit 5;
select * from report_product_quality where metric in ('highest_rating', 'lowest_rating', 'most_reviews') order by metric, rank limit 30;
