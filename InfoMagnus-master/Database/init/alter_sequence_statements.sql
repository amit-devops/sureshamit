BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.order_transfer_method IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.order_transfer_method_order_transfer_method_id_seq', COALESCE((SELECT MAX(order_transfer_method_id)+1 FROM dbo.order_transfer_method), 1), false);
COMMIT;

-----------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.oa_master_distributor IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.oa_master_distributor_oa_master_distributor_id_seq', COALESCE((SELECT MAX(oa_master_distributor_id)+1 FROM dbo.oa_master_distributor), 1), false);
COMMIT;

-----------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE archive.stg_dg_scans IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('archive.stg_dg_scans_archive_id_seq', COALESCE((SELECT MAX(archive_id)+1 FROM archive.stg_dg_scans), 1), false);
COMMIT;

------------------------------
BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE archive.stg_fd_scans IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('archive.stg_fd_scans_archive_id_seq', COALESCE((SELECT MAX(archive_id)+1 FROM archive.stg_fd_scans), 1), false);
COMMIT;

------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.stg_scan IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.stg_scan_id_seq', COALESCE((SELECT MAX(id)+1 FROM dbo.stg_scan), 1), false);
COMMIT;

-------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.categories IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.categories_category_id_seq', COALESCE((SELECT MAX(category_id)+1 FROM dbo.categories), 1), false);
COMMIT;

-------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.package IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.package_package_id_seq', COALESCE((SELECT MAX(package_id)+1 FROM dbo.package), 1), false);
COMMIT;

-------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.sub_category IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.sub_category_sub_category_id_seq', COALESCE((SELECT MAX(sub_category_id)+1 FROM dbo.sub_category), 1), false);
COMMIT;

--------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.oa_distributors IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.oa_distributors_oa_distributor_id_seq', COALESCE((SELECT MAX(oa_distributor_id)+1 FROM dbo.oa_distributors), 1), false);
COMMIT;

--------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.customer_distributor_dyad IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.cust_dist_dyad_cust_dist_dyad_id_seq', COALESCE((SELECT MAX(customer_distributor_dyad_id)+1 FROM dbo.customer_distributor_dyad), 1), false);
COMMIT;

---------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.oa_stores IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.oa_stores_oa_store_id_seq', COALESCE((SELECT MAX(oa_store_id)+1 FROM dbo.oa_stores), 1), false);
COMMIT;

----------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.customer_store_distributor_triad IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.cust_store_dis_triad_cust_store_dist_triad_id_seq', COALESCE((SELECT MAX(customer_store_distributor_triad_id)+1 FROM dbo.customer_store_distributor_triad), 1), false);
COMMIT;

-----------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.items IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.items_item_id_seq', COALESCE((SELECT MAX(item_id)+1 FROM dbo.items), 1), false);
COMMIT;

-----------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.distributor_items IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.distributor_items_distributor_item_id_seq', COALESCE((SELECT MAX(distributor_item_id)+1 FROM dbo.distributor_items), 1), false);
COMMIT;

------------------------------------
------------------------------------
------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.customer_store_item_triad IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.cust_store_item_triad_cust_store_item_triad_id_seq', COALESCE((SELECT MAX(customer_store_item_triad_id)+1 FROM dbo.customer_store_item_triad), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.shipments IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.shipments_shipment_id_seq', COALESCE((SELECT MAX(shipment_id)+1 FROM dbo.shipments), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.customer_store_item_distributor_dyad IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.cust_store_item_dist_dyad_cust_store_item_dist__dyad_id_seq', COALESCE((SELECT MAX(customer_store_item_distributor_dyad_id)+1 FROM dbo.customer_store_item_distributor_dyad), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.oa_scans IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.oa_scans_oa_scan_id_seq', COALESCE((SELECT MAX(oa_scan_id)+1 FROM dbo.oa_scans), 1), false);
COMMIT;

-------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.oa_scans_sales IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.oa_scans_sales_oa_scan_sales_id_seq', COALESCE((SELECT MAX(oa_scan_sales_id)+1 FROM dbo.oa_scans_sales), 1), false);
COMMIT;

---------------------------------------


BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.customer_distributor_category_triad IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.cust_dist_category_triad_cust_dist_category_triad_id_seq', COALESCE((SELECT MAX(customer_distributor_category_triad_id)+1 FROM dbo.customer_distributor_category_triad), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.base_order IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.base_order_base_order_id_seq', COALESCE((SELECT MAX(base_order_id)+1 FROM dbo.base_order), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.conversion_residual IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.conversion_residual_conversion_residual_id_seq', COALESCE((SELECT MAX(conversion_residual_id)+1 FROM dbo.conversion_residual), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.credit_thresholds IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.credit_thresholds_credit_threshold_id_seq', COALESCE((SELECT MAX(credit_threshold_id)+1 FROM dbo.credit_thresholds), 1), false);
COMMIT;

-------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.customer_store_distributor_attributes IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.cust_store_dis_attrib_cust_store_dist_attrib_id_seq', COALESCE((SELECT MAX(customer_store_distributor_attribute_id)+1 FROM dbo.customer_store_distributor_attributes), 1), false);
COMMIT;

---------------------------------------


BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.delivery_schedules IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.delivery_schedules_delivery_schedule_id_seq', COALESCE((SELECT MAX(delivery_schedule_id)+1 FROM dbo.delivery_schedules), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.customer_store_distributor_schedule IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.cust_store_dist_sched_cust_store_dist_sched_id_seq', COALESCE((SELECT MAX(customer_store_distributor_schedule_id)+1 FROM dbo.customer_store_distributor_schedule), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.lead_times IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.lead_times_lead_time_id_seq', COALESCE((SELECT MAX(lead_time_id)+1 FROM dbo.lead_times), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.lead_time_exceptions IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.lead_time_exceptions_lead_time_exceptions_id_seq', COALESCE((SELECT MAX(lead_time_exceptions_id)+1 FROM dbo.lead_time_exceptions), 1), false);
COMMIT;

-------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.operator_adjustments_reasons IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.operator_adjustments_reasons_operator_adjustments_reason_id_seq', COALESCE((SELECT MAX(operator_adjustments_reason_id)+1 FROM dbo.operator_adjustments_reasons), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.operator_adjustments IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.operator_adjustments_adjustment_id_seq', COALESCE((SELECT MAX(adjustment_id)+1 FROM dbo.operator_adjustments), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.orders IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.orders_order_id_seq', COALESCE((SELECT MAX(order_id)+1 FROM dbo.orders), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.order_status_type IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.order_status_type_order_status_type_id_seq', COALESCE((SELECT MAX(order_status_type_id)+1 FROM dbo.order_status_type), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.order_status IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.order_status_order_status_id_seq', COALESCE((SELECT MAX(order_status_id)+1 FROM dbo.order_status), 1), false);
COMMIT;

-------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.order_transfer_method IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.order_transfer_method_order_transfer_method_id_seq', COALESCE((SELECT MAX(order_transfer_method_id)+1 FROM dbo.order_transfer_method), 1), false);
COMMIT;

---------------------------------------


BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.override_adjustments IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.override_adjustments_adjustment_id_seq', COALESCE((SELECT MAX(adjustment_id)+1 FROM dbo.override_adjustments), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.store_lead_time_exceptions IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.store_lead_time_exceptions_store_lead_time_exception_id_seq', COALESCE((SELECT MAX(store_lead_time_exception_id)+1 FROM dbo.store_lead_time_exceptions), 1), false);
COMMIT;

------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.oa_sku_upc_conversion IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.oa_sku_upc_conversion_oa_sku_upc_conversion_id_seq', COALESCE((SELECT MAX(oa_sku_upc_conversion_id)+1 FROM dbo.oa_sku_upc_conversion), 1), false);
COMMIT;

-------------------------------------
-------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.conversion_factors IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.conversion_factors_conversion_factor_id_seq', COALESCE((SELECT MAX(conversion_factor_id)+1 FROM dbo.conversion_factors), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.spoils_adjustments IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.spoils_adjustments_spoils_adjustment_id_seq', COALESCE((SELECT MAX(spoils_adjustment_id)+1 FROM dbo.spoils_adjustments), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.order_forecasts IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.order_forecasts_order_forecast_id_seq', COALESCE((SELECT MAX(order_forecast_id)+1 FROM dbo.order_forecasts), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.credit_perc IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.credit_perc_credit_perc_id_seq', COALESCE((SELECT MAX(credit_perc_id)+1 FROM dbo.credit_perc), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.weight_data IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.weight_data_weight_data_id_seq', COALESCE((SELECT MAX(weight_data_id)+1 FROM dbo.weight_data), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.true_up_adjustments IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.true_up_adjustments_true_up_adjustment_id_seq', COALESCE((SELECT MAX(true_up_adjustment_id)+1 FROM dbo.true_up_adjustments), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.avg_scans_customer_store_item_wk_day IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.avg_scn_cust_store_item_avg_scn_cust_store_item_wk_day_id_seq', COALESCE((SELECT MAX(avg_scans_customer_store_item_wk_day_id)+1 FROM dbo.avg_scans_customer_store_item_wk_day), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.applied_override_adjustments IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.applied_override_adjustments_override_id_seq', COALESCE((SELECT MAX(override_id)+1 FROM dbo.applied_override_adjustments), 1), false);
COMMIT;

---------------------------------------

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE dbo.applied_operator_adjustments IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('dbo.applied_operator_adjustments_adjustment_id_seq', COALESCE((SELECT MAX(adjustment_id)+1 FROM dbo.applied_operator_adjustments), 1), false);
COMMIT;

---------------------------------------