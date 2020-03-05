CREATE OR REPLACE FUNCTION public.usp_create_base_order
(
	 p_oa_master_distributor_id integer
	,p_customer_distributor_dyad_id integer
	,p_distributor_item_id integer
	,p_customer_store_distributor_triad_id integer
	,p_customer_store_item_distributor_dyad_id integer
	,p_customer_store_item_triad_id integer
	,p_customer_distributor_category_triad_id integer
	,p_category_id integer
	,p_rec_delivery_date date
	,p_actual_scans integer
	,p_forecasted_scans integer
	,p_base_order integer
	,p_run_date date
	,p_create_date date
	,p_model_used varchar(255)
	,p_inc_in_anomaly boolean
	,p_inc_in_file boolean
	,p_inc_in_billing boolean
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
	
	declare v_work_group_id varchar(15);

begin
	
	select
		 woi.work_group_id
	into
		v_work_group_id
	from
		dbo.customer_store_item_triad woi
	where
		customer_store_item_triad_id = p_customer_store_item_triad_id;
	
    insert into dbo.base_order 
	(
		 oa_master_distributor_id
		,customer_distributor_dyad_id
		,distributor_item_id
		,customer_store_distributor_triad_id
		,customer_store_item_distributor_dyad_id
		,customer_store_item_triad_id
		,customer_distributor_category_triad_id
		,category_id
		,rec_delivery_date
		,actual_scans
		,forecasted_scans
		,base_order
		,run_date
		,create_date
		,model_used
		,inc_in_anomaly
		,inc_in_file
		,inc_in_billing
		,work_group_id
	) 
    select  
		 p_oa_master_distributor_id
		,p_customer_distributor_dyad_id
		,p_distributor_item_id
		,p_customer_store_distributor_triad_id
		,p_customer_store_item_distributor_dyad_id
		,p_customer_store_item_triad_id
		,p_customer_distributor_category_triad_id
		,p_category_id
		,p_rec_delivery_date
		,p_actual_scans
		,p_forecasted_scans
		,p_base_order
		,p_run_date
		,p_create_date
		,p_model_used
		,p_inc_in_anomaly
		,p_inc_in_file
		,p_inc_in_billing
		,v_work_group_id;         
end;
$BODY$;

