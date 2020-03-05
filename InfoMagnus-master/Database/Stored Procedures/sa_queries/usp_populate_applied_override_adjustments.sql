CREATE OR REPLACE FUNCTION public.usp_populate_applied_override_adjustments
(
	 p_work_order_id integer
	,p_run_date	date default (now() :: date)
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
BEGIN 
	
	insert into dbo.applied_override_adjustments
	(
		 customer_store_item_distributor_dyad_id
		,override_quantity
		,referenced_create_date
		,create_date
	)
	select
		 oa.customer_store_item_distributor_dyad_id
		,oa.override_quantity
		,z.max_dt
		,now() :: timestamp without time zone
	from
	(
		select
			oa.customer_store_item_distributor_dyad_id
			,max(cast(oa.create_date as date)) as max_dt
		from
			dbo.override_adjustments oa
			inner join dbo.base_order bo
				on oa.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			inner join dbo.customer_store_item_distributor_dyad c
				on c.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			inner join dbo.work_order_items woi
				on woi.work_group_id = c.work_group_id
				and woi.work_order_id = p_work_order_id
		where
			oa.is_used = false
		group by
			oa.customer_store_item_distributor_dyad_id
	) z
	inner join dbo.override_adjustments oa
		on z.customer_store_item_distributor_dyad_id = oa.customer_store_item_distributor_dyad_id
		and z.max_dt = cast(oa.create_date as date);
		
	update dbo.override_adjustments
	set
		 is_used = true
		,used_date = p_run_date
	from
		dbo.override_adjustments oa
		inner join dbo.applied_override_adjustments aoa 
			on oa.customer_store_item_distributor_dyad_id = aoa.customer_store_item_distributor_dyad_id
		inner join dbo.customer_store_item_distributor_dyad c 
			on c.customer_store_item_distributor_dyad_id = oa.customer_store_item_distributor_dyad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = c.work_group_id
			and woi.work_order_id = p_work_order_id
	where
		oa.create_date <= aoa.referenced_create_date
		and cast(aoa.create_date as date) = p_run_date
		and oa.is_used = false;

END
$BODY$;
