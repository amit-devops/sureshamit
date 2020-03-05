CREATE OR REPLACE FUNCTION public.usp_populate_applied_operator_adjustments
(
	 p_work_order_id integer
	,p_run_date date default (now() :: date)
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
begin

	insert into dbo.applied_operator_adjustments
	(
		 base_order_id
		,adjustment_quantity
		,create_date
		,adjusted_by
	)
	select
		 bo.base_order_id
		,oa.adjustment_quantity
		,now() as create_date
		,oa.create_user
	from
		dbo.base_order bo
		inner join dbo.operator_adjustments oa
			on oa.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			and oa.rec_delivery_date = bo.rec_delivery_date
			and cast(oa.create_date as date) = p_run_date
		left join dbo.applied_operator_adjustments aoa
			on aoa.base_order_id = bo.base_order_id
	where
		aoa.base_order_id is null;
		
	
	update dbo.applied_operator_adjustments 
	set 
		 adjustment_quantity = oa.adjustment_quantity
		,create_date = now()
		,adjusted_by = oa.create_user
	from
		dbo.base_order bo
		inner join dbo.operator_adjustments oa
			on oa.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			and oa.rec_delivery_date = bo.rec_delivery_date
			and cast(oa.create_date as date) = p_run_date
		inner join dbo.applied_operator_adjustments aoa
			on aoa.base_order_id = bo.base_order_id
	where
		oa.adjustment_quantity <> aoa.adjustment_quantity;
		
	if exists 
	(
		select 
			1 
		from 
			dbo.operator_adjustments oa
			inner join dbo.operator_adjustments_reasons oar
				on oa.operator_adjustments_reason_id = oar.operator_adjustments_reason_id
		where
			cast(oa.create_date as date) = p_run_date
			and lower(oar.operator_adjustments_reason_description) = 'casepack adjustment'
	) 
	then
		update dbo.conversion_residual as cr
		set 
			applied_date = now() :: date
		from
			dbo.conversion_residual cr
			inner join dbo.base_order bo
				on cr.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			inner join dbo.operator_adjustments oa 
				on oa.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
				and cast(oa.create_date as date) = p_run_date
			inner join dbo.operator_adjustments_reasons oar
				on oar.operator_adjustments_reason_id = oa.operator_adjustments_reason_id
		where
			cr.applied_date is null
			and bo.create_date = p_run_date
			and lower(oar.operator_adjustments_reason_description) = 'casepack adjustment';
	end if;
		
end;
$BODY$;