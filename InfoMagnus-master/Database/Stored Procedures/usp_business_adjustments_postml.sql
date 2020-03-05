CREATE OR REPLACE FUNCTION public.usp_business_adjustments_postml
(
	p_order_genaration_date date
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

declare v_variability int = 2;

begin
	-- set nocount on added to prevent extra result sets from
	-- interfering with select statements.

	if p_order_genaration_date is null
		then
			 select max(create_date) from dbo.vw_base_orders b;
		end if;
		
	-- too big adj
	drop table if exists temp_table;
    
	create temp table temp_table as
	select 
		 bo.base_order_id
		,bo.customer_store_item_distributor_dyad_id
		,bo.rec_delivery_date
		,bo.create_date
		,bo.adjusted_by
		,bo.max_delivered_quantity
		,bo.proposed_order_quantity
		,bo.conversion_units
		,case 
			when bo.proposed_order_quantity = bo.conversion_units 
				then 0
			when bo.max_delivered_quantity <= v_variability 
				then (bo.proposed_order_quantity - public.udf_excel_max(bo.conversion_units, v_variability)) * -1
			when bo.proposed_order_quantity > (bo.max_delivered_quantity + v_variability) and bo.max_delivered_quantity > 0
				then round(((bo.proposed_order_quantity - bo.max_delivered_quantity) / bo.conversion_units), 0) * -1 * bo.conversion_units -- note no cast to float. this is by design for int division.
			else 
				0
		 end as too_big_adjustment
		,case 
			when bo.proposed_order_quantity < 0 
				then abs(bo.proposed_order_quantity)
			else 
				bo.proposed_order_quantity
		 end as mlcontrol
	from 
		dbo.vw_base_orders bo
	where 
		bo.create_date = p_order_genaration_date
		and bo.override_quantity = 0;

	update dbo.operator_adjustments
	set 
		 adjustment_quantity = adjustment_quantity + t.toobigadj
		,create_user = 'bl - toobig2'
	from 
		dob.operator_adjustments oa
		inner join temp_table t 
			on oa.customer_store_item_distributor_dyad_id = t.customer_store_item_distributor_dyad_id
			and oa.rec_delivery_date = t.rec_delivery_date
			and cast(oa.create_date as date) = t.create_date
	where 
		t.adjusted_by = 'autoadjustml'
		and t.too_big_adjustment < 0
		and (t.max_delivered_quantity - t.proposed_order_quantity) < 0
		and t.proposed_order_quantity > 4;

	-- ml update negative orders to zero
	
	update dbo.operator_adjustments
	set 
		 adjustment_quantity = adjustment_quantity + t.mlcontrol
		,createuser = 'bl - mlzero'
	from 
		dbo.operator_adjustments oa
		inner join temp_table t 
			on oa.customer_store_item_distributor_dyad_id = t.customer_store_item_distributor_dyad_id
			and oa.rec_delivery_date = t.rec_delivery_date
			and cast(oa.create_date as date) = t.create_date
	where 
		t.adjusted_by = 'autoadjustml'
		and t.proposed_order_quantity < 0; 

end;
$BODY$;
