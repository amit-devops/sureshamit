create or replace view dbo.vw_base_orders
as
	select 
		 vbo.base_order_id
		,vbo.oa_master_distributor_id
		,vbo.customer_distributor_dyad_id
		,vbo.distributor_item_id
		,vbo.customer_store_distributor_triad_id
		,vbo.customer_store_item_distributor_dyad_id
		,vbo.customer_store_item_triad_id
		,vbo.customer_distributor_category_triad_id
		,vcsds.delivery_schedule_id
		,vbo.category_id
		,vbo.create_date  ::  date as create_date
		,vbo.create_date as create_date_ts
		,vbo.rec_delivery_date
		,vbo.actual_scans
		,vbo.forecasted_scans
		,vbo.base_order
		,vbo.model_used
		,coalesce(vwd.over_under, 0) as weight_data
		,coalesce(sum(coalesce(vac.adjustment_value, 0  ::  double precision)), 0 :: double precision) as mass_adjustment_quantity
		,coalesce(vov.variance, 0  ::  bigint) as trueup_adjustment_quantity
		,coalesce(vaoa.adjustment_quantity, 0) as operator_adjustment_quantity
		,coalesce(vaoa.adjusted_by, '' :: character varying) as adjusted_by
		,coalesce(sum(coalesce(vaova.override_quantity, 0)), 0 :: bigint) as override_quantity
		,case
			when coalesce(sum(coalesce(vaova.override_quantity, 0)), 0  ::  bigint) = 0 
				then vbo.base_order :: double precision + coalesce(sum(coalesce(vac.adjustment_value, 0 :: double precision)), 0 :: double precision) + coalesce(vaoa.adjustment_quantity, 0) :: double precision + coalesce(vov.variance, 0 :: bigint) :: double precision + coalesce(vwd.over_under, 0) :: double precision
			else 
				coalesce(sum(coalesce(vaova.override_quantity, 0)), 0 :: bigint) :: double precision
		 end  ::  integer as proposed_order_quantity
		,case
			when coalesce(sum(coalesce(vaova.override_quantity, 0)), 0 :: bigint) = 0 then vbo.base_order :: double precision + coalesce(sum(coalesce(vac.adjustment_value, 0 :: double precision)), 0 :: double precision) + coalesce(vov.variance, 0 :: bigint) :: double precision + coalesce(vwd.over_under, 0) :: double precision
				else coalesce(sum(coalesce(vaova.override_quantity, 0)), 0 :: bigint) :: double precision
		 end :: integer as original_prop_order_quantity
		,case
			when cf.conversion_units = null  ::  integer 
				then 1
			else null  ::  integer
		 end as conversion_units
		,mx.max_delivered_quantity
		,vbo.work_group_id
   from 
	dbo.base_order vbo
    inner join dbo.customer_store_distributor_schedule vcsds 
		on vcsds.customer_store_distributor_triad_id = vbo.customer_store_distributor_triad_id 
		and vbo.category_id = vcsds.category_id 
		and vbo.create_date >= vcsds.effective_date 
		and vbo.create_date <= vcsds.expiry_date
    left join dbo.adjustments_calculations vac 
		on vac.base_order_id = vbo.base_order_id 
		and vac.create_date :: date = vbo.create_date :: date
    left join dbo.applied_operator_adjustments vaoa 
		on vbo.base_order_id = vaoa.base_order_id 
		and vaoa.create_date :: date = vbo.create_date :: date
    left join dbo.applied_override_adjustments vaova 
		on vaova.customer_store_item_distributor_dyad_id = vbo.customer_store_item_distributor_dyad_id 
		and vaova.create_date :: date = vbo.create_date :: date
	left join dbo.weight_data vwd 
		on vwd.customer_store_item_distributor_dyad_id = vbo.customer_store_item_distributor_dyad_id 
		and vwd.category_id = vbo.category_id 
		and vwd.create_date = vbo.create_date :: date
    left join 
	( 
		select 
			 t.customer_store_item_triad_id
            ,t.base_order_id
            ,sum(t.variance) as variance
		from 
			dbo.true_up_adjustments t
		where 
			t.base_order_id is not null
		group by 
			 t.customer_store_item_triad_id
			,t.base_order_id
	) vov 
		on vov.customer_store_item_triad_id = vbo.customer_store_item_triad_id 
		and vov.base_order_id = vbo.base_order_id
    left join dbo.conversion_factors cf 
		on cf.customer_store_item_distributor_dyad_id = vbo.customer_store_item_distributor_dyad_id 
		and vbo.create_date >= cf.effective_date 
		and vbo.create_date <= cf.expiry_date
    left join 
	( 
		select 
			 s.customer_store_item_triad_id
            ,max(s.quantity) as max_delivered_quantity
		from 
			dbo.shipments s
		group by 
			s.customer_store_item_triad_id
	) mx 
		on mx.customer_store_item_triad_id = vbo.customer_store_item_triad_id
  group by 
	 vbo.base_order_id
	,vbo.oa_master_distributor_id
	,vbo.customer_distributor_dyad_id
	,vbo.distributor_item_id
	,vbo.customer_store_distributor_triad_id
	,vbo.customer_store_item_distributor_dyad_id
	,vbo.customer_store_item_triad_id
	,vbo.customer_distributor_category_triad_id
	,vbo.category_id
	,vbo.rec_delivery_date
	,vbo.actual_scans
	,vbo.forecasted_scans
	,vbo.base_order
	,vbo.create_date
	,vbo.model_used
	,(coalesce(vaoa.adjustment_quantity, 0))
	,(coalesce(vaoa.adjusted_by, '' :: character varying))
	,(coalesce(vov.variance, 0 :: bigint))
	,(coalesce(vwd.over_under, 0))
	,vcsds.delivery_schedule_id
	,cf.conversion_units
	,mx.max_delivered_quantity;