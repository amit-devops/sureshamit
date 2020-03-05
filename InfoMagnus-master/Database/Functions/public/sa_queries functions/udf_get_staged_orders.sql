CREATE OR REPLACE FUNCTION public.udf_get_staged_orders
(
	 p_work_order_id integer
	,p_run_date	timestamp without time zone default (now() :: timestamp without time zone)
)
    RETURNS TABLE
	(
		 base_order_id integer
		,oa_master_distributor_id integer
		,customer_distributor_dyad_id integer
		,distributor_item_id integer
		,customer_store_distributor_triad_id integer
		,customer_store_item_distributor_dyad_id integer
		,customer_store_item_triad_id integer
		,customer_distributor_category_triad_id integer
		,category_id integer
		,retailer_store_number integer
		,retailer_item_description varchar(255)
		,scans_prior_10_days integer
		,last_delivery_date date
		,recommended_delivery_date date
		,conversion_factor_used varchar(200)
		,conversion_residual integer
		,actual_scans integer
		,forecasted_scans integer
		,base_order integer
		,model_used varchar(255)
		,weight_data integer
		,mass_adjustment_quantity double precision
		,true_up_adjustment_quantity integer
		,adjustment_quantity integer
		,override_quantity integer
		,proposed_order_quantity double precision
		,max_shipment integer
	)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
	ROWS 1000
AS $BODY$
declare rec record;
BEGIN 
	
	drop table if exists staged_orders;
	create temp table staged_orders
	(
		 base_order_id integer
		,oa_master_distributor_id integer
		,customer_distributor_dyad_id integer
		,distributor_item_id integer
		,customer_store_distributor_triad_id integer
		,customer_store_item_distributor_dyad_id integer
		,customer_store_item_triad_id integer
		,customer_distributor_category_triad_id integer
		,category_id integer
		,retailer_store_number integer
		,retailer_item_description varchar(255)
		,scans_prior_10_days integer
		,last_delivery_date date
		,recommended_delivery_date date
		,conversion_factor_used varchar(200)
		,conversion_residual integer
		,actual_scans integer
		,forecasted_scans integer
		,base_order integer
		,model_used varchar(255)
		,weight_data integer
		,mass_adjustment_quantity double precision
		,true_up_adjustment_quantity integer
		,adjustment_quantity integer
		,override_quantity integer
		,proposed_order_quantity double precision
		,max_shipment integer
	);
	
	insert into staged_orders
	(
		 base_order_id
		,oa_master_distributor_id
		,customer_distributor_dyad_id
		,distributor_item_id
		,customer_store_distributor_triad_id
		,customer_store_item_distributor_dyad_id
		,customer_store_item_triad_id
		,customer_distributor_category_triad_id
		,category_id
		,retailer_store_number
		,retailer_item_description
		,scans_prior_10_days
		,last_delivery_date
		,recommended_delivery_date
		,conversion_factor_used
		,conversion_residual
		,actual_scans
		,forecasted_scans
		,base_order
		,model_used
		,weight_data
		,mass_adjustment_quantity
		,true_up_adjustment_quantity
		,adjustment_quantity
		,override_quantity
		,proposed_order_quantity
		,max_shipment
	)
	select
		 bo.base_order_id
		,bo.oa_master_distributor_id
		,bo.customer_distributor_dyad_id
		,bo.distributor_item_id
		,bo.customer_store_distributor_triad_id
		,bo.customer_store_item_distributor_dyad_id
		,bo.customer_store_item_triad_id
		,bo.customer_distributor_category_triad_id
		,bo.category_id
		,os.store_number as retailer_store_number
		,i.retailer_item_description
		,coalesce(scan.qty, 0) as scans_prior_10_days
		,public.udf_last_scheduled_delivery_date(css.delivery_schedule_id, (bo.rec_delivery_date - interval '-1 day')::date ) as last_delivery_date    
		,bo.rec_delivery_date
        ,coalesce(cf.conversion_factor, '') as conversion_factor_used
        ,sum(coalesce(cr.residual_quantity, 0)) as conversion_residual
        ,bo.actual_scans
        ,bo.forecasted_scans
        ,bo.base_order
        ,bo.model_used
        ,coalesce(wd.over_under, 0) as weight_data
        ,coalesce(sum(ac.adjustment_value), 0) as mass_adjustment_qty
        ,coalesce(ov.variance, 0)  as true_up_adj_qty
        ,coalesce(aoa.adjustment_quantity, 0) as operator_adj_qty
        ,coalesce(sum(aova.override_quantity) ,0) as override_qty
        ,case 
			when coalesce(sum(aova.override_quantity), 0) = 0 
				then 
					bo.base_order 
					+ coalesce(sum(ac.adjustment_value), 0) 
					+ coalesce(aoa.adjustment_quantity, 0) 
					+ coalesce(ov.variance, 0) 
					+ coalesce(wd.over_under, 0)
           else 
			coalesce(sum(aova.override_quantity) ,0)
         end as proposed_order_qty
		,coalesce(maxship.mxs, 0) as max_shipment
	from
		dbo.base_order bo
		inner join dbo.customer_store_distributor_triad csd 
			on csd.customer_store_distributor_triad_id = bo.customer_store_distributor_triad_id
		inner join dbo.customer_store_distributor_schedule css 
			on css.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id  
			and p_run_date <  css.expiry_date
		inner join dbo.oa_stores os 
			on os.oa_store_id = csd.oa_store_id
		inner join dbo.customer_store_item_triad csi 
			on csi.customer_store_item_triad_id = bo.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csi.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join dbo.items i 
			on i.item_id = csi.item_id 
			and i.expiry_date > p_run_date
		left outer join dbo.adjustments_calculations ac 
			on ac.base_order_id = bo.base_order_id
			and cast(ac.create_date as date) = cast(p_run_date as date)
		left outer join dbo.applied_operator_adjustments aoa 
			on bo.base_order_id = aoa.base_order_id
			and cast(aoa.create_date as date) = cast(p_run_date as date)
		left outer join dbo.weight_data w 
			on w.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			and cast(w.create_date as date) = cast(p_run_date as date)
		left outer join dbo.applied_override_adjustments aova 
			on aova.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			and cast(aova.create_date as date) = cast(p_run_date as date)
		left outer join
		(
			select 
				 sp.customer_store_item_triad_id
				,max(sp.quantity) as mxs
			from 
				dbo.shipments sp
				inner join dbo.work_order_items woi
					on woi.work_group_id = sp.work_group_id
					and woi.work_order_id = p_work_order_id
			group by 
				sp.customer_store_item_triad_id
		)as maxship 
			on maxship.customer_store_item_triad_id = bo.customer_store_item_triad_id
		left outer join 
		(
			select 
				 sc.customer_store_item_triad_id
				,sum(coalesce(sc.quantity, 0)) as qty
			from 
				dbo.oa_scans sc
				inner join dbo.work_order_items woi
					on sc.work_group_id = woi.work_group_id
					and woi.work_order_id = p_work_order_id
			where 
				sc.transaction_date between p_run_date - interval '10 day' and p_run_date
			group by 
				sc.customer_store_item_triad_id
		) scan 
			on scan.customer_store_item_triad_id = csi.customer_store_item_triad_id
		left outer join
		(
			select 
				 t.customer_store_item_triad_id
				,t.base_order_id
				,sum(t.variance) as variance
			from 
				dbo.true_up_adjustments t
				inner join dbo.customer_store_item_triad b
					on b.customer_store_item_triad_id = t.customer_store_item_triad_id
				inner join dbo.work_order_items woi
					on woi.work_group_id = b.work_group_id
					and woi.work_order_id = p_work_order_id
			where 
				t.base_order_id is not null
			group by 
				 t.customer_store_item_triad_id
				,t.base_order_id
		)ov 
			on ov.customer_store_item_triad_id = csi.customer_store_item_triad_id
			and ov.base_order_id = bo.base_order_id
		left outer join dbo.conversion_factors cf 
			on cf.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id 
			and p_run_date < cf.expiry_date
		left outer join dbo.conversion_residual cr 
			on cr.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id 
			and cr.applied_date is null
		left outer join dbo.weight_data wd 
			on wd.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id 
			and cast(wd.create_date as date) = cast(bo.create_date as date)
    group by 
		 bo.base_order_id
        ,bo.oa_master_distributor_id
        ,bo.customer_distributor_dyad_id
        ,bo.distributor_item_id
        ,bo.customer_store_distributor_triad_id
        ,bo.customer_store_item_distributor_dyad_id
        ,bo.customer_store_item_triad_id
        ,bo.customer_distributor_category_triad_id
        ,bo.category_id
        ,bo.rec_delivery_date
        ,bo.base_order
        ,os.store_number
		,i.retailer_item_description
		,bo.actual_scans
        ,bo.forecasted_scans
		,bo.model_used
        ,coalesce(ov.variance, 0)
        ,css.delivery_schedule_id
		,scan.qty
		,aoa.adjustment_quantity
        ,coalesce(cf.conversion_factor, '')
		,coalesce(wd.over_under, 0)
    	,maxship.mxs;  
	
	for rec in 
		select
			 so.base_order_id
			,so.oa_master_distributor_id
			,so.customer_distributor_dyad_id
			,so.distributor_item_id
			,so.customer_store_distributor_triad_id
			,so.customer_store_item_distributor_dyad_id
			,so.customer_store_item_triad_id
			,so.customer_distributor_category_triad_id
			,so.category_id
			,so.retailer_store_number
			,so.retailer_item_description
			,so.scans_prior_10_days
			,so.last_delivery_date
			,so.recommended_delivery_date
			,so.conversion_factor_used
			,so.conversion_residual
			,so.actual_scans
			,so.forecasted_scans
			,so.base_order
			,so.model_used
			,so.weight_data
			,so.mass_adjustment_quantity
			,so.true_up_adjustment_quantity
			,so.adjustment_quantity
			,so.override_quantity
			,so.proposed_order_quantity
			,so.max_shipment
		from
			staged_orders so
	loop 
		return query
			select
			 rec.base_order_id
			,rec.oa_master_distributor_id
			,rec.customer_distributor_dyad_id
			,rec.distributor_item_id
			,rec.customer_store_distributor_triad_id
			,rec.customer_store_item_distributor_dyad_id
			,rec.customer_store_item_triad_id
			,rec.customer_distributor_category_triad_id
			,rec.category_id
			,rec.retailer_store_number
			,rec.retailer_item_description
			,rec.scans_prior_10_days
			,rec.last_delivery_date
			,rec.recommended_delivery_date
			,rec.conversion_factor_used
			,rec.conversion_residual
			,rec.actual_scans
			,rec.forecasted_scans
			,rec.base_order
			,rec.model_used
			,rec.weight_data
			,rec.mass_adjustment_quantity
			,rec.true_up_adjustment_quantity
			,rec.adjustment_quantity
			,rec.override_quantity
			,rec.proposed_order_quantity
			,rec.max_shipment;
		end loop;
END
$BODY$;