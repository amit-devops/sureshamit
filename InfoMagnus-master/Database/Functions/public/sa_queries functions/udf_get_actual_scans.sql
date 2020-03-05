CREATE OR REPLACE FUNCTION public.udf_get_actual_scans
(
	 p_work_order_id integer
	,p_category_id integer
	,p_run_date	timestamp without time zone default (now() :: timestamp without time zone)
)
    RETURNS TABLE
	(
		 customer_store_distributor_triad_id integer
		,category_id integer
		,customer_store_item_triad_id integer
		,actual_quantity integer
		,max_scan_date date
		,customer_store_item_distributor_dyad_id integer
		,customer_distributor_dyad_id integer
		,last_delivery date
		,customer_distributor_category_triad_id integer
		,distributor_item_id integer
		,over_under integer
		,"variance" integer
	) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
		rec RECORD;
BEGIN 
	
	drop table if exists actual_scans;
	
	create temp table actual_scans
	(
		 customer_store_distributor_triad_id integer
		,category_id integer
		,customer_store_item_triad_id integer
		,actual_quantity integer
		,max_scan_date date
		,customer_store_item_distributor_dyad_id integer
		,customer_distributor_dyad_id integer
		,last_delivery date
		,customer_distributor_category_triad_id integer
		,distributor_item_id integer
		,over_under integer
		,"variance" integer
	);
	
	insert into actual_scans
	(
		 customer_store_distributor_triad_id
		,category_id
		,customer_store_item_triad_id
		,actual_quantity
		,max_scan_date
		,customer_store_item_distributor_dyad_id
		,customer_distributor_dyad_id
		,last_delivery
		,customer_distributor_category_triad_id
		,distributor_item_id
		,over_under
		,"variance"
	)
	select 
		 csd.customer_store_distributor_triad_id
		,i.category_id
		,csi.customer_store_item_triad_id
		,sum(coalesce(s.quantity,0)) as actual_quantity
		,rls.max_date
		,csid.customer_store_item_distributor_dyad_id
		,csd.customer_distributor_dyad_id
		,ad.last_delivery
		,cdc.customer_distributor_category_triad_id
		,di.distributor_item_id
		,coalesce(wd.over_under, 0) as over_under
		,coalesce(t."variance", 0) as "variance"
	from 
		dbo.customer_store_item_triad csi
		inner join dbo.work_order_items woi
			on woi.work_group_id = csi.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join dbo.items i 
			on i.item_id = csi.item_id
		inner join dbo.customer_store_item_distributor_dyad csid 
			on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id 
			and (p_run_date between csid.effective_date and csid.expiry_date)
		inner join dbo.customer_distributor_dyad cd 
			on cd.national_customer_id = csi.national_customer_id 
			and cd.oa_distributor_id = csid.oa_distributor_id
		inner join dbo.customer_store_distributor_triad csd 
			on csd.customer_distributor_dyad_id = cd.customer_distributor_dyad_id
			and csd.oa_store_id = csi.oa_store_id
		inner join 
		(
			select 
				 ld.customer_store_distributor_triad_id
				,ld.category_id
				,max(ld.last_delivery)::date as last_delivery
			from 
				dbo.last_deliveries ld
				inner join dbo.work_order_items woi
					on woi.customer_store_distributor_triad_id = ld.customer_store_distributor_triad_id
					and woi.category_id = ld.category_id
					and woi.work_order_id = p_work_order_id
			group by 
				 ld.customer_store_distributor_triad_id
				,ld.category_id
		) ad
			on ad.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id 
			and ad.category_id = i.category_id
		inner join dbo.retailer_last_scan_date as rls 
			on rls.customer_store_item_triad_id = csi.customer_store_item_triad_id
		inner join dbo.customer_distributor_category_triad cdc 
			on cdc.oa_distributor_id = csid.oa_distributor_id 
			and cdc.national_customer_id = csi.national_customer_id 
			and cdc.category_id = i.category_id
		inner join dbo.distributor_items di 
			on di.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
			and di.item_id = csi.item_id
		left outer join dbo.weight_data wd 
			on csid.customer_store_item_distributor_dyad_id = wd.customer_store_item_distributor_dyad_id 
			and cast(p_run_date as date) = cast(wd.create_date as date)
		left outer join
		(
			select 
				 tua.customer_store_item_triad_id
				,sum(coalesce(tua."variance",0)) as "variance"
			from 
				dbo.true_up_adjustments tua
			where 
				tua.base_order_id is null
			group by 
				tua.customer_store_item_triad_id
		)t 
			on t.customer_store_item_triad_id = csi.customer_store_item_triad_id
		left outer join dbo.oa_scans s 
			on s.customer_store_item_triad_id = csi.customer_store_item_triad_id 
			and s.transaction_date between ad.last_delivery and rls.max_date
	where 
		i.category_id = p_category_id
		and i.expiry_date > p_run_date
		and p_run_date between csi.effective_date and csi.expiry_date
	group by 
		 csd.customer_store_distributor_triad_id
		,i.category_id
		,csi.customer_store_item_triad_id
		,rls.max_date
		,csid.customer_store_item_distributor_dyad_id
		,csd.customer_distributor_dyad_id
		,ad.last_delivery
		,cdc.customer_distributor_category_triad_id
		,di.distributor_item_id
		,wd.over_under
		,t."variance"; 
		 
		
	for rec in 
		select 
			 acs.customer_store_distributor_triad_id
			,acs.category_id
			,acs.customer_store_item_triad_id
			,acs.actual_quantity
			,acs.max_scan_date
			,acs.customer_store_item_distributor_dyad_id
			,acs.customer_distributor_dyad_id
			,acs.last_delivery
			,acs.customer_distributor_category_triad_id
			,acs.distributor_item_id
			,acs.over_under
			,acs."variance"
		from
			actual_scans acs
	loop
		return query
			select
				 rec.customer_store_distributor_triad_id
				,rec.category_id
				,rec.customer_store_item_triad_id
				,rec.actual_quantity
				,rec.max_scan_date
				,rec.customer_store_item_distributor_dyad_id
				,rec.customer_distributor_dyad_id
				,rec.last_delivery
				,rec.customer_distributor_category_triad_id
				,rec.distributor_item_id
				,rec.over_under
				,rec."variance";
	end loop;

END
$BODY$;