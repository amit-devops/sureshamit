CREATE OR REPLACE FUNCTION public.usp_refresh_retailer_last_scan_dt
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

	if p_run_date is null then
		p_run_date := now() :: date;
	end if;
	
	with cte_wgids
	as
	(
		select
			work_group_id
		from
			dbo.work_order_items woi
		where
			woi.work_order_id = p_work_order_id
	)
	delete from dbo.retailer_last_scan_date where work_group_id in (select * from cte_wgids);
	
	with cte_max_date
	as
	(
		select 
			 cc.national_customer_id
			,max(s.transaction_date) as max_date 
		from 
			dbo.oa_scans s 
			inner join dbo.customer_store_item_triad cc 
				on cc.customer_store_item_triad_id  = s.customer_store_item_triad_id
		group by 
			cc.national_customer_id
	)
	insert into dbo.retailer_last_scan_date
	(
		 customer_store_item_triad_id
		,max_date
		,refresh_date
		,work_group_id
	)
	select 
		 csi.customer_store_item_triad_id
		,ld.max_date
		,p_run_date as refresh_date
		,csi.work_group_id as work_group_id
	from 
		dbo.customer_store_item_triad csi
		inner join dbo.customer_store_item_distributor_dyad csid 
			on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csi.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join dbo.customer_distributor_dyad cd 
			on cd.national_customer_id = csi.national_customer_id 
			and cd.oa_distributor_id = csid.oa_distributor_id
		inner join dbo.customer_store_distributor_triad csd 
			on csd.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
			and csd.oa_store_id = csi.oa_store_id
		inner join cte_max_date ld 	
			on ld.national_customer_id = csi.national_customer_id;

end;
$BODY$;
