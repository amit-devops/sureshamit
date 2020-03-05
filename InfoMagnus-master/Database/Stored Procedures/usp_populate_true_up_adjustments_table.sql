CREATE OR REPLACE FUNCTION public.usp_populate_true_up_adjustments
(
	 --p_national_customer_id integer default 24
	 p_work_order_id integer
	,p_use_date date default null::date
	,p_load_date date default null::date
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
begin

	if p_use_date is null 
	then
		p_use_date = 
		(
			select 
				max(r.max_date)
			from 
				dbo.retailer_last_scan_date r
				inner join dbo.customer_store_item_triad csi 
					on csi.customer_store_item_triad_id = r.customer_store_item_triad_id
				inner join dbo.work_order_items woi
					on woi.work_group_id = csi.work_group_id
					and woi.work_order_id = p_work_order_id
			-- where 
				-- csi.national_customer_id = p_national_customer_id
		);
	end if;
	
	if p_load_date is null 
	then
		p_load_date := now() :: date;
	end if;

	if exists
	(
		select 
			1 
		from 
			dbo.true_up_adjustments t
			inner join dbo.customer_store_item_triad csi 
				on csi.customer_store_item_triad_id = t.customer_store_item_triad_id
			inner join dbo.work_order_items woi
				on woi.work_group_id = csi.work_group_id
				and woi.work_order_id = p_work_order_id
		where 
			t.scan_date = p_use_date 
		--	and csi.national_customer_id = p_national_customer_id
	)
	then
		delete 
			from dbo.true_up_adjustments t
		using 
			 dbo.customer_store_item_triad csi
			,dbo.work_order_items woi
		where 
			csi.customer_store_item_triad_id = t.customer_store_item_triad_id
			and woi.work_group_id = csi.work_group_id
			and woi.work_order_id = p_work_order_id
			and t.scan_date = p_use_date;
			--and csi.national_customer_id = p_national_customer_id;
	end if;
	
	with cte_a
	as
	(
		select 
			 min(rec_delivery_date) as rec_delivery_date
			,customer_store_item_triad_id
		from 
			dbo.base_order 
		where 
			rec_delivery_date > p_use_date
		group by
			customer_store_item_triad_id
	)
	insert into dbo.true_up_adjustments
	(
		 customer_store_item_triad_id
		,scan_date
		,units_scanned
		,forecast_units
		,correction
		,variance
		,model_used
		,work_group_id
	)
	select distinct 
		 bo.customer_store_item_triad_id
		,coalesce(s.transaction_date, p_use_date) as scan_date
		,coalesce(s.quantity, 0) as scans
		,coalesce(a.units, coalesce(r.Units, 0)) as forecast_units
		,coalesce(tua.variance,0) as correction
		,coalesce(s.quantity, 0) - coalesce(a.units, coalesce(r.units, 0)) - coalesce(tua.variance,0) as variance
		,bo.model_used
		,csi.work_group_id
	from 
		dbo.base_order bo
		inner join dbo.customer_store_item_triad csi 
			on bo.customer_store_item_triad_id = csi.customer_store_item_triad_id 
			--and csi.national_customer_id = p_national_customer_id
			and bo.rec_delivery_date > p_use_date
		inner join dbo.work_order_items woi
			on woi.work_group_id = csi.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join cte_a c
			on c.customer_store_item_triad_id = bo.customer_store_item_triad_id
			and c.rec_delivery_date = bo.rec_delivery_date
		left outer join dbo.order_forecasts a 
			on a.forecast_date = p_use_date 
			and lower(a.forecast_source) = 'movingaverage' 
			and bo.customer_store_item_triad_id = a.customer_store_item_triad_id 
			and lower(bo.model_used) = lower(a.forecast_source)
		left outer join dbo.order_forecasts r 
			on r.forecast_date = p_use_date
			and lower(r.forecast_source) = 'python' 
			and bo.customer_store_item_triad_id = r.customer_store_item_triad_id 
			and lower(bo.model_used) = lower(r.forecast_source)
			and r.run_date = bo.run_date -- this will be added as part of the 
		left outer join dbo.oa_scans s 
			on s.customer_store_item_triad_id = bo.customer_store_item_triad_id 
			and s.run_date = p_load_date
		left outer join dbo.true_up_adjustments tua 
			on bo.customer_store_item_triad_id = tua.customer_store_item_triad_id 
			and tua.scan_date = s.transaction_date 
			and tua.units_scanned = 0;
		
end;
$BODY$;