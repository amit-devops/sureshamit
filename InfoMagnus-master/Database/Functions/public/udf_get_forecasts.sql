CREATE OR REPLACE FUNCTION public.usp_get_forecasts
(
	 p_work_order_id integer
	,p_rec_delivery_date date
	,p_run_date date default (now() :: date)
)
    RETURNS TABLE
	(
		 customer_store_distributor_triad_id integer
		,customer_store_item_triad_id integer
		,rec_delivery_date date
		,r_forecast integer
		,moving_average integer
	)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

declare rec record;
begin
	
	if p_run_date is null then
		p_run_date := now() :: date;
	end if;
	
	drop table if exists forecast_moving_average;
	create temp table forecast_moving_average
	(
		 customer_store_distributor_triad_id integer
		,customer_store_item_triad_id integer
		,rec_delivery_date date
		,r_forecast integer
		,moving_average integer
	);
	
	with ORFMA as 
	(
		select 
			 csd.customer_store_distributor_triad_id
			,csi.customer_store_item_triad_id
			,orf.forecast_source
			,orf.units
			,orf.forecast_date
			,orf.create_date
			,rls.max_date
			,public.udf_last_scheduled_delivery_date(css.delivery_schedule_id, (p_rec_delivery_date - 1 * interval '1 day') :: date) dt
		from 
			dbo.customer_store_item_triad csi
			inner join dbo.items i 
				on i.item_id = csi.item_id
				and i.expiry_date > p_run_date
			inner join dbo.customer_store_item_distributor_dyad csid 
				on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id
			inner join dbo.customer_distributor_dyad cd 
				on cd.national_customer_id = csi.national_customer_id 
				and cd.oa_distributor_id = csid.oa_distributor_id
			inner join dbo.customer_store_distributor_triad csd 
				on csd.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
				and csd.oa_store_id = csi.oa_store_id
			inner join dbo.work_order_items swo 
				on csd.customer_store_distributor_triad_id = swo.customer_store_distributor_triad_id
				and i.category_id = swo.category_id
				and swo.rec_delivery_date = p_rec_delivery_date
				and swo.work_order_id = p_work_order_id
			inner join dbo.customer_store_distributor_schedule css 
				on css.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id
				and (p_run_date between css.effective_date and css.expiry_date)
				and i.category_id = css.category_id
			inner join dbo.retailer_last_scan_date rls 
				on rls.customer_store_item_triad_id = csi.customer_store_item_triad_id
			inner join dbo.order_forecasts orf 
				on csi.customer_store_item_triad_id = orf.customer_store_item_triad_id
				and (cast(orf.create_date as date) between p_run_date - 7 * interval '1 day' and p_run_date)
		where 
			lower(orf.forecast_source) = 'movingaverage'
	),
	ORFR as 
	(
		select 
			 csd.customer_store_distributor_triad_id
			,csi.customer_store_item_triad_id
			,orf.forecast_source
			,orf.units
			,orf.forecast_date
			,orf.create_date
			,rls.max_date
			,public.udf_last_scheduled_delivery_date(css.delivery_schedule_id, (p_rec_delivery_date - 1 * interval '1 day') :: date) dt
		from 
			dbo.customer_store_item_triad csi
			inner join dbo.items i 
				on i.item_id = csi.item_id
				and i.expiry_date > p_run_date
			inner join dbo.customer_store_item_distributor_dyad csid 
				on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id
			inner join dbo.customer_distributor_dyad cd 
				on cd.national_customer_id = csi.national_customer_id 
				and cd.oa_distributor_id = csid.oa_distributor_id
			inner join dbo.customer_store_distributor_triad csd 
				on csd.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
				and csd.oa_store_id = csi.oa_store_id
			inner join dbo.work_order_items swo 
				on csd.customer_store_distributor_triad_id = swo.customer_store_distributor_triad_id 
				and swo.category_id = i.category_id
				and swo.rec_delivery_date = p_rec_delivery_date
				and swo.work_order_id = p_work_order_id
			inner join dbo.customer_store_distributor_schedule css 
				on css.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id
				and (p_run_date between css.effective_date and css.expiry_date)
				and i.category_id = css.category_id
			inner join dbo.retailer_last_scan_date rls 
				on rls.customer_store_item_triad_id = csi.customer_store_item_triad_id
			inner join dbo.order_forecasts orf 
				on csi.customer_store_item_triad_id = orf.customer_store_item_triad_id
				and cast(orf.create_date as date) = cast(p_run_date as date)
			where 
				lower(orf.forecast_source) = 'python'
	)
	insert into forecast_moving_average 
	(
		 customer_store_distributor_triad_id
		,customer_store_item_triad_id
		,rec_delivery_date
		,r_forecast
		,moving_average
	)
	select 
		 coalesce(ma.customer_store_distributor_triad_id, r.customer_store_distributor_triad_id) as customer_store_distributor_triad_id
		,coalesce(ma.customer_store_item_triad_id, r.customer_store_item_triad_id) as customer_store_item_triad_id
		,cast(coalesce(ma.rec_delivery_date, r.rec_delivery_date) as date) as rec_delivery_date
		,r.r_forecast
		,ma.moving_average
	from 
	(
			select 
				 a.customer_store_distributor_triad_id
				,a.customer_store_item_triad_id
				,p_rec_delivery_date as rec_delivery_date
				,sum(a.units) as moving_average
			from 
				ORFMA a
			where 
				forecast_date 
				between
					case 
						when max_date >= dt 
							then interval '1 day' + max_date
						else dt
					end
					and - 1 * interval '1 day' + p_rec_delivery_date
			group by
				 a.customer_store_distributor_triad_id
				,a.customer_store_item_triad_id
	) ma
	full outer join 
	(
		select 
			 b.customer_store_distributor_triad_id
			,b.customer_store_item_triad_id
			,p_rec_delivery_date as rec_delivery_date
			,sum(b.units) as r_forecast
		from 
			ORFR b
		where 
			forecast_date 
			between
				case 
					when max_date >= dt 
						then interval '1 day' + max_date
					else dt
				end
				and - 1 * interval '1 day' + p_rec_delivery_date
		group by 
			 b.customer_store_distributor_triad_id
			,b.customer_store_item_triad_id
	) r
		on r.customer_store_distributor_triad_id = ma.customer_store_distributor_triad_id 
		and r.customer_store_item_triad_id = ma.customer_store_item_triad_id;
		
	for rec in 
		select 
			 st.customer_store_distributor_triad_id
			,st.customer_store_item_triad_id
			,st.rec_delivery_date
			,st.r_forecast
			,st.moving_average 
		from 
			forecast_moving_average st
	loop
		return query 
			select 
				 rec.customer_store_distributor_triad_id
				,rec.customer_store_item_triad_id
				,rec.rec_delivery_date
				,rec.r_forecast
				,rec.moving_average;
	end loop;

end;
$BODY$;