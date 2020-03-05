CREATE OR REPLACE FUNCTION public.udf_get_last_ship_dates
(
	 p_work_order_id integer
	,p_rec_delivery_date date
	,p_run_date	timestamp without time zone default (now() :: timestamp without time zone)
)
    RETURNS TABLE
	(
		 customer_store_distributor_triad_id integer
		,last_ship_date date
		,last_scheduled_delivery_date date
		,category_id integer
		,inc_in_anomaly boolean
		,inc_in_file boolean
		,inc_in_billing boolean
	) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
		rec RECORD;
BEGIN 
	
	drop table if exists last_ship_dates;
	
	create temp table last_ship_dates
	(
		 customer_store_distributor_triad_id integer
		,last_ship_date date
		,last_scheduled_delivery_date date
		,category_id integer
		,inc_in_anomaly boolean
		,inc_in_file boolean
		,inc_in_billing boolean
	);
	
	insert into last_ship_dates
	(
		 customer_store_distributor_triad_id
		,last_ship_date
		,last_scheduled_delivery_date
		,category_id
		,inc_in_anomaly
		,inc_in_file
		,inc_in_billing
	)
	select 
		 csd.customer_store_distributor_triad_id
		,coalesce(h.last_ship_date, public.udf_last_scheduled_delivery_date(css.delivery_schedule_id, (p_rec_delivery_date - interval '1 day')::date)) as last_ship_date
		,public.udf_last_scheduled_delivery_date(css.delivery_schedule_id, (p_rec_delivery_date - interval '1 day')::date) as last_scheduled_delivery_date
		,css.category_id
		,css.inc_in_anomaly
		,css.inc_in_file
		,css.inc_in_billing
    from 
		dbo.customer_store_distributor_triad csd
		inner join dbo.customer_store_distributor_schedule css 
			on css.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id
			and css.expiry_date > p_run_date
		inner join dbo.delivery_schedules ds 
			on ds.delivery_schedule_id = css.delivery_schedule_id
		inner join dbo.customer_distributor_dyad cd 
			on cd.customer_distributor_dyad_id = csd.customer_distributor_dyad_id
		inner join dbo.work_order_items woi
			on woi.customer_store_distributor_triad_id = css.customer_store_distributor_triad_id
			and woi.category_id = css.category_id
			and woi.work_order_id = p_work_order_id
		left outer join 
		(
			select 
				 csdd.customer_store_distributor_triad_id
				,max(s.ship_date) :: date as last_ship_date 
			from 
				dbo.customer_store_item_triad csi
				inner join dbo.customer_store_item_distributor_dyad csid
					on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id
				inner join dbo.work_order_items woi
					on woi.work_group_id = csi.work_group_id
					and woi.work_order_id = p_work_order_id
				inner join dbo.customer_distributor_dyad cdd 
					on cdd.oa_distributor_id = csid.oa_distributor_id
					and cdd.national_customer_id = csi.national_customer_id
				inner join dbo.customer_store_distributor_triad csdd 
					on csdd.customer_distributor_dyad_id = cdd.customer_distributor_dyad_id 
					and csdd.oa_store_id = csi.oa_store_id
				inner join dbo.shipments s 
					on s.customer_store_item_triad_id = csi.customer_store_item_triad_id
				group by 
					csdd.customer_store_distributor_triad_id
		) h --dbo.customer_store_item_distributor_dyad_denormal h 
			on h.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id;
    -- where -- check this
		-- csd.customer_store_distributor_triad_id = p_customer_store_distributor_triad_id 
		-- and css.category_id = p_category_id;
		
	for rec in 
		select 
			 lsd.customer_store_distributor_triad_id
			,lsd.last_ship_date
			,lsd.last_scheduled_delivery_date
			,lsd.category_id
			,lsd.inc_in_anomaly
			,lsd.inc_in_file
			,lsd.inc_in_billing
		from
			last_ship_dates lsd
	loop
		return query
			select
				 rec.customer_store_distributor_triad_id
				,rec.last_ship_date
				,rec.last_scheduled_delivery_date
				,rec.category_id
				,rec.inc_in_anomaly
				,rec.inc_in_file
				,rec.inc_in_billing;
	end loop;

END
$BODY$;