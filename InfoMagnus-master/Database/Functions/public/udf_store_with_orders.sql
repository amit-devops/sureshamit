CREATE OR REPLACE FUNCTION public.udf_store_with_orders
(
	p_run_date date
)
    RETURNS TABLE
    (
    	 oa_master_distributor_id integer
    	,oa_distributor_id integer
		,national_customer_id integer
		,oa_store_id integer
    	,customer_store_distributor_triad_id integer
		,category_id integer
		,rec_delivery_date date
		,create_date timestamp without time zone
    )
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$

	DECLARE rec RECORD;
	BEGIN
		DROP TABLE IF EXISTS stores;
		CREATE TEMP TABLE stores
		(
			oa_master_distributor_id integer,
			oa_distributor_id integer,
			national_customer_id integer,
			oa_store_id integer,
			customer_store_distributor_triad_id integer,
			category_id integer,
			rec_delivery_date date,
			create_date timestamp without time zone
		);

		if p_run_date is null then
			p_run_date := now();
		end if;

		insert into stores
		(
			oa_master_distributor_id,
			oa_distributor_id,
			national_customer_id,
			oa_store_id,
			customer_store_distributor_triad_id,
			category_id,
			rec_delivery_date,
			create_date
		)
		select d.oa_master_distributor_id
			, d.oa_distributor_id
			, cd.national_customer_id
			, csd.oa_store_id
			, csd.customer_store_distributor_triad_id
			, csds.category_id
			, lt.lead_time_days * interval '1 day' + cast(p_run_date as date) as rec_delivery_date
			, now() as create_date
		from dbo.delivery_schedules ds
		inner join dbo.customer_store_distributor_schedule csds
			on ds.delivery_schedule_id = csds.delivery_schedule_id
		inner join dbo.customer_store_distributor_triad csd
			on csds.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id
		inner join dbo.customer_distributor_dyad cd
			on csd.customer_distributor_dyad_id = cd.customer_distributor_dyad_id
		inner join dbo.oa_distributors d
			on cd.oa_distributor_id = d.oa_distributor_id
		inner join dbo.customer_distributor_category_triad cdc
			on cdc.national_customer_id = cd.national_customer_id
			and cdc.oa_distributor_id = cd.oa_distributor_id
			and cdc.category_id = csds.category_id
		inner join dbo.lead_times lt
			on lt.oa_store_id = csd.oa_store_id
			and lt.customer_distributor_category_triad_id = cdc.customer_distributor_category_triad_id
		where p_run_date between ds.effective_date and ds.expiry_date
			and p_run_date between csd.effective_date and csd.expiry_date
			and p_run_date between csds.effective_date and csds.expiry_date
			and p_run_date between lt.effective_date and lt.expiry_date
			and public.udf_is_delivery_day(ds.delivery_schedule_id, (lt.lead_time_days * interval '1 day' + p_run_date):: date) = 1

		union

		select distinct
			d.oa_master_distributor_id
			, d.oa_distributor_id
			, cd.national_customer_id
			, csd.oa_store_id
			, csd.customer_store_distributor_triad_id
			, csds.category_id
			, lte.lead_time * interval '1 day' + p_run_date as rec_delivery_date
			, now() as create_date
		from dbo.delivery_schedules ds
		inner join dbo.customer_store_distributor_schedule csds
			on csds.delivery_schedule_id = ds.delivery_schedule_id
		inner join dbo.customer_store_distributor_triad csd
			on csd.customer_store_distributor_triad_id = csds.customer_store_distributor_triad_id
		inner join dbo.customer_distributor_dyad cd
			on cd.customer_distributor_dyad_id = csd.customer_distributor_dyad_id
		inner join dbo.oa_distributors d
			on d.oa_distributor_id = cd.oa_distributor_id
		inner join dbo.lead_time_exceptions lte
			on lte.oa_master_distributor_id = d.oa_master_distributor_id
		left outer join dbo.store_lead_time_exceptions slte
			on slte.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id
			and p_run_date between slte.effective_date and slte.expiry_date
		where p_run_date between ds.effective_date and ds.expiry_date
			and p_run_date between csds.effective_date and csds.expiry_date
			and p_run_date between csd.effective_date and csd.expiry_date
			and p_run_date between lte.effective_date and lte.expiry_date
			and lte.order_date_dow = to_char(p_run_date, 'Day')
			and public.udf_is_delivery_day(ds.delivery_schedule_id,(lte.lead_time * interval '1 day' + p_run_date):: date) = 1
			and slte.customer_store_distributor_triad_id is null

		union

		select distinct
			d.oa_master_distributor_id
			, d.oa_distributor_id
			, cd.national_customer_id
			, csd.oa_store_id
			, csd.customer_store_distributor_triad_id
			, csds.category_id
			, lte.lead_time * interval '1 day' + p_run_date as rec_delivery_date
			, now() as create_date
		from dbo.delivery_schedules ds
		inner join dbo.customer_store_distributor_schedule csds
			on csds.delivery_schedule_id = ds.delivery_schedule_id
		inner join dbo.customer_store_distributor_triad csd
			on csd.customer_store_distributor_triad_id = csds.customer_store_distributor_triad_id
		inner join dbo.customer_distributor_dyad cd
			on cd.customer_distributor_dyad_id = csd.customer_distributor_dyad_id
		inner join dbo.oa_distributors d
			on d.oa_distributor_id = cd.oa_distributor_id
		inner join dbo.store_lead_time_exceptions lte
			on lte.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id
		where p_run_date between ds.effective_date and ds.expiry_date
			and p_run_date between csds.effective_date and csds.expiry_date
			and p_run_date between csd.effective_date and csd.expiry_date
			and p_run_date between lte.effective_date and lte.expiry_date
			and lte.order_date_dow = to_char(p_run_date, 'Day')
			and public.udf_is_delivery_day(ds.delivery_schedule_id,(lte.lead_time * interval '1 day' + p_run_date)::date)  = 1;
			
		FOR rec IN
		select
			st.oa_master_distributor_id,
			st.oa_distributor_id,
			st.national_customer_id,
			st.oa_store_id,
			st.customer_store_distributor_triad_id,
			st.category_id,
			st.rec_delivery_date,
			st.create_date
		from
			stores st
		LOOP
			return query
			select
				rec.oa_master_distributor_id,
				rec.oa_distributor_id,
				rec.national_customer_id,
				rec.oa_store_id,
				rec.customer_store_distributor_triad_id,
				rec.category_id,
				rec.rec_delivery_date,
				rec.create_date;
			
	 	END LOOP;
		END

$BODY$;
