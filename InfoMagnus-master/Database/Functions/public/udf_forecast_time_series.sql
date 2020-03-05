CREATE OR REPLACE FUNCTION public.udf_forecast_time_series
(
	 p_work_order_id integer
	,p_prediction_id integer
	,p_run_date date default (now()::date)
)
    RETURNS TABLE
	(
		 customer_store_item_triad_id integer
		,calendar_date date
		,ttlu integer
	) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
		rec RECORD;
BEGIN 

    if (
			select 
				1 
			from 
				information_schema.tables
			where 
				table_name = 'current_predicted_items'
		) = 1
	then	
       if (
				select 
					max(prediction_date) 
				from 
					dbo.current_predicted_items
		   )  < p_run_date
       then
			drop table dbo.current_predicted_items;
	   end if;
	end if;
	
	create table if not exists dbo.current_predicted_items
    (
		 prediction_id integer
		,prediction_date date
		,customer_store_item_triad_id integer
	);
	
	if p_run_date is null then
		p_run_date := now() :: date;
	end if;
	
	drop table if exists customer_store_item_triad_temp;
	
	create temp table customer_store_item_triad_temp as
	select distinct 
		s.customer_store_item_triad_id
	from 
		dbo.oa_scans s
		inner join dbo.customer_store_item_distributor_dyad csid 
			on csid.customer_store_item_triad_id = s.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csid.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join dbo.customer_store_item_triad csi 
			on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id
		inner join dbo.items i
			on csi.item_id = i.item_id
		inner join dbo.customer_distributor_dyad cd 
			on cd.national_customer_id = csi.national_customer_id and cd.oa_distributor_id = csid.oa_distributor_id
		inner join dbo.customer_store_distributor_triad csd 
			on csd.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
			and csd.oa_store_id = csi.oa_store_id
	where 
		s.transaction_date > (cast ((p_run_date - interval '2 week') as date))
		and (p_run_date between csid.effective_date and csid.expiry_date)
		and s.customer_store_item_triad_id not in 
		(
			select distinct 
				c.customer_store_item_triad_id 
			from 
				dbo.current_predicted_items c
		); 
	
	drop table if exists ts;
	
	create temp table ts as
	select distinct 
		 tr.customer_store_item_triad_id
		,dd.calendar_date
		,0 as TTLU
	from 
		customer_store_item_triad_temp tr
		inner join 
		(
		  select distinct 
			d.calendar_date 
		  from 
			dbo.dim_date d 
		  where 
			(
				d.calendar_date between (p_run_date - interval '3 week'):: date and cast(p_run_date as date)
			)
		) dd 
			on 1 = 1
	order by 
		tr.customer_store_item_triad_id;
	
	delete 
		from ts t
	using 
		dbo.retailer_last_scan_date ld 
	where 
		ld.customer_store_item_triad_id = t.customer_store_item_triad_id
		and t.calendar_date > ld.max_date;
	
	drop table if exists scans;
	
	create temp table scans as
	select 
		 s.customer_store_item_triad_id
		,s.transaction_date
		,s.quantity 
	from 
		dbo.oa_scans s
		inner join dbo.work_order_items woi
			on woi.work_group_id = s.work_group_id
			and woi.work_order_id = p_work_order_id
	where 
		s.transaction_date >= (p_run_date - interval '3 week') :: date;
	
	update ts 
		set ttlu = x.quantity
	from 
		scans x
	where 
		x.customer_store_item_triad_id = ts.customer_store_item_triad_id
		and x.transaction_date = ts.calendar_date;
	
	insert into dbo.current_predicted_items
	select 
		 p_prediction_id as prediction_id
		,p_run_date as predition_date
		,t.customer_store_item_triad_id
	from 
		ts t;
	
	for rec in select ts.customer_store_item_triad_id, ts.calendar_date, ts.ttlu from ts 
	loop
		return query select rec.customer_store_item_triad_id, rec.calendar_date, rec.ttlu;
	end loop;

END
$BODY$;