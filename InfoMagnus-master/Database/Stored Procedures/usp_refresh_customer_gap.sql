CREATE OR REPLACE FUNCTION public.usp_refresh_customer_gap
(
	 p_national_customer_id integer
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
	
	delete from dbo.customer_gap where national_customer_id = p_national_customer_id;

	/* Populate CustomerGap table. This stores how many days it's been since we last received scans
           from the Retailer. This allows us to be flexible with calculating our gap units if we miss data */

	-- Find the min of reasonable range (avg - 2*StDev) of record count BY RETAILER
	
	drop table if exists scan_range;
	
	create temp table scan_range 
	as
	select 
		 x.national_customer_id
		,avg(x.ct) as avg_cnt
		,stddev(x.ct) as "std"
		,case 
			when avg(x.ct) - 2 * (stddev(x.ct)) < 0 
				then 0
			else avg(x.ct) - 2 * (stddev(x.ct)) 
		 end as "min_range"
	from
	(
		select 
			 tr.national_customer_id
			,sc.transaction_date
			,cast(count(sc.oa_scan_id) as double precision) as "ct"
		from 
			dbo.oa_scans sc
			inner join dbo.customer_store_item_triad tr 
				on tr.customer_store_item_triad_id = sc.customer_store_item_triad_id
				and tr.national_customer_id = p_national_customer_id
		where 
			sc.transaction_date between cast(p_run_date as date) - interval '14 day' and cast(p_run_date as date) - interval '3 day'
		group by 
			 tr.national_customer_id
			,sc.transaction_date
	) x
	group by 
		x.national_customer_id;
	

	-- Create Daily Counts table for updating. This allows us to have 0 unit days appear in results.
	-- Note that we are using clientkey = 1. This is only to prevent duplicates. Customer is not
	-- important for this query as it only pertains to calendar information
	drop table if exists daily_cnts;
	
	create temp table daily_cnts
	as
	select distinct 
		 nc.national_customer_id
		,dd.calendar_date
		,0 as units
	from 
		dbo.dim_date dd
		inner join dbo.oa_national_customers nc
			on 1 = 1
	where 
	(
		dd.calendar_date between p_run_date - interval '5 day' and p_run_date
	)
	and dd.client_key = 1;	
            
    
    -- Update the Daily counts with actual Daily Counts by Retailer.
	update daily_cnts
		set units = z.ct
	from 
		daily_cnts dc
		inner join 
		(
			select 
				 tr.national_customer_id
				,sc.transaction_date
				,count(sc.oa_scan_id) as ct
			from 
				dbo.oa_scans sc
				inner join dbo.customer_store_item_triad tr
					on tr.customer_store_item_triad_id = sc.customer_store_item_triad_id
					and tr.national_customer_id = p_national_customer_id
			where 
				sc.transaction_date between p_run_date - interval '5 day' and p_run_date
			group by 
				 tr.national_customer_id
				,sc.transaction_date
		) z
			on z.national_customer_id = dc.national_customer_id
			and z.transaction_date = dc.calendar_date;
	
    -- For each RETAILER, find the # of days since last full scan load. This will be used in the GapUnits calculation
	insert into dbo.customer_gap 
	(
		 national_customer_id
		,days_since_scans
		,refresh_date
	)
    select 
		 sr.national_customer_id
		,date_part('day', p_run_date :: timestamp - max(calendar_date) :: timestamp)
		,p_run_date
	from 
		scan_range sr 
		inner join daily_cnts dc
			on sr.national_customer_id = dc.national_customer_id
	where 
		dc.units >= sr.min_range
	group by 
		sr.national_customer_id;
	
end;
$BODY$;
