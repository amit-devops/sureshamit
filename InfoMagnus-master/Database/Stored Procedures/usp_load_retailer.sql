CREATE OR REPLACE FUNCTION public.usp_load_retailer
(
	 p_national_customer_id integer
	,p_scan_source 			varchar(255)
	,p_run_date				timestamp without time zone default(now() :: timestamp without time zone)
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

declare v_national_customer_id int = p_national_customer_id;
declare v_scan_source varchar(255) = p_scan_source;

begin

	if p_run_date is null then
		p_run_date := now() :: timestamp without time zone;
	end if;

	insert into dbo.etl_log
	(
		notes
		,run_date_time
		,national_customer_id
	)
	values 
	(
		'Start'
		,now()
		,v_national_customer_id
	);
	
	-- Archive the data
	perform usp_archive_scan_data(v_national_customer_id); 

	insert into dbo.etl_log
	(
		 notes
		,run_date_time
		,national_customer_id
	)
	values 
	(
		 'Archives Complete'
		,now()
		,v_national_customer_id
	);

    -- Add missing stores
    insert into dbo.oa_stores 
	(
		 national_customer_id
		,store_number
	)
	select distinct
		 s.national_customer_id
		,s.store_number
	from 
		dbo.stg_scan s
		left outer join dbo.oa_stores os 
			on os.store_number = s.store_number 
			and os.national_customer_id = s.national_customer_id
	where 
		os.oa_store_id is null
		and s.national_customer_id = v_national_customer_id;

	insert into dbo.etl_log
	(
		 notes
		,run_date_time
		,national_customer_id
	)
	values 
	(
		 'Stores Added'
		,now()
		,v_national_customer_id
	);

    /**
     *   Create the many-to-many relationship between stores and items.
     *  We want to link a store to it's items and items to their respective stores.
     *
     */
	 
    insert into dbo.customer_store_item_triad 
	(
		 national_customer_id
		,oa_store_id
		,item_id
		,work_group_id
	)
    select distinct
		 v_national_customer_id
		,os.oa_store_id
		,i.item_id
		,cast(os.oa_store_id as varchar(10)) || '/' || cast(i.category_id as varchar(5)) as work_group_id
	from 
		dbo.stg_scan s
		inner join dbo.oa_stores os 
			on os.store_number = s.store_number 
			and os.national_customer_id = s.national_customer_id
			and s.national_customer_id = v_national_customer_id
		inner join dbo.oa_sku_upc_conversion sk 
			on s.sku = sk.sku 
			and sk.national_customer_id = s.national_customer_id and now() <= sk.expiry_date
		inner join dbo.items i 
			on cast(i.upc as bigint) = cast(sk.upc as bigint) 
			and i.sku = sk.sku 
			and i.national_customer_id = s.national_customer_id 
			and i.category_id is not null
		left outer join dbo.customer_store_item_triad csi 
			on csi.oa_store_id = os.oa_store_id 
			and csi.item_id = i.item_id 
			and csi.national_customer_id = s.national_customer_id
    where 
		csi.customer_store_item_triad_id is null;

	insert into dbo.etl_log
	(
		 notes
		,run_date_time
		,national_customer_id
	)
	values 
	(
		 'CustStorItemTriad Added'
		,now()
		,v_national_customer_id
	);

	-- Insert the staged records to OaScans
    insert into dbo.oa_scans
	(
		 customer_store_item_triad_id
		,transaction_date
		,quantity
		,scans_source
		,work_group_id
	)
	select 
		 csi.customer_store_item_triad_id
		,cast(s.transaction_date as date) transaction_date
		,sum(cast(cast(s.quantity as double precision) as int)) as quantity
		,v_scan_source || cast(now() as varchar(25))
		,cast(os.oa_store_id as varchar(10)) || '/' || cast(i.category_id as varchar(5)) as work_group_id
	from 
		dbo.stg_scan s
		inner join dbo.oa_stores os 
			on os.store_number = s.store_number 
			and os.national_customer_id = s.national_customer_id
			and s.national_customer_id = v_national_customer_id
		inner join dbo.oa_sku_upc_conversion sk 
			on s.sku = sk.sku 
			and sk.national_customer_id = s.national_customer_id 
			and p_run_date <= sk.expiry_date
		inner join dbo.items i 
			on cast(i.upc as bigint) = cast(sk.upc as bigint) 
			and i.sku = sk.sku 
			and i.national_customer_id = s.national_customer_id 
			and i.category_id is not null
		inner join dbo.customer_store_item_triad csi 
			on csi.oa_store_id = os.oa_store_id 
			and csi.item_id = i.item_id 
			and csi.national_customer_id = s.national_customer_id
		left outer join dbo.oa_scans osc 
			on osc.customer_store_item_triad_id = csi.customer_store_item_triad_id 
			and osc.transaction_date = cast(s.transaction_date as date)
		where 
			osc.oa_scan_id is null
		group by 
			 csi.customer_store_item_triad_id
			,s.transaction_date
			,os.oa_store_id
			,i.category_id;

	insert into dbo.etl_log
	(
		 notes
		,run_date_time
		,national_customer_id
	)
	values 
	(
		 'OAScans Added'
		,now()
		,v_national_customer_id
	);

     -- Insert into OAScansSales
	insert into dbo.oa_scans_sales 
	(
		 oa_scan_id
		,units
		,sales_dollars
		,transaction_date
		,work_group_id
	)
	select 
		 osc.oa_scan_id
		,cast(cast(s.quantity as double precision) as int) as units
		,s.total_cost
		,osc.transaction_date
		,osc.work_group_id
	from 
		dbo.stg_scan s
		inner join dbo.oa_stores os 
			on os.store_number = s.store_number 
			and os.national_customer_id = s.national_customer_id 
			and s.national_customer_id = v_national_customer_id
		inner join dbo.oa_sku_upc_conversion sk 
			on s.sku = sk.sku 
			and sk.national_customer_id = s.national_customer_id
		inner join dbo.items i 
			on cast(i.upc as bigint) = cast(sk.upc as bigint) 
			and i.sku = sk.sku 
			and i.national_customer_id = s.national_customer_id 
			and i.category_id is not null
		inner join dbo.customer_store_item_triad csi 
			on csi.oa_store_id = os.oa_store_id 
			and csi.item_id = i.item_id 
			and csi.national_customer_id = s.national_customer_id
		inner join dbo.oa_scans osc 
			on osc.customer_store_item_triad_id = csi.customer_store_item_triad_id 
			and osc.transaction_date = cast(s.transaction_date as date);

	insert into dbo.etl_log
	(
		 notes
		,run_date_time
		,national_customer_id
	)
	values 
	(
		 'OAScansSales Added'
		,now()
		,v_national_customer_id
	);

	-- Refresh Customer Gap
	perform public.usp_refresh_customer_gap(p_national_customer_id, cast(p_run_date as date));
	
	-- Refresh last scan date
	perform public.usp_refresh_retailer_last_scan_dt(p_national_customer_id, cast(p_run_date as date));

	insert into dbo.etl_log
	(
		 notes
		,run_date_time
		,national_customer_id
	)
	values 
	(
		  'End'
		 ,now()
		 ,v_national_customer_id
	);

end;
$BODY$;