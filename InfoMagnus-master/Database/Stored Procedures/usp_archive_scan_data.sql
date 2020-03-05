CREATE OR REPLACE FUNCTION public.usp_archive_scan_data
(
	p_national_customer_id integer
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

begin

	insert into archive.stg_scan
	select 
		 s.sku
		,s.store_number
		,s.transaction_date
		,cast(cast(s.quantity as float) as int) as quantity -- We can remove cast statement post changing data type in dbo.stgscan
		,s.total_cost
		,s.national_customer_id
	from 
		dbo.stg_scan s 
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_scan 
		where 
			transaction_date = s.transaction_date 
			and national_customer_id = s.national_customer_id
	)
	and s.national_customer_id = p_national_customer_id;

end;
$BODY$;