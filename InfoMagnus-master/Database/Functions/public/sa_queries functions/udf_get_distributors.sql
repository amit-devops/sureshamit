CREATE OR REPLACE FUNCTION public.udf_get_distributors
(
	p_run_date	timestamp without time zone default (now() :: timestamp without time zone)
)
    RETURNS TABLE
	(
		 process_name varchar(50)
		,is_auto_transmit boolean 
		,order_transfer_method varchar(50)
	) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
		rec RECORD;
BEGIN 
	
	for rec in 
		select 
			 md.process_name
			,md.is_auto_transmit
			,otm.order_transfer_method
		from
			dbo.oa_master_distributor md
			inner join dbo.order_transfer_method otm
				on md.order_transfer_method_id = otm.order_transfer_method_id
		where
			p_run_date between md.effective_date and md.expiry_date
	loop
		return query
			select
				 rec.process_name
				,rec.is_auto_transmit
				,rec.order_transfer_method;
	end loop;

END
$BODY$;