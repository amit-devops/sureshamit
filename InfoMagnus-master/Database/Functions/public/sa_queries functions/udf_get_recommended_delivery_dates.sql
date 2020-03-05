CREATE OR REPLACE FUNCTION public.udf_get_recommended_delivery_dates
(
	 p_work_order_id integer
	,p_run_date	timestamp without time zone default (now() :: timestamp without time zone)
)
    RETURNS TABLE
	(
		 oa_master_distributor_id integer
		,customer_store_distributor_triad_id integer
		,category_id integer
		,rec_delivery_date date
		,create_date timestamp without time zone
		,true_up varchar(20)
	) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
		rec RECORD;
BEGIN 
	
	drop table if exists rec_delivery_dates;
	
	create temp table rec_delivery_dates
	(
		 oa_master_distributor_id integer
		,customer_store_distributor_triad_id integer
		,category_id integer
		,rec_delivery_date date
		,create_date timestamp without time zone
		,true_up varchar(20)
		,rnk integer
	);
	
	insert into rec_delivery_dates
	(
		 oa_master_distributor_id
		,customer_store_distributor_triad_id
		,category_id
		,rec_delivery_date
		,create_date
		,true_up
		,rnk
	)
	select distinct  -- distinct is added to remove extra step in Python for removing duplicates
		 woi.oa_master_distributor_id 
		,woi.customer_store_distributor_triad_id 
		,woi.category_id  
		,woi.rec_delivery_date  
		,now() :: timestamp without time zone
		,coalesce(csda.attribute_value, '') as true_up
		,row_number() over (partition by woi.customer_store_distributor_triad_id, woi.category_id order by woi.rec_delivery_date) as rnk
	from 
		dbo.work_order_items woi
		left outer join dbo.customer_store_distributor_attributes csda 
			on woi.customer_store_distributor_triad_id = csda.customer_store_distributor_triad_id
			and csda.attribute = 'trueupadjustments'
			and (p_run_date between csda.effective_date and csda.expiry_date)
	where
		woi.work_order_id = p_work_order_id;
	
	for rec in 
		select 
			 rdd.oa_master_distributor_id
			,rdd.customer_store_distributor_triad_id
			,rdd.category_id
			,rdd.rec_delivery_date
			,rdd.create_date
			,case 
				when rdd.rnk > 1 
					then 'suppress' 
				else 
					rdd.true_up 
			end as true_up
		from
			rec_delivery_dates rdd
	loop
		return query
			select
				 rec.oa_master_distributor_id
				,rec.customer_store_distributor_triad_id
				,rec.category_id
				,rec.rec_delivery_date
				,rec.create_date
				,rec.true_up;
	end loop;

END
$BODY$;