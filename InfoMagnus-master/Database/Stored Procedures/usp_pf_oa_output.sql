CREATE OR REPLACE FUNCTION public.usp_pf_oa_output
(
	 p_master_distributor_id integer
	,p_run_date timestamp without time zone default (now() :: timestamp without time zone)
	,p_suppress_zero integer default 1
)
    RETURNS TABLE
	(
		 location_number integer
		,customer_number integer
		,order_type integer
		,delivery_date varchar(15)
		,order_number integer
		,product_number integer
		,cases integer
		,units integer
		,pfpo varchar(10)
		,distributor_item_description varchar(255)
		,customer_name varchar(20)
		,po_number varchar(255)
	) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
declare
	rec record;
begin

	drop table if exists batchquery;
	create temp table batchquery 
	(
		 location_number integer
		,customer_number integer
		,order_type integer
		,delivery_date varchar(15)
		,order_number integer
		,product_number integer
		,cases integer default 0
		,units integer
		,pfpo varchar(10) default ''
		,distributor_item_description varchar(255)
		,customer_name varchar(20) default ''
		,po_number varchar(255)
	);
	
	with dist_category_details 
	as
	(
		select 
			 csd.distributor_store_number
			,o.category_id
			,o.rec_delivery_date
		from 
			dbo.orders o
			inner join dbo.customer_store_distributor_triad csd 
				on o.customer_store_distributor_triad_id = csd.customer_store_distributor_triad_id
		where 
			cast(o.create_date as date) = cast(p_run_date as date)
	)
	,dist_category_final
	 as
	 (
		select 
			 x.distributor_store_number
			,x.category_id
			,x.rec_delivery_date
			,row_number() over(partition by x.distributor_store_number, x.category_id order by x.rec_delivery_date) as order_seq_number
		from 
			dist_category_details x
	)
	insert into batchquery
	(
		 location_number
		,customer_number
		,order_type 
		,delivery_date
		,order_number 
		,product_number 
		,units 
		,distributor_item_description
		,po_number
	)
	select distinct 
		 d.distributor_number as location_number 
		,csd.distributor_store_number as customer_number 
		,case 
			when lower(c.category_name) = 'ice cream' 
				then 2 
			else 
				1
		 end as order_type
		,case 
			when p_master_distributor_id = 5 
				then to_char(o.rec_delivery_date :: date, 'dd/mm/yy')
			else 
				replace(to_char(o.rec_delivery_date :: date, 'dd/mm/yy'), '/', '')
		 end as "delivery_date"
		,a.order_seq_number as order_number
		,di.distributor_product_code as product_number
		,case 
			when p_master_distributor_id = 5 and lower(di.distributor_item_description) like '%v8%'
				then cast(round( cast(od.order_quantity as numeric) / cast(12 as numeric), 0) as integer)
			else 
				od.order_quantity
		  end
		,coalesce(di.distributor_item_description, '') as distributor_item_description
		,o.po_number
	from 
		dbo.orders o 
		inner join dbo.order_status os 
			on os.order_id = o.order_id 
		inner join dbo.order_details od 
			on od.order_status_id = os.order_status_id
		inner join dbo.order_status_type ost 
			on ost.order_status_type_id = os.order_status_type_id
		inner join dbo.customer_store_distributor_triad csd 
			on csd.customer_store_distributor_triad_id = o.customer_store_distributor_triad_id
		inner join dbo.customer_store_distributor_schedule csds 
			on csd.customer_store_distributor_triad_id = csds.customer_store_distributor_triad_id
			and (p_run_date between csds.effective_date and csds.expiry_date)
			and csds.inc_in_file = true
		inner join dbo.customer_distributor_dyad dy 
			on dy.customer_distributor_dyad_id  = csd.customer_distributor_dyad_id
		inner join dbo.oa_distributors d 
			on d.oa_distributor_id = dy.oa_distributor_id
		inner join dbo.items i 
			on i.item_id = od.item_id 
			and i.expiry_date > p_run_date
		inner join dbo.categories c 
			on c.category_id = i.category_id
		inner join dbo.distributor_items di 
			on di.item_id = i.item_id
			and di.customer_distributor_dyad_id = dy.customer_distributor_dyad_id 
			and di.expiry_date > p_run_date
		inner join dist_category_final a
			on a.rec_delivery_date = o.rec_delivery_date
			and o.category_id = a.category_id
			and csd.distributor_store_number = a.distributor_store_number
	where 
		d.oa_master_distributor_id = p_master_distributor_id
		and cast(o.create_date as date) = cast(p_run_date as date)
		and (p_suppress_zero = 0 or od.order_quantity > 0)
	order by 
		 location_number 
		,customer_number
		,delivery_date 
		,product_number; 
	
	for rec in 
		select 
			 b.location_number
			,b.customer_number
			,b.order_type
			,b.delivery_date
			,b.order_number
			,b.product_number
			,b.cases
			,b.units
			,b.pfpo
			,b.distributor_item_description
			,b.customer_name
			,b.po_number 
		from 
			batchquery b 
	loop 
		return query 
			select 
				 rec.location_number
				,rec.customer_number
				,rec.order_type
				,rec.delivery_date
				,rec.order_number
				,rec.product_number
				,rec.cases
				,rec.units
				,rec.pfpo
				,rec.distributor_item_description
				,rec.customer_name
				,rec.po_number;
	end loop;

end;
$BODY$;
