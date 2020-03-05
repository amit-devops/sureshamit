CREATE OR REPLACE FUNCTION public.usp_generate_smart_order
(
	p_run_date timestamp without time zone default (now() :: timestamp without time zone),
	p_clear boolean default false
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
begin

	if p_clear = false 
	then
		insert into dbo.orders 
		(
			 customer_store_distributor_triad_id
			,po_number
			,rec_delivery_date
			,category_id
			,inc_in_billing
		)
		select distinct 
			 b.customer_store_distributor_triad_id
			,cast(b.customer_store_distributor_triad_id as varchar(20)) || cast(b.category_id as varchar(20)) || 
				right(replace(cast(cast(b.rec_delivery_date as date) as varchar(11)), '-', ''), 5) as po_number
			,b.rec_delivery_date
			,b.category_id
			,csds.inc_in_billing
		from 
			dbo.base_order b
			inner join dbo.customer_distributor_dyad cd 
				on cd.customer_distributor_dyad_id = b.customer_distributor_dyad_id 
				and cd.expiry_date > p_run_date
			inner join dbo.customer_store_distributor_schedule csds 
				on b.customer_store_distributor_triad_id = csds.customer_store_distributor_triad_id 
				and b.category_id = csds.category_id 
				and p_run_date between csds.effective_date and csds.expiry_date
		where 
			cast(b.create_date as date) = cast(p_run_date as date);

		-- order status
		insert into dbo.order_status 
		(
			 order_id
			,order_status_type_id
			,effective_date
		)
		select distinct 
			 o.order_id
			,1
			,o.create_date
		from 
			dbo.orders o 
			left outer join dbo.order_status os 
				on os.order_id = o.order_id
		where 
			os.order_status_id is null;
	 
	 
		-- order details
		insert into dbo.order_details
		(
			 order_status_id
			,item_id
			,package_id
			,order_quantity
			,order_date
			,generation_date
			,modified_date
			,should_order
		)
		select 
			 os.order_status_id
			,csi.item_id
			,i.package_id
			,b.proposed_order_quantity
			,b.rec_delivery_date
			,cast(p_run_date as date)
			,now() :: date
			,true
		from 
			dbo.order_status os 
			inner join dbo.orders o 
				on o.order_id = os.order_id
			inner join dbo.vw_base_orders b 
				on b.customer_store_distributor_triad_id = o.customer_store_distributor_triad_id 
				and b.category_id = o.category_id 
				and b.rec_delivery_date = o.rec_delivery_date
			inner join dbo.customer_store_item_triad csi
				on csi.customer_store_item_triad_id = b.customer_store_item_triad_id
			inner join dbo.items i 
				on i.item_id = csi.item_id
			left outer join dbo.order_details od 
				on od.order_status_id = os.order_status_id
		where 
			od.order_detail_id is null;
	else 
	
		drop table if exists order_ids;
		create temp table order_ids as 
		select distinct 
			order_id 
		from 
			dbo.orders 
		where 
			cast(create_date as date) = cast(p_run_date as date);
		
		delete from 
			dbo.order_details od 
		using 
			 dbo.order_status os
			,order_ids o
		where 
			os.order_status_id = od.order_status_id
			and o.order_id = os.order_id;
		
		delete from dbo.order_status os 
		using 
			order_ids o 
		where 
			o.order_id = os.order_id;
		
		delete from dbo.orders oo 
		using 
			order_ids o 
		where 
			o.order_id = oo.order_id;

	end if;
 
end;
$BODY$;

ALTER FUNCTION public.usp_oa_smartorder_generate(date, boolean)
    OWNER TO kbtro;
