CREATE OR REPLACE FUNCTION public.usp_business_adjustments
(
	 p_order_generation_date date default null::date
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

	declare v_variability integer = 2;
	declare v_threshold double precision = 1.0;
	declare v_numweeksmaxshipdate integer = 1;
	declare v_numweekslastorderdeldt integer = 1;
	declare v_numdayslastscandt integer = 14;
	declare v_cspk1adj integer = 3;
	
begin

	if p_order_generation_date is null
	then
		select 
			p_order_generation_date = max(create_date) 
		from 
			dbo.vw_base_orders b;
	end if;
		
    drop table if exists small_items;

	insert into dbo.operator_adjustments 
	(
		 customer_store_item_distributor_dyad_id
		,adjustment_quantity
		,rec_delivery_date
		,create_user
		,operator_adjustments_reason_id
	)
	select 
		 x.customer_store_item_distributor_dyad_id
		,x.too_big_adjustment
		,x.rec_delivery_date
		,'bl - toobig'
		,2
	from 
	(
		select 
			 bo.base_order_id
			,bo.customer_store_distributor_triad_id
			,bo.customer_store_item_triad_id
			,bo.customer_store_item_distributor_dyad_id
			,bo.rec_delivery_date
			,bo.conversion_units
			,bo.proposed_order_quantity
			,bo.max_delivered_quantity
			,bo.max_delivered_quantity - bo.proposed_order_quantity as diff
			,case 
				when bo.proposed_order_quantity = bo.conversion_units 
					then 0
				when bo.max_delivered_quantity <= v_variability 
					then (bo.proposed_order_quantity - public.udf_excel_max(bo.conversion_units, v_variability)) * -1
				when bo.proposed_order_quantity > (bo.max_delivered_quantity + v_variability) and bo.max_delivered_quantity > 0
					then round(((bo.proposed_order_quantity - bo.max_delivered_quantity) / bo.conversion_units), 0) * -1 * bo.conversion_units -- note no cast to float. this is by design for int division.
				else 0
			 end as too_big_adjustment
		from 
			dbo.vw_base_orders bo
			left join dbo.operator_adjustments oa 
				on oa.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id and oa.rec_delivery_date = bo.rec_delivery_date
			left join dbo.applied_operator_adjustments aoa 
				on aoa.base_order_id = bo.base_order_id
		where 
			bo.create_date = p_order_generation_date
			and (bo.max_delivered_quantity - bo.proposed_order_quantity) < 0
			and bo.proposed_order_quantity > 4
			and bo.override_quantity = 0
			and oa.adjustment_id is null
			and aoa.adjustment_id is null
	) x
	where x.too_big_adjustment < 0;

	-- small / slow moving items
		-- use: to identify slow moving items that are in need of an order even though their
		-- inter-delivery replenishment doesn't require one
	--declare @ordergendate as date = (select max(bo.create_date) from dbo.vw_base_orders bo)\
	
	create temp table small_items as
	select 
		 bo.base_order_id
		,bo.customer_store_item_distributor_dyad_id
		,bo.customer_store_item_triad_id
		,bo.customer_store_distributor_triad_id
		,bo.rec_delivery_date
		,bo.conversion_units as casepack
		,bo.proposed_order_quantity
		,cast(null as date) as maxscandt
		,cast(null as date) as maxdeldate
		,cast(null as int) as qtyscanned
		,cast('99991231' as date) as lastorderdeldt
		,cast(null as double precision) as numcases
		,cast(null as integer) as adjustment_quantity
	from 
		dbo.vw_base_orders bo
		inner join 
		(
			-- only look at the first delivery date
			select 
				 bo1.customer_store_item_triad_id
				,min(bo1.rec_delivery_date) as rec_delivery_date
			from 
				dbo.base_order bo1
			where 
				cast(bo1.create_date as date) = p_order_generation_date
			group by 
				bo1.customer_store_item_triad_id
		) bo2 
			on bo.customer_store_item_triad_id = bo2.customer_store_item_triad_id
			and bo.rec_delivery_date = bo2.rec_delivery_date
	where 
		cast(bo.create_date as date) = p_order_generation_date
		and bo.proposed_order_quantity < 1
	group by 
		 bo.base_order_id
		,bo.customer_store_item_distributor_dyad_id
		,bo.customer_store_item_triad_id
		,bo.customer_store_distributor_triad_id
		,bo.rec_delivery_date
		,bo.conversion_units
		,bo.proposed_order_quantity;

	update small_items
		set maxscandt = sc.maxscandt
	from 
		small_items si
		inner join 
		(
			select 
				 sc1.customer_store_item_triad_id
				,max(sc1.transaction_date) as maxscandt
			from 
				dbo.oa_scans sc1
			group by 
				sc1.customer_store_item_triad_id
		) sc 
			on si.customer_store_item_triad_id = sc.customer_store_item_triad_id;

	update small_items
		set maxdeldate = s.maxdeldate
	from 
		small_items si
		inner join 
		(
			select 
				s2.*
			from 
			(
				select 
					 s1.customer_store_item_triad_id
					,max(s1.shipdate) as maxdeldate
				from 
					dbo.shipments s1
				where 
					s1.is_credit = false
				group by 
					s1.customer_store_item_triad_id
			) s2
		where 
			s2.maxdeldate <= dateadd(week, -1 * @numweeksmaxshipdate, p_order_generation_date)
	) s 
		on si.customer_store_item_triad_id = s.customer_store_item_triad_id;

	update small_items
		set qtyscanned = sc.qtyscanned
	from 
		small_items si
	inner join 
	(
		select 
			 si1.customer_store_item_triad_id
			,sum(sc1.qty) as qtyscanned
		from 
			small_items si1
			inner join dbo.oa_scans sc1 
				on si1.customer_store_item_triad_id = sc1.customer_store_item_triad_id
		where 
			sc1.transdate >= si1.maxdeldate
		group by 
			si1.customer_store_item_triad_id
	) sc 
		on si.customer_store_item_triad_id = sc.customer_store_item_triad_id;

	update 
		small_items
	set 
		lastorderdeldt = x.lastorderdeldt
	from 
		small_items si
	inner join 
	(
		select 
			 csi.customer_store_item_triad_id
			,max(o.rec_delivery_date) as lastorderdeldt
		from 
			dbo.orders o
			inner join dbo.order_status os 
				on os.order_id = o.order_id
			inner join dbo.order_details od 
				on od.order_status_id = os.order_status_id
			inner join dbo.customer_store_distributor_triads csd 
				on csd.customer_store_distributor_triad_id = o.customer_store_distributor_triad_id
			inner join dbo.customer_distributor_dyad cd 
				on cd.customer_distributor_dyad_id = csd.customer_distributor_dyad_id
			inner join dbo.customer_store_item_triad csi 
				on csi.national_customer_id = cd.national_customer_id
				and csi.oa_store_id = csd.oa_store_id
				and csi.item_id = od.item_id
		where 
			od.orderqty > 0
		group by 
			csi.customer_store_item_triad_id
	) x 
		on x.customer_store_item_triad_id = si.customer_store_item_triad_id;

	update 
		small_items
	set 
		numcases = round(cast(qtyscanned as double precision) / cast(casepack as double precision), 2);

	update 
		small_items
	set 
		adjustment_quantity = 
			case 
				when 
					proposed_order_quantity < 1 
					and maxscandt >= dateadd(day, -1 * v_numdayslastscandt, p_order_generation_date)
					and numcases >= 1
					and lastorderdeldt < dateadd(week, -1 * v_numweekslastorderdeldt, cast(now() as date))
				then 
				(
					case 
						when casepack = 1 
							then v_cspk1adj
						else casepack
					end + (-1 * proposed_order_quantity)
				)
				else 
					0
			end;

	insert into dbo.operator_adjustments 
	(
		 customer_store_item_distributor_dyad_id
		,adjustment_quantity
		,rec_delivery_date
		,create_user
		,operator_adjustments_reason_id
	)
	select 
		 si.customer_store_item_distributor_dyad_id
		,si.adjustment_quantity
		,si.rec_delivery_date
		,'bl - slowmoving'
		,2
	from 
		small_items si
		left join dbo.operator_adjustments oa 
			on oa.customer_store_item_distributor_dyad_id = si.customer_store_item_distributor_dyad_id 
		left join dbo.applied_operator_adjustments aoa 
			on aoa.base_order_id = si.base_order_id
	where 
		si.numcases >= @threshold
		and si.adjustment_quantity > 0
		and oa.adjustment_id is null
		and aoa.adjustment_id is null;
		
end;
$BODY$;