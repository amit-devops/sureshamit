CREATE OR REPLACE FUNCTION public.usp_calculate_shrink
(
	 p_work_order_id integer
	,p_crweeks integer
	,p_run_date timestamp without time zone default (now() :: timestamp without time zone)
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

/* Add weights to the orders based on credit thresholds */
	
/* Find the credit % over the defined # of weeks on a per item per store 
	basis. First, set basis to 0 so there are no null values */
begin

	delete 
		from dbo.credit_perc a
	using
		 dbo.customer_store_item_triad csit
		,dbo.work_order_items woi
	where
		a.customer_store_item_triad_id = csit.customer_store_item_triad_id
		and woi.work_group_id = csit.work_group_id
		and woi.work_order_id = p_work_order_id 
		and a.run_date = cast(p_run_date as date);

	insert into dbo.credit_perc
	( 
		 oa_distributor_id
		,customer_store_item_triad_id
		,ttl_del
		,ttl_cr
		,cr_perc
		,create_date
		,customer_store_item_distributor_dyad_id
		,run_date
	)
	select 
		 ship.oa_distributor_id
		,ship.customer_store_item_triad_id
		,sum(ship.quantity) as ttl_del
		,0 as Cr
		,0.0 as CrPerc
		,now() as create_date
		,dy.customer_store_item_distributor_dyad_id
		,p_run_date :: date
	from 
		dbo.shipments ship
		inner join dbo.customer_store_item_distributor_dyad dy 
			on dy.customer_store_item_triad_id = ship.customer_store_item_triad_id
			and dy.oa_distributor_id = ship.oa_distributor_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = dy.work_group_id
			and woi.work_order_id = p_work_order_id
	where 
		ship.ship_date >= p_CrWeeks * interval '1 week * -1' + cast(p_run_date as date)
		and ship.is_credit = false
	group by 
		 ship.customer_store_item_triad_id
		,ship.oa_distributor_id
		,dy.customer_store_item_distributor_dyad_id;

	/* Now, update the credit Units with the actual Volume for those items with credits */
	update 
		dbo.credit_perc
	set 
		ttl_cr = x.ttl_cr
	from 
		dbo.credit_perc cp
		inner join dbo.customer_store_item_triad csit
			on cp.customer_store_item_triad_id = csit.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csit.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join 
		(
			select 
				 dy.customer_store_item_distributor_dyad_id
				,sum(ship.quantity) as ttl_cr
			from 
				dbo.shipments ship
				inner join dbo.customer_store_item_distributor_dyad dy 
					on dy.customer_store_item_triad_id = ship.customer_store_item_triad_id
				inner join dbo.work_order_items woi
					on woi.work_group_id = dy.work_group_id
					and woi.work_order_id = p_work_order_id
			where 
				ship.ship_date >= p_CrWeeks * interval '1 week * -1' + cast(p_run_date as date)
				and ship.is_credit = false  
			group by 
				dy.customer_store_item_distributor_dyad_id
		) x 
			on x.customer_store_item_distributor_dyad_id = cp.customer_store_item_distributor_dyad_id
	where 
		cp.run_date = cast(p_run_date as date);

	/* Now, update the credit % with the actual % for those items with credits */
	update 
		dbo.credit_perc
	set 
		cr_perc = x.CrPerc
	from 
		dbo.credit_perc cp
		inner join dbo.customer_store_item_triad csit
			on cp.customer_store_item_triad_id = csit.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csit.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join 
		(
			select 
				 cp2.customer_store_item_distributor_dyad_id
				,case 
					when cp2.ttl_cr != 0
						then round(cast(cp2.ttl_cr as numeric)/cast(cp2.ttl_del as numeric) * -1, 2)
					else 
						0
				 end as CrPerc
			from 
				dbo.credit_perc cp2
				inner join dbo.customer_store_item_triad csit
					on csit.customer_store_item_triad_id = cp2.customer_store_item_triad_id
				inner join dbo.work_order_items woi
					on csit.work_group_id = woi.work_group_id
					and woi.work_order_id = p_work_order_id	
			where 
				cp2.run_date = cast(p_run_date as date)
		) x 
			on x.customer_store_item_distributor_dyad_id = cp.customer_store_item_distributor_dyad_id
	where 
		cp.run_date = cast(p_run_date as date);

	/* Now, if the credit % is outside of the threshold, either add or subtract
		a unit from the order. Otherwise, leave the order as is */
	
	--remove any Weight date for today
	delete 
		from dbo.weight_data a
	using
		 dbo.customer_store_item_triad csit
		,dbo.work_order_items woi
	where
		a.customer_store_item_triad_id = csit.customer_store_item_triad_id
		and woi.work_group_id = csit.work_group_id
		and woi.work_order_id = p_work_order_id 
		and a.run_date = cast(p_run_date as date);			
			
	insert into dbo.weight_data 
	(
		 customer_store_item_triad_id
		,oa_distributor_id
		,customer_store_item_distributor_dyad_id
		,category_id
		,spoils
		,over_under
		,create_date
		,run_date
	)
	select distinct 
		 cp.customer_store_item_triad_id
		,cp.oa_distributor_id
		,cp.customer_store_item_distributor_dyad_id
		,i.category_id
		,cp.cr_perc as Spoils
		,case 
			when abs(cp.cr_perc)>= z.max_credit_percentage 
				then -1
			when abs(cp.cr_perc)<z.max_credit_percentage and abs(cp.cr_perc)>z.min_credit_percentage 
				then 0
			else 
				case 
					when cp.ttl_del > (p_crweeks * 5) and abs(cp.cr_perc) <= z.min_credit_percentage 
						then 1 
					else 
						0
				end
		 end as overunder
		,now() 
		,p_run_date :: date
	from 
		dbo.credit_perc cp
		inner join dbo.customer_store_item_distributor_dyad csd 
			on cp.customer_store_item_distributor_dyad_id = csd.customer_store_item_distributor_dyad_id
		inner join dbo.customer_store_item_triad tr 
			on csd.customer_store_item_triad_id = cp.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = tr.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join dbo.items i 
			on i.item_id = tr.item_id
		inner join 
		(
			select distinct 
				 tr.category_id
				,tr.oa_distributor_id
				,ct.min_credit_percentage
				,ct.max_credit_percentage
			from 
				dbo.credit_thresholds ct
				inner join dbo.customer_distributor_category_triad tr 
					on tr.customer_distributor_category_triad_id = ct.customer_distributor_category_triad_id
			where 
				p_run_date between ct.effective_date and ct.expiry_date
		) z 
			on z.category_id = i.category_id
			and z.oa_distributor_id = csd.oa_distributor_id
	where 
		cp.run_date = cast(p_run_date as date);
	        
	/* Update Weight Data for exceptions to standard thresholds */
	update 
		dbo.weight_data
	set 
		over_under = p.over_under
	from 
		dbo.weight_data w
		inner join dbo.customer_store_item_triad csit
			on csit.customer_store_item_triad_id = w.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csit.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join 
		(
			select 
				 cp.customer_store_item_triad_id
				,cp.customer_store_item_distributor_dyad_id
				,case 
					when abs(cp.cr_perc)> z.max_credit_percentage 
						then -1
					when abs(cp.cr_perc)<z.max_credit_percentage and abs(cp.cr_perc)>z.min_credit_percentage 
						then 0
					else 1
				 end as over_under
				,cp.run_date 
			from 
				dbo.credit_perc cp
				inner join dbo.customer_store_item_triad csit
					on cp.customer_store_item_triad_id = csit.customer_store_item_triad_id
				inner join dbo.work_order_items woi
					on woi.work_group_id = csit.work_group_id
					and woi.work_order_id = p_work_order_id
				inner join 
				(
					select distinct 
						 ct.customer_store_item_distributor_dyad_id
						,ct.min_credit_percentage
						,ct.max_credit_percentage
					from 
						dbo.credit_threshold_exceptions ct
					where 
						p_run_date between ct.effective_date and ct.expiry_date
				) z 
					on cp.customer_store_item_distributor_dyad_id = z.customer_store_item_distributor_dyad_id
		) p 
			on p.customer_store_item_distributor_dyad_id = w.customer_store_item_distributor_dyad_id 
			and p.run_date = w.run_date;
end;
$BODY$;
