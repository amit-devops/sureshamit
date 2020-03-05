CREATE OR REPLACE FUNCTION public.usp_final_cleanup
(
	 p_work_order_id integer
	,p_run_date timestamp without time zone default (now() :: timestamp without time zone)
)	
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

 declare v_thisdate date = p_run_date;--(select cast(max(run_date) as date) from dbo.base_order);
         v_prevdate date = interval '-10 day' + v_thisdate;
		  
begin

	update 
		dbo.true_up_adjustments
	set 
		base_order_id = f.applied_id
	from 
		dbo.true_up_adjustments t
		inner join 
		(
			select 
				 tua.true_up_adjustment_id
				,case 
					when lower(x.trueup) = 'suppress' and cast(tua.create_date as date) < interval '-14 day' + cast(p_run_date as date)
						then 1
					when x.trueup = '' 
						then x.base_order_id
					else null
				 end as applied_id
			from 
				dbo.true_up_adjustments tua
				inner join 
				(
					select 
						 bo.base_order_id
						,bo.customer_store_item_triad_id
						,bo.rec_delivery_date
						,coalesce(csda.attribute_value,'') as trueup
						,row_number() over (partition by bo.customer_store_item_triad_id order by bo.rec_delivery_date) as rn
					from 
						dbo.base_order bo
						inner join dbo.work_order_items woi
							on woi.work_group_id = bo.work_group_id
							and woi.work_order_id = p_work_order_id
						left outer join dbo.customer_store_distributor_attributes csda 
							on csda.customer_store_distributor_triad_id = bo.customer_store_distributor_triad_id
							and lower(csda.attribute) = 'trueupadjustments'
							and p_run_date between csda.effective_date and csda.expiry_date
					where 
						bo.run_date= cast(p_run_date as date)
				) x
					on x.customer_store_item_triad_id = tua.customer_store_item_triad_id
			where 
				tua.base_order_id is null
				and x.rn = 1
		) f 
			on f.true_up_adjustment_id = t.true_up_adjustment_id
	where 
		f.applied_id is not null;

    if not exists 
	(
		select 
			* 
		from 
			dbo.oa_scans_10_days a
			-- inner join dbo.work_order_items woi
				-- on woi.work_group_id = a.work_group_id
				-- and woi.work_order_id = p_work_order_id
		where 
			run_date = v_thisdate
	)
	then
		
		truncate table dbo.oa_scans_10_days;

		insert into dbo.oa_scans_10_days 
		(
			 customer_store_item_triad_id
			,scan_10_days
			,work_group_id
			,run_date
		)
		select 
			 sc.customer_store_item_triad_id
			,sum(coalesce(sc.quantity, 0)) as scan_10_days
			,woi.work_group_id
			,p_run_date
		from 
			dbo.oa_scans sc
			inner join dbo.work_order_items woi
				on woi.work_group_id = sc.work_group_id
				and woi.work_order_id = p_work_order_id
		where 
			sc.transaction_date between v_prevdate and v_thisdate
		group by 
			 sc.customer_store_item_triad_id
			,woi.work_group_id;
	end if;

	if not exists 
	(
		select 
			* 
		from 
			dbo.last_scheduled_delivery
		where 
			run_date = v_thisdate
	)
	then
		truncate table dbo.last_scheduled_delivery;

		insert into dbo.last_scheduled_delivery 
		(
			 customer_store_distributor_triad_id
			,category_id
			,rec_delivery_date
			,last_delivery
			,work_group_id
			,run_date
		)
		select
			 b.customer_store_distributor_triad_id
			,b.category_id
			,b.rec_delivery_date
			,public.udf_last_scheduled_delivery_date(b.delivery_schedule_id, interval '-1 day' + b.rec_delivery_date) as lastdel
			,woi.work_group_id
			,p_run_date
		from 
			dbo.vw_base_orders b -- add the work_group_id in the view definition
			inner join dbo.work_order_items woi
				on woi.work_group_id = b.work_group_id
				and woi.work_order_id = p_work_order_id
		where 
			b.run_date = v_thisdate
		group by 
			 b.customer_store_distributor_triad_id
			,b.category_id
			,b.delivery_schedule_id
			,b.rec_delivery_date;
	end if;
end;
$BODY$;
