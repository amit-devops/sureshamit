CREATE OR REPLACE FUNCTION public.usp_cleanup_and_prep
(
	p_work_order_id integer,
	p_run_date date default (now() :: date),
	p_crweeks integer default 2
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
begin

	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values
	(
		 'usp_cleanup_and_prep'
		,'start'
		,now()
	);
	
	
	if p_run_date is null then
		p_run_date := now() :: date;
	end if;
	
	-- update true up adjustment table to set base_order_id to null for records created on the rundate--
	update 
		dbo.true_up_adjustments
	set 
		base_order_id = null
	from 
		dbo.true_up_adjustments t
		inner join dbo.work_order_items woi
			on woi.work_group_id = t.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join dbo.base_order b 
			on b.base_order_id = t.base_order_id
			and b.run_date = p_run_date;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'clean - dbo.true_up_adjustments'
		, now()
	);
	
	--update override adjustments table to set isused to 0 and useddate to null for records created on the rundate--
	update 
		dbo.override_adjustments
	set 
		 is_used = false
		,used_date = null
	from 
		dbo.override_adjustments ov
		inner join dbo.applied_override_adjustments aov
			on aov.customer_store_item_distributor_dyad_id = ov.customer_store_item_distributor_dyad_id
			and cast(aov.create_date as date) = p_run_date
		inner join dbo.customer_store_item_distributor_dyad csid -- this join is not required if we add work_group_id in base orders table.
			on csid.customer_store_item_distributor_dyad_id = aov.customer_store_item_distributor_dyad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csid.work_group_id
			and woi.work_order_id = p_work_order_id
		inner join dbo.base_order b
			on b.customer_store_item_distributor_dyad_id = ov.customer_store_item_distributor_dyad_id
			and b.run_date = p_run_date;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'clean - overrideadjustments'
		,now()
	);
	
	--delete applied override adjustments for the rundate--
	delete 
		from dbo.applied_override_adjustments aov
	using 
		 dbo.base_order b
		--,dbo.customer_store_item_distributor_dyad csid
		,dbo.work_order_items woi
	where
		b.customer_store_item_distributor_dyad_id = aov.customer_store_item_distributor_dyad_id
		--b.customer_store_item_distributor_dyad_id = csid.customer_store_item_distributor_dyad_id
		and woi.work_group_id = b.work_group_id
		and woi.work_order_id = p_work_order_id 
		and b.run_date = p_run_date;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'clean - dbo.applied_override_adjustments'
		,now()
	);
	
	--delete all conversion residuals for the rundate--
	delete 
		from dbo.conversion_residual cr
	using 
		 dbo.base_order b
		--,dbo.customer_store_item_distributor_dyad csid
		,dbo.work_order_items woi
	where
		b.customer_store_item_distributor_dyad_id = cr.customer_store_item_distributor_dyad_id
		--b.customer_store_item_distributor_dyad_id = csid.customer_store_item_distributor_dyad_id
		and woi.work_group_id = b.work_group_id
		and woi.work_order_id = p_work_order_id 
		and cr.residual_date = p_run_date;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'clean - conversion_residual'
		,now()
	);
	
	--delete applied operator adjustments and overrides for the rundate--
	delete 
		from dbo.applied_operator_adjustments aop
	using 
		 dbo.base_order b
		--,dbo.customer_store_item_distributor_dyad csid
		,dbo.work_order_items woi
	where
		b.base_order_id = aop.base_order_id
		--b.customer_store_item_distributor_dyad_id = csid.customer_store_item_distributor_dyad_id 
		and woi.work_group_id = b.work_group_id
		and woi.work_order_id = p_work_order_id 
		and b.run_date = p_run_date;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'clean - appliedoperatoradjustments'
		,now()
	);
	
	--delete operator adjustments for the rundate--
	delete 
		from dbo.operator_adjustments op
	using 
		 dbo.base_order b
		--,dbo.customer_store_item_distributor_dyad csid
		,dbo.work_order_items woi
	where
		b.customer_store_item_distributor_dyad_id = op.customer_store_item_distributor_dyad_id
		--b.customer_store_item_distributor_dyad_id = csid.customer_store_item_distributor_dyad_id
		and woi.work_group_id = b.work_group_id
		and woi.work_order_id = p_work_order_id 
		and b.run_date = p_run_date;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'clean - operatoradjustments'
		,now()
	);
	
	--delete all base orders that were created on the rundate--
	delete 
		from dbo.base_order b
	using
		 --dbo.customer_store_item_distributor_dyad csid
		dbo.work_order_items woi
	where
		-- csid.customer_store_item_distributor_dyad_id
		woi.work_group_id = b.work_group_id
		and woi.work_order_id = p_work_order_id
		and b.run_date = p_run_date;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'clean - baseorder'
		,now()
	);
	
	--calculate shrink--
	-- check to see if the data has been refreshed today and only refresh if not current
	if not exists 
	(
		select 
			* 
		from 
			dbo.credit_perc a
			inner join dbo.customer_store_item_triad csit -- this can be removed once we add the work_group_id in credict_perc
				on a.customer_store_item_triad_id = csit.customer_store_item_triad_id
			inner join dbo.work_order_items woi
				on woi.work_group_id = csit.work_group_id
				and woi.work_order_id = p_work_order_id
		where 
			cast(a.run_date as date) = p_run_date 
	)
	and not exists 
	(
		select 
			* 
		from 
			dbo.weight_data a
			inner join dbo.customer_store_item_triad csit
				on a.customer_store_item_triad_id = csit.customer_store_item_triad_id
			inner join dbo.work_order_items woi
				on woi.work_group_id = csit.work_group_id
				and woi.work_order_id = p_work_order_id
		where 
			a.run_date = p_run_date
	)
	then
		perform public.usp_calculate_shrink(p_work_order_id, p_crweeks, p_run_date);
	
		insert into dbo.so_log
		(
			 process
			,notes
			,run_date_time
		)
		values 
		(
			 'usp_cleanup_and_prep'
			,'exec dbo.usp_calculateshrink'
			,now()
		);
	end if;
	
	insert into dbo.so_log
	(
		 process
		,notes
		,run_date_time
	)
	values 
	(
		 'usp_cleanup_and_prep'
		,'end'
		,now()
	);
	
	end;
$BODY$;