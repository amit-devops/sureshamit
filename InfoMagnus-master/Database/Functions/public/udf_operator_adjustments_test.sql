CREATE OR REPLACE FUNCTION public.udf_operator_adjustments_test
(
	p_order_gen_date date default null::date
)
    RETURNS TABLE
	(
		 base_order_id	int
		,order_gen_date	date
		,customer_store_distributor_triad_id	int
		,national_customer_id	int
		,category_id	int
		,customer_store_item_triad_id	int
		,last_delivery	date
		,lastscandt	date
		,dom	int
		,dow	int
		,monum	int
		,rec_delivery_date	date
		,scans_since	int
		,tendayavgquantity	int
		,model_used	varchar(255)
		,actualscans	int
		,forecastedscans	int 
		,trueup	bigint
		,massadjustments	int
		,spoilsadj	int
		,operatoradj	int
		,distroverride	bigint
		,casepack	int
		,casepackres bigint
		,override_quantity	int
		,original_prop_order_quantity	int
		,adjustedorder	int
		,mathorder	int
		,dayssincelast_delivery	int
		,maxdelquantity	int
		,inventorychange	int
	)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
	ROWS 100
AS $BODY$
declare rec record;
begin

	if p_order_gen_date is null
	then 
		p_order_gen_date := cast(now() as date);
	end if;

	create sequence if not exists temp_seq;
	drop table if exists temp_table_inv;
	create temp table temp_table_inv
	(
		 id int default nextval ('temp_seq')
		,customer_store_item_triad_id int
		,last_delivery_date date
		,last_delivery_quantity int
		,scans_since int
		,inventory_change int
	);
	
	insert into temp_table_inv 
	(
		 customer_store_item_triad_id
		,last_delivery_date
		,last_delivery_quantity
		,scans_since
		,inventory_change
	)
	select 
		 z.customer_store_item_triad_id
		,z.max_date as last_delivery_date
		,sum(s.quantity) as last_delivery_quantity
		,0 as scans_since
		,0 as inventory_change
	from 
	(
		select 
			 s.customer_store_item_triad_id
			,max(s.ship_date) as max_date
		from 
			dbo.shipments s
		where 
			s.is_credit = false
			and s.ship_date < p_order_gen_date
		group by 
			s.customer_store_item_triad_id
	) z
	inner join dbo.shipments s
		on s.ship_date = z.max_date
		and s.customer_store_item_triad_id = z.customer_store_item_triad_id
		and s.is_credit = false
	group by 
		 z.customer_store_item_triad_id
		,z.max_date;
	
	drop table if exists scans;
	create temp table scans 
	as
	select 
		 t.customer_store_item_triad_id
		,sum(s.quantity) as ttlu
	from 
		temp_table_inv t
		inner join dbo.oa_scans s
			on s.customer_store_item_triad_id = t.customer_store_item_triad_id
	where 
		s.transaction_date >= t.last_delivery_date
		and s.transaction_date < p_order_gen_date
	group by 
		t.customer_store_item_triad_id;
	
	update 
		temp_table_inv
	set 
		scans_since = s.ttlu
	from 
		temp_table_inv t
		inner join scans s
			on t.customer_store_item_triad_id = s.customer_store_item_triad_id;
	
	update 
		temp_table_inv
	set 
		inventory_change = x.invchange
	from 
		temp_table_inv t
		inner join 
		(
			select 
				 t.customer_store_item_triad_id
				,t.last_delivery_quantity - t.scans_since as invchange
			from 
				temp_table_inv t
		) x
			on x.customer_store_item_triad_id = t.customer_store_item_triad_id;
	
	drop table if exists scans;

	drop table if exists opr_adj;
	create temp table opr_adj
	as
	select 
		 bo.base_order_id
		,p_order_gen_date as order_gen_date
		,bo.customer_store_distributor_triad_id
		,dy.national_customer_id
		,bo.category_id
		--,distributorstorenumber
		--,distributorproductcode
		,bo.customer_store_item_triad_id
		,ld.last_delivery
		,lsc.maxscandt as lastscandt
		,cast(date_part('day', bo.rec_delivery_date) as int) as dom
		,cast(date_part('dow', bo.rec_delivery_date) as int) as dow
		,cast(date_part('month', bo.rec_delivery_date) as int) as monum
		,bo.rec_delivery_date
		,cast(case 
			when lsc.maxscandt = ld.last_delivery 
				then date_part('day', bo.rec_delivery_date :: timestamp - ld.last_delivery :: timestamp)
			when ld.last_delivery > lsc.maxscandt 
				then date_part('day', bo.rec_delivery_date :: timestamp - ld.last_delivery :: timestamp)
			else 
				date_part('day', bo.rec_delivery_date :: timestamp - lsc.maxscandt :: timestamp)
		 end as int) as scans_since
		,cast(coalesce(sc.avgquantity, 0) as int) as tendayavgquantity
		,bo.model_used
		,coalesce(bo.actual_scans, 0) as actualscans
		,coalesce(bo.forecasted_scans, 0) as forecastedscans
		,coalesce(bo.trueup_adjustment_quantity, 0) as trueup
		,cast(coalesce(bo.mass_adjustment_quantity, 0) as int) as massadjustments
		,coalesce(bo.weight_data, 0) as spoilsadj
		,coalesce(bo.operator_adjustment_quantity, 0) as operatoradj
		,coalesce(bo.override_quantity, 0) as distroverride
		,coalesce(cf.conversion_units, 1) as casepack
		,coalesce(cr.residquantity, 0) as casepackres
		,cast(case
				when bo.override_quantity = 0 then coalesce(bo.actual_scans, 0) 
					+ coalesce(bo.forecasted_scans, 0) 
					+ coalesce(bo.mass_adjustment_quantity)
					+ coalesce(bo.weight_data, 0)
					+ coalesce(bo.trueup_adjustment_quantity, 0) 
					+ coalesce(cr.residquantity, 0)
				else 
					bo.override_quantity
		 end as int) as override_quantity
		,cast(case
			when bo.original_prop_order_quantity >= 0 
				then coalesce(bo.original_prop_order_quantity, 0)
			else 
				round(cast(bo.original_prop_order_quantity as numeric) / cast(coalesce(cf.conversion_units, 1) as numeric),0) *coalesce(cf.conversion_units, 1)
		 end as int) as original_prop_order_quantity
		,coalesce(bo.proposed_order_quantity, 0) as adjustedorder
		,cast(round((cast((coalesce(sc.avgquantity, 0) * 
			(date_part('day', bo.rec_delivery_date :: timestamp - p_order_gen_date :: timestamp))) + 
			coalesce(bo.actual_scans,0) as numeric)) / 
			cast(coalesce(cf.conversion_units, 1) as numeric), 0) * 
			coalesce(cf.conversion_units, 1) as int ) as mathorder
	  --,actualships
		,cast(date_part('day',  p_order_gen_date :: timestamp - ld.last_delivery :: timestamp) as int) as dayssincelast_delivery
		,coalesce(msq.maxqt, 0) as maxdelquantity
		,coalesce(t.inventory_change, 0) as inventorychange
	from 
		dbo.vw_base_orders bo
		inner join dbo.customer_distributor_dyad dy
			on dy.customer_distributor_dyad_id = bo.customer_distributor_dyad_id
		inner join 
		(
			select 
				 ldd.customer_store_distributor_triad_id
				,ldd.category_id
				,ldd.create_date
				,min(ldd.last_delivery) as last_delivery
			from 
				dbo.last_deliveries ldd
			where 
				cast(ldd.create_date as date) = p_order_gen_date
			group by 
				 ldd.customer_store_distributor_triad_id
				,ldd.category_id
				,ldd.create_date
		) ld
			on ld.customer_store_distributor_triad_id = bo.customer_store_distributor_triad_id
			and ld.category_id = bo.category_id
		left outer join dbo.conversion_factors cf
			on cf.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
			and p_order_gen_date between cf.effective_date and cf.expiry_date
		left outer join 
		(
			select 
				 customer_store_item_distributor_dyad_id
				,sum(residual_quantity) as residquantity
				,applied_date
			from 
				dbo.conversion_residual
			where 
				applied_date = p_order_gen_date
			group by 
				 customer_store_item_distributor_dyad_id
				,applied_date
		) cr 
			on cr.customer_store_item_distributor_dyad_id = bo.customer_store_item_distributor_dyad_id
		left outer join 
		(
			select 
				 tr.national_customer_id
				,max(s.transaction_date) as maxscandt
			from 
				dbo.oa_scans s
				inner join dbo.customer_store_item_triad tr
					on tr.customer_store_item_triad_id = s.customer_store_item_triad_id
			where 
				cast(s.create_date as date) <= p_order_gen_date
				and cast(s.create_date as date) > interval '-30 day' + p_order_gen_date
			group by 
				tr.national_customer_id
		) lsc 
			on lsc.national_customer_id = dy.national_customer_id
		left outer join 
		(
			select 
				 s.customer_store_item_triad_id
				,max(s.quantity) as maxqt
			from 
				dbo.shipments s
			where 
				s.ship_date < p_order_gen_date
			group by 
				s.customer_store_item_triad_id
		) msq
			on msq.customer_store_item_triad_id = bo.customer_store_item_triad_id
		left outer join 
		(
			select 
				 s.customer_store_item_triad_id
				,round(cast(sum(s.quantity) as numeric)/cast(10 as numeric),2) as avgquantity
			from 
				dbo.oa_scans s
			where 
				s.transaction_date between interval '-10 day' + p_order_gen_date and p_order_gen_date
			group by 
				s.customer_store_item_triad_id
		) sc
			on sc.customer_store_item_triad_id = bo.customer_store_item_triad_id
		left outer join temp_table_inv t
			on t.customer_store_item_triad_id = bo.customer_store_item_triad_id
	where 
		bo.create_date = p_order_gen_date;
		
	for rec in
		select
			 oa.base_order_id
			,oa.order_gen_date
			,oa.customer_store_distributor_triad_id
			,oa.national_customer_id
			,oa.category_id
			,oa.customer_store_item_triad_id
			,oa.last_delivery
			,oa.lastscandt
			,oa.dom
			,oa.dow
			,oa.monum
			,oa.rec_delivery_date
			,oa.scans_since
			,oa.tendayavgquantity
			,oa.model_used
			,oa.actualscans
			,oa.forecastedscans
			,oa.trueup
			,oa.massadjustments
			,oa.spoilsadj
			,oa.operatoradj
			,oa.distroverride
			,oa.casepack
			,oa.casepackres
			,oa.override_quantity
			,oa.original_prop_order_quantity
			,oa.adjustedorder
			,oa.mathorder
			,oa.dayssincelast_delivery
			,oa.maxdelquantity
			,oa.inventorychange
		from
			opr_adj oa
	loop
		return query
			select
				base_order_id
				,rec.order_gen_date
				,rec.customer_store_distributor_triad_id
				,rec.national_customer_id
				,rec.category_id
				,rec.customer_store_item_triad_id
				,rec.last_delivery
				,rec.lastscandt
				,rec.dom
				,rec.dow
				,rec.monum
				,rec.rec_delivery_date
				,rec.scans_since
				,rec.tendayavgquantity
				,rec.model_used
				,rec.actualscans
				,rec.forecastedscans
				,rec.trueup
				,rec.massadjustments
				,rec.spoilsadj
				,rec.operatoradj
				,rec.distroverride
				,rec.casepack
				,rec.casepackres
				,rec.override_quantity
				,rec.original_prop_order_quantity
				,rec.adjustedorder
				,rec.mathorder
				,rec.dayssincelast_delivery
				,rec.maxdelquantity
				,rec.inventorychange;
			
	end loop;

end;
$BODY$;