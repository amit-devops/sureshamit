CREATE OR REPLACE FUNCTION public.usp_load_oa_cleanup
(
	p_run_date timestamp without time zone default (now() :: timestamp without time zone),
	p_master_distributor_id integer default 0
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
BEGIN

	if p_run_date is null then
		p_run_date := now() :: timestamp without time zone;
	end if;
	
	-- append conversion factors	
	insert into dbo.conversion_factors 
	(
		 customer_store_item_distributor_dyad_id
		,conversion_factor
		,conversion_units
	)
	select
		 csid.customer_store_item_distributor_dyad_id
		,x.conversion_factor
		,x.conversion_units
	from 
		dbo.customer_store_item_distributor_dyad csid
		inner join dbo.customer_store_item_triad csi 
			on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id 
		inner join 
		(
			select 
				 y.item_id
				,y.oa_distributor_id
				,y.conversion_factor
				,y.conversion_units
				,y.csdcount
			from
			(
				select
					 csi.item_id
					,csid.oa_distributor_id
					,cf.conversion_factor
					,cf.conversion_units
					,count(cf.customer_store_item_distributor_dyad_id) as csdcount
					,row_number() over (partition by csi.item_id, csid.oa_distributor_id order by count(*) desc) as seqnum
				from 
					dbo.conversion_factors cf
					inner join dbo.customer_store_item_distributor_dyad csid 
						on cf.customer_store_item_distributor_dyad_id = csid.customer_store_item_distributor_dyad_id
					inner join dbo.customer_store_item_triad csi 
						on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id
				where 
					p_run_date between cf.effective_date and cf.expiry_date
				group by 
					 csi.item_id
					,csid.oa_distributor_id
					,cf.conversion_factor
					,cf.conversion_units
			) y 
		where
			y.seqnum = 1
	) x 
		on csi.item_id = x.item_id 
		and csid.oa_distributor_id = x.oa_distributor_id
	inner join dbo.oa_distributors d 
		on d.oa_distributor_id = csid.oa_distributor_id 
		and x.oa_distributor_id = d.oa_distributor_id 
	inner join dbo.oa_master_distributor md 
		on d.oa_master_distributor_id = md.oa_master_distributor_id 
		and (md.oa_master_distributor_id = p_master_distributor_id or p_master_distributor_id = 0) 
	left outer join dbo.conversion_factors cf 
		on csid.customer_store_item_distributor_dyad_id = cf.customer_store_item_distributor_dyad_id 
	where 
		p_run_date between csid.effective_date and csid.expiry_date
		and cf.conversion_factor_id is null
	group by 
		 csid.customer_store_item_distributor_dyad_id
		,x.item_id
		,x.conversion_factor
		,x.conversion_units;

	-- append spoils adjustments
	Insert into dbo.spoils_adjustments 
	(
		 customer_store_item_triad_id
		,spoils_inc
		,spoils_dec
	)
	select 
		 tr.customer_store_item_triad_id
		,coalesce (y.inc, 4)
		,coalesce (y.decr, -2)
	from 
		dbo.customer_store_item_distributor_dyad csd 
		inner join dbo.customer_store_item_triad tr 
			on csd.customer_store_item_triad_id = tr.customer_store_item_triad_id and p_run_date between tr.effective_date and tr.expiry_date
		inner join dbo.items i 
			on i.item_id = tr.item_id 
			and p_run_date < i.expiry_date
		inner join dbo.oa_distributors d 
			on d.oa_distributor_id = csd.oa_distributor_id  
		inner join dbo.oa_master_distributor md 
			on d.oa_master_distributor_id = md.oa_master_distributor_id 
			and (md.oa_master_distributor_id = p_master_distributor_id or p_master_distributor_id = 0) 
		inner join 
		(
			select 
				 csi.item_id
				,csid.oa_distributor_id
				,max(sa.spoils_inc) as inc
				,max(sa.spoils_dec) as decr
			from 
				dbo.customer_store_item_triad csi 
				inner join dbo.customer_store_item_distributor_dyad csid 
					on csid.customer_store_item_triad_id = csi.customer_store_item_triad_id
				left outer join dbo.spoils_adjustments sa 
					on csi.customer_store_item_triad_id = sa.customer_store_item_triad_id and p_run_date between sa.effective_date and sa.expiry_date
				group by 
					 csi.item_id
					,csid.oa_distributor_id
		) y 
			on y.item_id = i.item_id 
			and d.oa_distributor_id = y.oa_distributor_id 
			and p_run_date between csd.effective_date and csd.expiry_date
		left outer join dbo.spoils_adjustments sa 
			on sa.customer_store_item_triad_id = csd.customer_store_item_triad_id
	where 
		sa.spoils_adjustment_id is null;

end;
$BODY$;
