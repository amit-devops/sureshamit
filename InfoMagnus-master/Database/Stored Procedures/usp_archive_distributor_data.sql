CREATE OR REPLACE FUNCTION public.usp_archive_distributor_data
(
	p_masterdistributorid integer
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

begin

	insert into archive.stg_oa_deliveries
	select 
		s.* 
	from 
		dbo.stg_oa_deliveries s 
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_oa_deliveries p 
		where 
			p.import_date = s.import_date 
			and p.import_file_name = s.import_file_name 
			and p.oa_master_distributor_id = p_masterdistributorid
	)
	and s.oa_master_distributor_id = p_masterdistributorid;
	
	insert into archive.stg_oa_delivery_schedule
	select 
		s.* 
	from 
		dbo.stg_oa_delivery_schedule s
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_oa_delivery_schedule p 
		where 
			p.import_date = s.import_date 
			and p.import_file_name = s.import_file_name 
			and p.oa_master_distributor_id = p_masterdistributorid
	)
	and s.oa_master_distributor_id = p_masterdistributorid;

	insert into archive.stg_oa_distributors
	select 
		s.* 
	from 
		dbo.stg_oa_distributors s
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_oa_distributors p 
		where 
			p.import_date = s.import_date 
			and p.import_file_name = s.import_file_name 
			and p.oa_master_distributor_id = p_masterdistributorid
	)
	and s.oa_master_distributor_id = p_masterdistributorid;

	insert into archive.stg_oa_items
	select 
		s.* 
	from 
		dbo.stg_oa_items s
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_oa_items p 
		where 
			p.import_date = s.import_date 
			and p.import_file_name = s.import_file_name 
			and p.oa_master_distributor_id = p_masterdistributorid
	)
	and s.oa_master_distributor_id = p_masterdistributorid;

	insert into archive.stg_oa_promotions
	select 
		s.* 
	from 
		dbo.stg_oa_promotions s
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_oa_promotions p 
		where 
			p.import_date = s.import_date 
			and p.import_file_name = s.import_file_name 
			and p.oa_master_distributor_id = p_masterdistributorid
	)
	and s.oa_master_distributor_id = p_masterdistributorid;
	
	insert into archive.stg_oa_store_routing
	select 
		s.* 
	from 
		dbo.stg_oa_store_routing s
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_oa_store_routing p 
		where 
			p.import_date = s.import_date 
			and p.import_file_name = s.import_file_name 
			and p.oa_master_distributor_id = p_masterdistributorid
	)
	and s.oa_master_distributor_id = p_masterdistributorid;

	insert into archive.stg_oa_stores
	select 
		s.* 
	from 
		dbo.stg_oa_stores s
	where not exists 
	(
		select 
			1 
		from 
			archive.stg_oa_stores p 
		where 
			p.import_date = s.import_date 
			and p.import_file_name = s.import_file_name 
			and p.oa_master_distributor_id = p_masterdistributorid
	)
	and s.oa_master_distributor_id = p_masterdistributorid;

end;
$BODY$;
