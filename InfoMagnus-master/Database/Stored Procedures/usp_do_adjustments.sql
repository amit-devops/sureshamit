CREATE OR REPLACE FUNCTION public.usp_do_adjustments
(
	 p_work_order_id integer
	,p_run_date timestamp without time zone default (now() :: timestamp without time zone)
)
    RETURNS void
    LANGUAGE 'plpgsql'

    cost 100
    volatile 
AS $BODY$

declare 
	v_i int := 1; 
	v_ii int := 1; 
	v_sql text;

begin

	drop table if exists v_gran;
	create temp table v_gran
	(
		 id serial
		,granularity_id integer
		,apply_id_name varchar(200)
		,order_index integer
	);

	insert into v_gran
	(
		 granularity_id
		,apply_id_name
		,order_index
	)
	select distinct 
		 granularity_id
		,apply_id_name
		,order_index 
	from 
		dbo.adjustment_granularity
	order by 
		order_index;

	drop table if exists temp_adjustments_staging;

    create temp table temp_adjustments_staging as
	select 
		 gran.apply_id_name
		,mat.adjustment_type_id
		,mat.apply_id
		,mat.adjustment_value 
	from 
		dbo.mass_adjustments_table mat
		inner join dbo.adjustment_types aty 
			on aty.adjustment_type_id = mat.adjustment_type_id
		inner join dbo.adjustment_granularity gran 
			on gran.granularity_id = mat.granularity_id
	where mat.expiry_date >= p_run_date
		and mat.begin_date <= p_run_date
		and aty.expiry_date >= p_run_date
		and gran.expiry_date >= p_run_date;

	delete from 
		dbo.adjustments_calculation a
	using
		 dbo.base_order b
		,dbo.work_order_items woi
	where 
		 b.base_order_id = a.base_order_id
		and b.work_group_id = woi.work_group_id
		and b.work_order_id = p_work_order_id
		and cast(create_date as date) = cast(p_run_date as date);

	drop table if exists temp_adjustmentscalc;
	
	create temp table temp_adjustmentscalc as
	select 
		 bo.base_order_id
		,x.adjustment_type_id
		,cast(0.0 as double precision) as adjustment_value
		,p_run_date as create_date
	from 
		dbo.base_orders bo
		inner join dbo.work_order_items woi
			on bo.work_group_id = woi.work_group_id
			and woi.work_order_id = p_work_order_id
	inner join 
	(
		select distinct 
			aty.adjustment_type_id
		from 
			dbo.mass_adjustments_table mat 
			inner join dbo.adjustment_types aty 
				on aty.adjustment_type_id = mat.adjustment_type_id
	) x on 1 = 1
	where 
		cast(bo.create_date as date) = cast(p_run_date as date);
	
	drop table if exists v_at;
	
	create temp table v_at  
	(
		 id serial
		,adjustment_type_id integer
	);
		
	insert into v_at 
	(
		adjustment_type_id
	)
	select distinct 
		adjustment_type_id
	from 
		temp_adjustmentsstaging ua;
	
	while v_i <= (select max(id) from v_at)
	loop
		v_ii := 1;
		while v_ii <= (select max(id) from v_gran)
		loop
			v_sql := '
				update temp_adjustmentscalc as ac
					set adjustment_value = z.adjqty
				using 
				(
					select 
						 bo.base_order_id
						,ua.adjustment_type_id
						,ua.adjustment_value
						,bo.base_order * ua.adjustment_value as adjqty
					from 
						temp_adjustmentsstaging ua
						inner join dbo.base_orders bo 
							on bo.' || (select apply_id_name from v_gran where id = v_ii) || ' = ua.apply_id
				where 
					ua.apply_id_name = '''|| (select apply_id_name from v_gran where id = v_ii) ||'''
					and ua.adjustment_type_id = ' || (select cast(adjustment_type_id as varchar(200)) from v_at where id = v_i) || '
					and cast(bo.create_date as date) = ''' || cast(p_run_date as varchar(20)) || '''
				) z
				where 
					z.base_order_id = ac.base_order_id
					and z.adjustment_type_id = ac.adjustment_type_id
					and cast(ac.create_date as date) = ''' || cast(p_run_date as varchar(20)) || '''';
			execute (v_sql);
		
			v_ii := v_ii + 1;
		end loop;
		v_i := v_i + 1;
	end loop;

	insert into dbo.adjustments_calculation 
	(
		 base_order_id
		,adjustment_type_id
		,adjustment_value
		,create_date
	)
	select 
		 base_order_id
		,adjustment_type_id
		,adjustment_value
		,create_date
	from 
		temp_adjustmentscalc
	where 
		adjustment_value > 0;
end;
$body$;
