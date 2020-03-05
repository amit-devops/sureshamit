CREATE OR REPLACE FUNCTION public.udf_last_scheduled_delivery_date
(
	 p_del_schedule_id integer
	,p_from_date date 
)
    returns date
    language 'plpgsql'

    cost 100
    volatile 
as 
$body$
declare
	v_cur_day integer;
	v_ld integer;
	v_ldd date := null;
	v_i integer := 0;
begin

	drop table if exists sch;
	create temp table sch 
	(
		 id integer 
		,val boolean
	);
	
	insert into sch 
	(
		 id
		,val
	)
	select 
		 1
		,ds.sunday
	from 
		dbo.delivery_schedules ds  
	where 
		ds.delivery_schedule_id = p_del_schedule_id

	union 
	
	select 
		 2
		,ds.monday
	from 
		dbo.delivery_schedules ds  
	where 
		ds.delivery_schedule_id = p_del_schedule_id
	
	union 
	
	select 
		 3
		,ds.tuesday
	from 
		dbo.delivery_schedules ds  
	where 
		ds.delivery_schedule_id = p_del_schedule_id
	
	union 
	
	select 
		 4
		,ds.wednesday
	from 
		dbo.delivery_schedules ds  
	where 
		ds.delivery_schedule_id = p_del_schedule_id
	
	union 
	
	select 
		 5
		,thursday
	from 
		dbo.delivery_schedules ds  
	where 
		ds.delivery_schedule_id = p_del_schedule_id
	
	union 
	
	select 
		 6
		,friday
	from 
		dbo.delivery_schedules ds  
	where 
		ds.delivery_schedule_id = p_del_schedule_id
	
	union 
	
	select 
		 7
		,saturday
	from 
		dbo.delivery_schedules ds  
	where 
		ds.delivery_schedule_id = p_del_schedule_id;
	
	select (date_part('dow', p_from_date) + 1) into v_cur_day;

	if (select coalesce(max(id), 0) from sch where id <= v_cur_day and val = true) <> 0 
	then
		select coalesce(max(id), 0) into v_ld from sch s where s.id <= v_cur_day and s.val = true;
	else
		select coalesce(max(id), 0) into v_ld from sch s where s.val = true;
	end if;

	while v_ldd is null 
	loop
		if(select (date_part('dow', p_from_date - v_i * interval '1 day') + 1)) = v_ld
		then
			select (p_from_date - v_i * interval '1 day') into v_ldd;
		end if;
		v_i := v_i + 1;
	end loop;
	
	return v_ldd;
	
end;
$body$