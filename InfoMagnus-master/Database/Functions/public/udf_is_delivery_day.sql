CREATE OR REPLACE FUNCTION public.udf_is_delivery_day(
	p_delivery_schedule_id integer,
	p_from_date date DEFAULT NULL::date)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE 
	c integer := (select date_part('dow', coalesce(p_from_date, now())) + 1 ::integer);
	thisday character varying(20);
	nextdel varchar(20);	
	--select thisday = Name from @days where id = @c 
	ct integer := 1;
	is_del integer := 0;
	

BEGIN 
	drop table if exists days;
	
	create temp table days 
	(
		id integer,
		name varchar(20),
		sched boolean default '0'
	);
	
	insert into days 
	(
		id,
		name
	)
	select 1, 'Sunday'
	union
	select 2, 'Monday'
	union
	select 3, 'Tuesday'
	union
	select 4, 'Wednesday'
	union
	select 5, 'Thursday'
	union
	select 6, 'Friday'
	union
	select 7, 'Saturday';
	
	select name into thisday from days where id = c;
	
	while ct <= 7
		loop 		
			update days set sched = 
				case ct
					when 1 then
					(select sunday from dbo.delivery_schedules where delivery_schedule_id = p_delivery_schedule_id)
					when 2 then
					(select monday from dbo.delivery_schedules where delivery_schedule_id = p_delivery_schedule_id)
					when 3 then
					(select tuesday from dbo.delivery_schedules where delivery_schedule_id = p_delivery_schedule_id)
					when 4 then
					(select wednesday from dbo.delivery_schedules where delivery_schedule_id = p_delivery_schedule_id)
					when 5 then
					(select thursday from dbo.delivery_schedules where delivery_schedule_id = p_delivery_schedule_id)
					when 6 then
					(select friday from dbo.delivery_schedules where delivery_schedule_id = p_delivery_schedule_id)
					when 7 then
					(select saturday from dbo.delivery_schedules where delivery_schedule_id = p_delivery_schedule_id)
				end	
				where id = ct;
			ct := ct + 1;
			
		end loop;
	
	if exists (select name from days where id = c and sched <> '0' ) then
				is_del := 1;
	end if;		
	return is_del;
END
$BODY$;
