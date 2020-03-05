CREATE OR REPLACE FUNCTION public.usp_refresh_item_averages
(
	 p_work_order_id integer
	--,p_national_customer_id integer default 24
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

    declare 
		v_su int := 0; 
		v_m int := 0; 
		v_tu int := 0; 
		v_we int := 0; 
		v_th int := 0; 
		v_fri int := 0; 
		v_sa int := 0;
		v_dstart date;
		v_dend date;
begin	

	delete from 
		dbo.avg_scans_customer_store_item_wk_day a
	using 
		 dbo.customer_store_item_triad csi 
		,dbo.work_order_items woi
	where 
		csi.customer_store_item_triad_id = a.customer_store_item_triad_id 
		and woi.work_group_id = csi.work_group_id
		and woi.work_order_id = p_work_order_id;
		--and cast(last_update_date as date) < CAST(getdate() as DATE) 
		--and csi.national_customer_id = p_national_customer_id;
	
	drop table if exists v_I;	
	create temp table v_I
	(
		 customer_store_item_triad_id integer
		,transaction_date date
		,wk_day int
		,quantity Int
	);

	--select public.usp_refresh_retailer_last_scan_date(NULL)

	insert into v_I
	select 
		 s.customer_store_item_triad_id
		,s.transaction_date
		,(extract(dow from s.transaction_date) + 1):: integer as wk_day
		,s.quantity 
	from 
		dbo.oa_scans s 
		inner join dbo.retailer_last_scan_date r 
			on s.customer_store_item_triad_id = r.customer_store_item_triad_id
		inner join dbo.customer_store_item_triad csi 
			on csi.customer_store_item_triad_id = s.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csi.work_group_id
			and woi.work_order_id = p_work_order_id
	where 
		s.transaction_date >= r.max_date - interval '60 day';
		--and csi.national_customer_id = p_national_customer_id;
	
	drop table if exists v_ts;
	create temp table v_ts 
	(
		 customer_store_item_triad_id int
		,std_dev_min numeric(8,2)
		,std_dev_max numeric(8,2)
	);
 
	insert into v_ts 
	select 
		 s.customer_store_item_triad_id
		,avg(s.quantity) - (stddev_pop(s.quantity) * 2) as std_dev_min
		,avg(s.quantity) + (stddev_pop(s.quantity) * 2) as std_dev_max
	from 
		dbo.oa_scans s
		inner join dbo.retailer_last_scan_date r 
			on s.customer_store_item_triad_id = r.customer_store_item_triad_id
		inner join dbo.customer_store_item_triad csi
			on csi.customer_store_item_triad_id = s.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csi.work_group_id
			and woi.work_order_id = p_work_order_id
	where 
		s.transaction_date >= r.max_date - interval '60 day' 
		--and csi.national_customer_id = p_national_customer_id
	group by 
		s.customer_store_item_triad_id; 

	select 
		min(transaction_date) 
	into 
		v_dstart 
	from 
		v_I;
	
	select 
		max(transaction_date) 
	into 
		v_dend 
	from 
		v_I;
	
	while v_dstart <= v_dend
		loop
			if date_part('dow', v_dstart) = 0
				then v_su := v_su + 1; end if;
			if date_part('dow', v_dstart) = 1
				then v_m := v_m + 1; end if;
			if date_part('dow', v_dstart) = 2
				then v_tu := v_tu + 1; end if;
			if date_part('dow', v_dstart) = 3
				then v_we := v_we + 1; end if;
			if date_part('dow', v_dstart) = 4
				then v_th := v_th + 1; end if;
			if date_part('dow', v_dstart) = 5
				then v_fri := v_fri + 1; end if;
			if date_part('dow', v_dstart) = 6
				then v_sa := v_sa + 1; end if;
				
			v_dstart := v_dstart + interval '1 day';
		end loop;
	
	drop table if exists v_avgt;
	create temp  table v_avgt
	(
		 customer_store_item_triad_id int null
		,sunday numeric(8, 2) null
		,monday numeric(8, 2) null
		,tuesday numeric(8, 2) null
		,wednesday numeric(8, 2) null
		,thursday numeric(8, 2) null
		,friday numeric(8, 2) null
		,saturday numeric(8, 2) null	
		,period_used varchar(20)
	);

	insert into v_avgt
	select 
		 t.customer_store_item_triad_id
		,sum(case when t.wk_day = 1 then t.quantity else 0 end) as sunday
		,sum(case when t.wk_day = 2 then t.quantity else 0 end) as monday
		,sum(case when t.wk_day = 3 then t.quantity else 0 end) as tuesday
		,sum(case when t.wk_day = 4 then t.quantity else 0 end) as wednesday
		,sum(case when t.wk_day = 5 then t.quantity else 0 end) as thursday
		,sum(case when t.wk_day = 6 then t.quantity else 0 end) as friday
		,sum(case when t.wk_day = 7 then t.quantity else 0 end) as saturday
		,'60Day'
	from
		v_I t
	group by 
		t.customer_store_item_triad_id;
						   
	drop table if exists avgs;
    create temp table avgs
	(
		 customer_store_item_triad_id integer
		,su	numeric(8, 2)
		,mo	numeric(8, 2)
		,tu	numeric(8, 2)
		,we	numeric(8, 2)
		,th	numeric(8, 2)
		,fr	numeric(8, 2)
		,sa	numeric(8, 2)
		,week_avg	numeric(8, 2)
		,std_dev_min numeric(8, 2)
		,std_dev_max numeric(8, 2)
		,period_used varchar(20)
	);
	insert into avgs					  
	select 
		 a.customer_store_item_triad_id
		,cast(a.sunday/v_su as numeric(8,2)) as su
		,cast(a.monday/v_m as numeric(8,2)) as mo 
		,cast(a.tuesday/v_tu as numeric(8,2)) as tu
		,cast(a.wednesday/v_we as numeric(8,2)) as we
		,cast(a.thursday/v_th as numeric(8,2)) as th
		,cast(a.friday/v_fri as numeric(8,2)) as fr 
		,cast(a.saturday/v_sa as numeric(8,2)) as sa
		,(
			cast(a.sunday/v_su as numeric(8,2)) + 
			cast(a.monday/v_m as numeric(8,2)) + 
			cast(a.tuesday/v_tu as numeric(8,2)) + 
			cast(a.wednesday/v_we as numeric(8,2)) + 
			cast(a.thursday/v_th as numeric(8,2)) +
			cast(a.friday/v_fri as numeric(8,2)) + 
			cast(a.saturday/v_sa as numeric(8,2))
		 ) as week_avg
		,t.std_dev_min as std_dev_min
		,t.std_dev_max as std_dev_max
		,a.period_used as period_used
	 from 
		v_avgt a 
		inner join v_ts t 
			on t.customer_store_item_triad_id = a.customer_store_item_triad_id;

	delete from v_I 
	where transaction_date < v_dend - interval '14 day' ;
	
	v_dstart := v_dend - interval '14 day';

	v_su := 0;
	v_m := 0;
	v_tu := 0;
	v_we := 0;
	v_th := 0;
	v_fri := 0;
	v_sa := 0;

	while v_dstart <= v_dend
		loop
			if date_part('dow', v_dstart) = 0
				then v_su := v_su + 1; end if;
			if date_part('dow', v_dstart) = 1
				then v_m := v_m + 1; end if;
			if date_part('dow', v_dstart) = 2
				then v_tu := v_tu + 1; end if;
			if date_part('dow', v_dstart) = 3
				then v_we := v_we + 1; end if;
			if date_part('dow', v_dstart) = 4
				then v_th := v_th + 1; end if;
			if date_part('dow', v_dstart) = 5
				then v_fri := v_fri + 1; end if;
			if date_part('dow', v_dstart) = 6
				then v_sa := v_sa + 1; end if;
				
			v_dstart := v_dstart + interval '1 day' ;
		end loop;

	truncate table v_avgt;
	Insert into v_avgt
	select 
		 t.customer_store_item_triad_id
		,sum(case when t.wk_day = 1 then T.quantity else 0 end) as sunday
		,sum(case when t.wk_day = 2 then T.quantity else 0 end) as monday
		,sum(case when t.wk_day = 3 then T.quantity else 0 end) as tuesday
		,sum(case when t.wk_day = 4 then T.quantity else 0 end) as wednesday
		,sum(case when t.wk_day = 5 then T.quantity else 0 end) as thursday
		,sum(case when t.wk_day = 6 then T.quantity else 0 end) as friday
		,sum(case when t.wk_day = 7 then T.quantity else 0 end) as saturday
		,'14Day'
	from 
		v_I t
	group by 
		t.customer_store_item_triad_id;

	insert into avgs
	select 
		 a.customer_store_item_triad_id
		,cast(a.sunday/v_su as numeric(8,2)) as su
		,cast(a.monday/v_m as numeric(8,2)) as mo 
		,cast(a.tuesday/v_tu as numeric(8,2)) as tu 
		,cast(a.wednesday/v_we as numeric(8,2)) as we 
		,cast(a.thursday/v_th as numeric(8,2)) as th
		,cast(a.friday/v_fri as numeric(8,2)) as fr 
		,cast(a.saturday/v_sa as numeric(8,2)) as sa
		,(
			cast(a.sunday/v_su as numeric(8,2)) + 
			cast(a.monday/v_m as numeric(8,2)) + 
			cast(a.tuesday/v_tu as numeric(8,2)) + 
			cast(a.wednesday/v_we as numeric(8,2)) + 
			cast(a.thursday/v_th as numeric(8,2)) +
			cast(a.friday/v_fri as numeric(8,2)) + 
			cast(a.saturday/v_sa as numeric(8,2))
		 ) as week_avg
		,t.std_dev_min
		,t.std_dev_max
		,a.period_used
	 from 
		v_avgt a 
		inner join v_ts t 
			on t.customer_store_item_triad_id = a.customer_store_item_triad_id;
 
	 insert into dbo.avg_scans_customer_store_item_wk_day 
	 (
		 customer_store_item_triad_id
		,sunday
		,monday 
		,tuesday 
		,wednesday 
		,thursday 
		,friday
		,saturday 
		,avg_weekly 
		,std_dev_min 
		,std_dev_max
		,last_update_date
	)
	 select 
		 a.customer_store_item_triad_id
		,sum
		(
			case when a.period_used = '60Day' 
				then a.su * 7
			else a.su * 3 end
		)/10 as su
		,sum
		(
			case when a.period_used = '60Day' 
				then a.mo * 7
			else a.mo * 3 end
		)/10 as mo
		,sum
		(
			case when a.period_used = '60Day' 
				then a.tu * 7
			else a.tu * 3 end
		)/10 as tu
		,sum
		(
			case when a.period_used = '60Day' 
				then a.we * 7
			else a.we * 3 end
		)/10 as we
		,sum
		(
			case when a.period_used = '60Day' 
				then a.th * 7
			else a.th * 3 end
		)/10 as th
		,sum
		(
			case when a.period_used = '60Day' 
				then a.fr * 7
			else a.fr * 3 end
		)/10 as fr
		,sum
		(
			case when a.period_used = '60Day' 
				then a.sa * 7
			else a.sa * 3 end
		)/10 as sa 
		,null
		,a.std_dev_min 
		,a.std_dev_max
		,now() :: timestamp without time zone
	 from 
		avgs a 
	 group by 
		 a.customer_store_item_triad_id
		,a.std_dev_min 
		,a.std_dev_max;

	update 
		dbo.avg_scans_customer_store_item_wk_day
	set 
		avg_weekly = a.sunday + a.monday + a.tuesday + a.wednesday + a.thursday + a.friday + a.saturday 
	from
		dbo.avg_scans_customer_store_item_wk_day a
		inner join dbo.customer_store_item_triad csit
			on a.customer_store_item_triad_id = csit.customer_store_item_triad_id
		inner join dbo.work_order_items woi
			on woi.work_group_id = csit.work_group_id
			and woi.work_order_id = p_work_order_id
	where 
		a.avg_weekly is null;

end;
$BODY$;