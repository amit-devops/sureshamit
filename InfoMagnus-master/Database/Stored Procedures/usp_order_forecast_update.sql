CREATE OR REPLACE FUNCTION public.usp_order_forecast_update
(
	 p_work_order_id integer
	,p_source varchar(20) default 'Python'
	,p_run_date date default (now() :: date)
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

declare
		v_ii int := 1;

begin

	-- -- get the required work_group_ids corresponding to work_order_id
	-- drop table if exists work_group_ids;
	-- create temp table work_group_ids
	-- (
		-- work_group_id varchar(15)
	-- )
		
	-- insert into work_group_ids
	-- (
		-- work_group_id
	-- )
	-- select 
		-- work_group_id 
	-- from 
		-- dbo.work_order_items
	-- where
		-- work_order_id = p_work_order_id;

	if p_run_date is null then
		p_run_date := now() :: date;
	end if;

	-- insert the new records for Python
	if lower(p_source) = 'python'
		then
			-- Clean out existing records
			delete 
				from dbo.order_forecasts as f 
			using 
				 dbo.customer_store_item_distributor_dyad as csi
				,dbo.oa_distributors as d
				,dbo.work_order_items as woi
			where 
				csi.customer_store_item_triad_id = f.customer_store_item_triad_id 
				and d.oa_distributor_id = csi.oa_distributor_id 
				and f.run_date = p_run_date
				and f.forecast_source = p_source
				and (woi.work_group_id = csi.work_group_id 
				and woi.work_order_id = p_work_order_id);
			
			insert into dbo.order_forecasts 
			(
				 customer_store_item_triad_id
				,units
				,forecast_date
				,forecast_source
				,run_date
				,create_date
			)
			select 
				 ofp.customer_store_item_triad_id
				,ofp.units
				,cast(ofp.forecast_date as date)
				,ofp.forecast_source
				,p_run_date
				,now() :: timestamp without time zone
			from 
				dbo.stg_order_forecasts ofp
				inner join dbo.customer_store_item_distributor_dyad csi 
					on csi.customer_store_item_triad_id = ofp.customer_store_item_triad_id
				inner join dbo.work_order_items woi
					on woi.work_group_id = csi.work_group_id 
					and woi.work_order_id = p_work_order_id
				inner join dbo.oa_distributors d 
					on d.oa_distributor_id = csi.oa_distributor_id
			where 
				ofp.run_date = p_run_date;
		end if;

	-- insert the new records for moving average
	if lower(p_source) = 'movingaverage'
		then
			delete 
				from dbo.order_forecasts as f 
			using 
				 dbo.customer_store_item_distributor_dyad as csi
				,dbo.oa_distributors as d
				,dbo.work_order_items as woi
			where 
				csi.customer_store_item_triad_id = f.customer_store_item_triad_id
				and d.oa_distributor_id = csi.oa_distributor_id
				and f.forecast_date between p_run_date + interval '1 day' and p_run_date + interval '7 day'
				and lower(f.forecast_source) = 'movingaverage'
				and (woi.work_group_id = csi.work_group_id and woi.work_order_id = p_work_order_id);
			
			while v_ii <= 7
				loop
					insert into dbo.order_forecasts 
					(
						 customer_store_item_triad_id
						,units
						,forecast_date
						,forecast_source
						,run_date
						,create_date
					)
					select distinct
						 csi.customer_store_item_triad_id
						,case when cast(round(case date_part('dow', v_ii * interval '1 day' + p_run_date)
										when 0 then avs.sunday
										when 1 then avs.monday
										when 2 then avs.tuesday
										when 3 then avs.wednesday
										when 4 then avs.thursday
										when 5 then avs.friday
										when 6 then avs.saturday
									end, 0) as int) > 0 then
									cast(round(case date_part('dow',  v_ii * interval '1 day' + p_run_date)
										when 0 then avs.sunday
										when 1 then avs.monday
										when 2 then avs.tuesday
										when 3 then avs.wednesday
										when 4 then avs.thursday
										when 5 then avs.friday
										when 6 then avs.saturday
									end, 0) as int)
								else 0
						 end as units			
						,v_ii * interval '1 day' + p_run_date
						,'MovingAverage'
						,p_run_date
						,now()::timestamp without time zone
					from 
						dbo.avg_scans_customer_store_item_wk_day avs 
						inner join dbo.customer_store_item_triad csi 
							on csi.customer_store_item_triad_id = avs.customer_store_item_triad_id 
							and (p_run_date between csi.effective_date and csi.expiry_date)
						inner join dbo.work_order_items woi
							on woi.work_group_id = csi.work_group_id
							and woi.work_order_id = p_work_order_id
						inner join dbo.customer_store_item_distributor_dyad csd
							on csd.customer_store_item_triad_id = csi.customer_store_item_triad_id 
							and (p_run_date between csd.effective_date and csd.expiry_date)
						inner join dbo.oa_distributors d 
							on d.oa_distributor_id = csd.oa_distributor_id
						inner join dbo.customer_distributor_dyad cd 
							on cd.oa_distributor_id = d.oa_distributor_id 
							and cd.national_customer_id = csi.national_customer_id
						inner join dbo.distributor_items di 
							on di.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
							and di.item_id = csi.item_id	
						inner join dbo.customer_store_distributor_triad csdt 
							on csdt.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
							and csdt.oa_store_id = csi.oa_store_id;
							
					v_ii := v_ii + 1;
				end loop;
		end if;
	
END;
$BODY$;