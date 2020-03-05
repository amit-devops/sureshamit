CREATE OR REPLACE FUNCTION public.udf_predictions_with_work_orders
(
    p_run_date date,
    p_prediction_date date
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS 
$BODY$
    declare v_max_work_order_id integer;
    declare v_max_work_order_item_id integer;

BEGIN

	if p_run_date is null then
		p_run_date := now() :: date;
	end if;

	if p_prediction_date is null then
		p_prediction_date := p_run_date;
	end if;

	create table if not exists dbo.prediction_schedules -- this table will be truncated one data is dumped to control database.
	(
		prediction_id      serial,
		run_date           date not null,
		prediction_date    date null,
		constraint run_predict_date unique (run_date, prediction_date)
	);

	if not exists(select 1 from dbo.prediction_schedules where run_date = p_run_date and prediction_date = p_prediction_date) then
	   insert into dbo.prediction_schedules
		(
			 run_date
			,prediction_date
		)
		values
		(
			 p_run_date
			,p_prediction_date
		);
	end if;

	create table if not exists dbo.work_order_items
	(
		oa_master_distributor_id integer,
		oa_distributor_id integer,
		prediction_id integer,
		national_customer_id integer,
		oa_store_id integer,
		customer_store_distributor_triad_id integer,
		category_id integer,
		rec_delivery_date date,
		run_date date,
		prediction_date date,
		create_date timestamp without time zone,
		work_order_id integer,
		work_order_item_id integer,
		work_group_id varchar(15), -- persistent identifier for a store + distributor + category_id
		constraint unique_csdtid_catid_predictid unique (customer_store_distributor_triad_id, category_id, prediction_id)
	);

	drop table if exists work_order_items_temp;

	create temp table work_order_items_temp
	(
		oa_master_distributor_id integer,
		oa_distributor_id integer,
		prediction_id integer,
		national_customer_id integer,
		oa_store_id integer,
		customer_store_distributor_triad_id integer,
		category_id integer,
		rec_delivery_date date,
		run_date date,
		prediction_date date,
		create_date timestamp without time zone,
		work_order_id integer,
		work_order_item_id integer,
		work_group_id varchar(15)
	);

	insert into work_order_items_temp
	(
		oa_master_distributor_id,
		oa_distributor_id,
		prediction_id,
		national_customer_id,
		oa_store_id,
		customer_store_distributor_triad_id,
		category_id,
		rec_delivery_date,
		run_date,
		prediction_date,
		create_date,
		work_order_id,
		work_order_item_id,
		work_group_id
	)
	select
		swo.oa_master_distributor_id,
		swo.oa_distributor_id,
		ps.prediction_id,
		swo.national_customer_id,
		swo.oa_store_id,
		swo.customer_store_distributor_triad_id,
		swo.category_id,
		swo.rec_delivery_date,
		p_run_date,
		p_prediction_date,
		swo.create_date,
		dense_rank() over (order by swo.national_customer_id, swo.oa_distributor_id) as work_order_id,
		dense_rank() over (order by swo.oa_distributor_id, swo.oa_store_id, swo.category_id) as work_order_item_id,
		cast(swo.oa_store_id as varchar(10)) || '/' || cast(swo.category_id as varchar(5)) as work_group_id
	from
		public.udf_store_with_orders(p_prediction_date) swo
		inner join dbo.prediction_schedules ps
			on ps.run_date = p_run_date
			and ps.prediction_date = p_prediction_date;

	if exists (select 1 from dbo.work_order_items where prediction_date = p_prediction_date and run_date = p_run_date) then
		delete from dbo.work_order_items
		where prediction_date = p_prediction_date
		and run_date = p_run_date;
	end if;

	v_max_work_order_id = (select max(work_order_id) from dbo.work_order_items);
	v_max_work_order_item_id = (select max(work_order_item_id) from dbo.work_order_items);

	if v_max_work_order_id is null then
		v_max_work_order_id = 0;
	end if;

	if v_max_work_order_item_id is null then
		v_max_work_order_item_id = 0;
	end if;

	insert into dbo.work_order_items
	(
		oa_master_distributor_id,
		oa_distributor_id,
		prediction_id,
		national_customer_id,
		oa_store_id,
		customer_store_distributor_triad_id,
		category_id,
		rec_delivery_date,
		run_date,
		prediction_date,
		create_date,
		work_order_id,
		work_order_item_id,
		work_group_id
	)
	select
		oa_master_distributor_id,
		oa_distributor_id,
		prediction_id,
		national_customer_id,
		oa_store_id,
		customer_store_distributor_triad_id,
		category_id,
		rec_delivery_date,
		run_date,
		prediction_date,
		create_date,
		v_max_work_order_id + work_order_id,
		v_max_work_order_item_id + work_order_item_id,
		work_group_id
	from
		work_order_items_temp;
    END

$BODY$;