CREATE OR REPLACE FUNCTION public.usp_load_distributor
(
	 p_master_distributor_id integer
	,p_run_date timestamp without time zone default(now() :: timestamp without time zone)
)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
		v_master_distributor_name varchar(200);
BEGIN
		select
		    m.master_distributor_name
        into
            v_master_distributor_name
        from
            dbo.oa_master_distributor m
        where
            m.oa_master_distributor_id = p_master_distributor_id;

		if p_run_date is null then
			p_run_date := now() :: timestamp without time zone;
		end if;

		-- archive the data
		perform public.usp_archive_distributor_data(p_master_distributor_id);

		-- distributors
		insert into dbo.oa_distributors
		(
			 distributor_name
			,distributor_number
			,oa_master_distributor_id
			,oa_contact_id
			,create_user
			,create_date
			,expiry_date
			,effective_date
			,short_name
		)
		select distinct
			 od.distributor_name
			,od.distributor_number
			,od.oa_master_distributor_id
			,cast(null as integer)
			,v_master_distributor_name || '_load'
			,now()
			,'9999-12-31' :: timestamp without time zone
			,now()
			,''
		from
		    dbo.stg_oa_distributors od
		    left outer join dbo.oa_distributors d
		        on lower(d.distributor_name) = lower(od.distributor_name)
	    where
	        d.oa_distributor_id is null
			and od.oa_master_distributor_id = p_master_distributor_id;

		-- customer_distributor_dyad
		insert into dbo.customer_distributor_dyad
		(
			 national_customer_id
			,oa_distributor_id
			,create_user
			,create_date
			,effective_date
			,expiry_date
		)
		select distinct
			 os.national_customer_id
			,d.oa_distributor_id
			,v_master_distributor_name || '_load'
			,now()
			,now()
			,'9999-12-31' :: timestamp without time zone
		from
		    dbo.stg_oa_stores os
		    inner join dbo.oa_distributors d
		        on lower(d.distributor_name) = lower(os.distributor_name)
		    left outer join dbo.customer_distributor_dyad cdd
		        on cdd.national_customer_id = os.national_customer_id
		        and cdd.oa_distributor_id = d.oa_distributor_id
		where
		    cdd.customer_distributor_dyad_id is null
			and os.oa_master_distributor_id = p_master_distributor_id;

		-- stores
		insert into dbo.oa_stores
		(
			 national_customer_id
			,pmi_dev_store_id
			,store_number
			,address_1
			,address_2
			,store_city
			,store_state
			,store_zip
			,create_user
			,create_date
			,effective_date
			,expiry_date
			,phone_number
		)
		select distinct
			 os.national_customer_id
			,cast(null as integer)
			,os.store_number
			,os.address_1
			,os.address_2
			,os.store_city
			,os.store_state
			,os.store_zip
			,v_master_distributor_name || '_load'
			,now()
			,now()
			,'9999-12-31' :: timestamp without time zone
			,''
		from
		    dbo.stg_oa_stores os
		    inner join dbo.oa_distributors d
		        on lower(d.distributor_name) = lower(os.distributor_name)
		    left outer join dbo.oa_stores s
		        on s.national_customer_id = os.national_customer_id
		        and s.store_number = os.store_number
		where
		    s.oa_store_id is null
			and os.oa_master_distributor_id = p_master_distributor_id;

		-- customer_store_distributor_triad
		insert into dbo.customer_store_distributor_triad
		(
			 customer_distributor_dyad_id
			,oa_store_id
			,distributor_store_number
			,distributor_store_name
			,store_classification
			,distributor_route_number
			,create_user
			,create_date
			,effective_date
			,expiry_date
		)
		select distinct
			 cdd.customer_distributor_dyad_id
			,s.oa_store_id
			,os.distributor_store_number
			,os.store_name
			,os.store_classification
			,min(osd.distributor_route_number)
			,v_master_distributor_name || '_load'
			,now()
			,now()
			,'9999-12-31' :: timestamp without time zone
		from 
			dbo.stg_oa_stores os
			inner join dbo.stg_oa_store_routing osd 
				on lower(osd.distributor_name) = lower(os.distributor_name) 
				and osd.distributor_store_number = os.distributor_store_number
			inner join dbo.oa_distributors d 
				on lower(d.distributor_name) = lower(os.distributor_name)
			inner join dbo.oa_stores s 
				on s.national_customer_id = os.national_customer_id 
				and s.store_number = os.store_number
			inner join dbo.customer_distributor_dyad cdd 
				on cdd.national_customer_id = os.national_customer_id 
				and cdd.oa_distributor_id = d.oa_distributor_id
			left outer join dbo.customer_store_distributor_triad csdt 
				on csdt.customer_distributor_dyad_id = cdd.customer_distributor_dyad_id 
				and csdt.oa_store_id = s.oa_store_id
		where 
			csdt.customer_store_distributor_triad_id is null
			and os.oa_master_distributor_id = p_master_distributor_id
		group by 
			 cdd.customer_distributor_dyad_id
			,s.oa_store_id
			,os.distributor_store_number
			,os.store_name
			,os.store_classification;

		-- items
		insert into dbo.items
		(
			 national_customer_id
			,sku
			,upc
			,category_id
			,create_user
			,create_date
			,expiry_date
			,units_per_case
			,is_case_order
			,package_id
			,retailer_item_description
		)
		select distinct
			 oi.national_customer_id
			,c.sku
			,oi.upc
			,ct.category_id
			,v_master_distributor_name || '_load'
			,now()
			,'9999-12-31' :: timestamp
			,oi.units_per_case
			,oi.is_case_order
			,p.package_id
			,oi.item_description
		from
		    dbo.stg_oa_items oi
		    left outer join dbo.oa_sku_upc_conversion c
		        on cast(c.upc as bigint) = cast(oi.upc as bigint)
		        and c.national_customer_id = oi.national_customer_id and p_run_date <= c.expiry_date
		    left outer join dbo.categories ct
		        on lower(ct.category_name) = lower(oi.category_name)
		    left outer join dbo.package p
		        on lower(p.package) = lower(oi.size_type)
		    left outer join dbo.items i
		            on cast(i.upc as bigint) = cast(oi.upc as bigint)
		            and i.national_customer_id = oi.national_customer_id
		where
		    i.item_id is null
			and oi.oa_master_distributor_id = p_master_distributor_id;

		-- distributor_items
		insert into dbo.distributor_items
		(
			 customer_distributor_dyad_id
			,item_id
			,distributor_product_code
			,create_user
			,create_date
			,effective_date
			,expiry_date
			,distributor_item_description
			,sub_category_id
		)
		select distinct
			 cdd.customer_distributor_dyad_id
			,i.item_id
			,oi.distributor_item_number
			,v_master_distributor_name || '_load'
			,now()
			,now()
			,'9999-12-31' :: timestamp
			,oi.item_description
			,cast(null as integer)
		from
		    dbo.stg_oa_items oi
		    inner join dbo.oa_distributors d
		        on lower(d.distributor_name) = lower(oi.distributor_name)
		    inner join dbo.customer_distributor_dyad cdd
		        on cdd.national_customer_id = oi.national_customer_id
		        and cdd.oa_distributor_id = d.oa_distributor_id
		    inner join dbo.items i
		        on cast(i.upc as bigint) = cast(oi.upc as bigint)
		        and i.national_customer_id = cdd.national_customer_id
		    left outer join dbo.distributor_items di
		        on di.customer_distributor_dyad_id = cdd.customer_distributor_dyad_id
		        and di.item_id = i.item_id
		where
		    di.item_id is null
			and oi.oa_master_distributor_id = p_master_distributor_id;

		-- customer_store_item_triad
		with cte_store_ids
		(
			select distinct
				oa_store_id
			from
				dbo.customer_store_distributor_triad
			where
				p_run_date < expiry_date
		)
		insert into dbo.customer_store_item_triad
		(
			 national_customer_id
			,oa_store_id
			,item_id
			,create_user
			,create_date
			,effective_date
			,expiry_date
			,work_group_id
		)
		select distinct
			 oi.national_customer_id
			,coalesce(s.oa_store_id, ss.oa_store_id)
			,i.item_id
			,v_master_distributor_name || '_load'
			,now()
			,now()
			,'9999-12-31' :: timestamp without time zone
			,cast(coalesce(s.oa_store_id, ss.oa_store_id) as varchar(10)) || '/' || cast(i.category_id as varchar(5)) as work_group_id
		from 
			dbo.oa_distributors d
			inner join dbo.stg_oa_items oi 
				on lower(oi.distributor_name) = lower(d.distributor_name)
				and d.oa_master_distributor_id = p_master_distributor_id
			inner join dbo.items i 
				on i.upc = oi.upc 
				and i.national_customer_id = oi.national_customer_id
			inner join dbo.customer_distributor_dyad cd 
				on cd.national_customer_id = oi.national_customer_id 
				and cd.oa_distributor_id = d.oa_distributor_id
			-- inner join dbo.customer_store_distributor_triad csd 
				-- on csd.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
				-- and p_run_date < csd.expiry_date
			left outer join dbo.oa_stores s 
				on s.national_customer_id = oi.national_customer_id 
				and s.store_number = (select cast(public.udf_store_number_from_string(oi.store_name) as int))
			left outer join cte_store_ids csd
				on csd.oa_store_id = s.oa_store_id
			left outer join dbo.oa_stores ss 
				on  ss.oa_store_id = csd.oa_store_id
			left outer join dbo.customer_store_item_triad cst 
				on cst.national_customer_id = oi.national_customer_id 
				and cst.oa_store_id = coalesce(s.oa_store_id, ss.oa_store_id) 
				and cst.item_id = i.item_id
		where 
			cst.customer_store_item_triad_id is null
			
		--deliveries
		;with cle as (
			select distinct
				 d.distributor_name
				,d.distributor_number
				,d.store_name
				,d.distributor_store_number
				,d.distributor_item_number
				,d.delivery_date
				,d.quantity
				,d.unit_price
				,d.po_number
				,d.national_customer_id
				,case 
					when cast(d.quantity as decimal(18,10)) < 0 
						then true
					else 
						false
				 end as is_credit
			from 
				dbo.stg_oa_deliveries d
				left outer join 
				(
					select distinct 
						coalesce(ship_date, '2000-01-01') as ship_date
					from 
						dbo.shipments ss
						inner join dbo.oa_distributors dd on 
							dd.oa_distributor_id = ss.oa_distributor_id
					where 
						dd.oa_master_distributor_id = p_master_distributor_id
				) s 
				on s.ship_date = d.delivery_date
			where 
				s.ship_date is null
				and d.oa_master_distributor_id = p_master_distributor_id
		)
		insert into dbo.shipments
		(
			 customer_store_item_triad_id
			,ship_date
			,quantity
			,is_credit
			,shipment_source
			,create_user
			,create_date
			,effective_date
			,expiry_date
			,po_number
			,oa_distributor_id
			,work_group_id
		)
		select distinct
			 cst.customer_store_item_triad_id
			,c.delivery_date
			,c.quantity
			,c.is_credit
			,v_master_distributor_name || '_load'
			,'dsdload'
			,now()
			,now()
			,'9999-12-31' :: timestamp without time zone
			,c.po_number
			,d.oa_distributor_id
			,cast(cst.oa_store_id as varchar(10)) || '/' || cast(it.category_id as varchar(5)) as work_group_id
		from 
			cle c
			inner join dbo.oa_distributors d 
				on lower(d.distributor_name) = lower(c.distributor_name)
			inner join dbo.customer_distributor_dyad cd 
				on cd.national_customer_id = c.national_customer_id 
				and cd.oa_distributor_id = d.oa_distributor_id
			left join dbo.customer_store_distributor_triad cdt 
				on cdt.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
				and p_run_date < cdt.expiry_date 
				and cdt.distributor_store_number = c.distributor_store_number
			inner join dbo.oa_stores os 
				on os.oa_store_id = cdt.oa_store_id
			inner join dbo.distributor_items di 
				on di.customer_distributor_dyad_id = cd.customer_distributor_dyad_id 
				and cast(di.distributor_product_code as bigint) = cast(c.distributor_item_number as bigint)
			inner join dbo.customer_store_item_triad cst 
				on cst.national_customer_id = c.national_customer_id 
				and cst.item_id = di.item_id 
				and cst.oa_store_id = os.oa_store_id
			inner join dbo.items it
				on it.item_id = cst.item_id
			left outer join dbo.shipments s 
				on cst.customer_store_item_triad_id = s.customer_store_item_triad_id 
				and c.delivery_date = s.ship_date 
				and s.is_credit = c.is_credit
		where 
			s.shipment_id is null;
			
		-- update customer_store_item_distributor_dyad
		insert into dbo.customer_store_item_distributor_dyad
		(
		     customer_store_item_triad_id
		    ,oa_distributor_id
		    ,create_user
			,work_group_id
        )
		select distinct
		     csi.customer_store_item_triad_id
			,d.oa_distributor_id
			,v_master_distributor_name || '_load'
			,cast(csi.oa_store_id as varchar(10)) || '/' || cast(it.category_id as varchar(5)) as work_group_id
		from
		    dbo.shipments s
		    inner join dbo.customer_store_item_triad csi
		        on csi.customer_store_item_triad_id = s.customer_store_item_triad_id
			inner join dbo.items it
				on it.item_id = csi.item_id
		    inner join dbo.oa_distributors d
		        on d.oa_distributor_id = s.oa_distributor_id
		    left outer join dbo.customer_store_item_distributor_dyad csd
		        on csd.customer_store_item_triad_id = csi.customer_store_item_triad_id
		        and csd.oa_distributor_id = d.oa_distributor_id
		where
		    csd.customer_store_item_distributor_dyad_id is null
			and d.oa_master_distributor_id = p_master_distributor_id;

		-- change to this and remove conversion factor code
		 perform public.usp_load_oa_cleanup(p_run_date, p_master_distributor_id);

END;

$BODY$;

