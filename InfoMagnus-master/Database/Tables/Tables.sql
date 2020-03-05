BEGIN;

CREATE SCHEMA IF NOT EXISTS "dbo";
CREATE SCHEMA IF NOT EXISTS "archive";

------------------------- Start of dimensional tables ------------------------

CREATE TABLE IF NOT EXISTS "dbo"."order_transfer_method"
(
	 "order_transfer_method_id"			serial primary key,
	 "order_transfer_method"			varchar(50) not null,
	 "create_user"						varchar(255) null,
	 "create_date"						timestamp without time zone null,
	 "effective_date"					timestamp without time zone null,
	 "expiry_date"						timestamp without time zone null default '9999-12-31'
);


CREATE TABLE IF NOT EXISTS "dbo"."oa_master_distributor"
( 
	"oa_master_distributor_id" 	serial primary key,
	"master_distributor_name" 	varchar(255) null,
	"create_user" 				varchar(255) null default user,
	"create_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date" 			timestamp without time zone null,
	"expiry_date" 				timestamp without time zone null default '9999-12-31',
	"order_transfer_method_id" 	int references "dbo"."order_transfer_method"(order_transfer_method_id),
	"abbr" 						varchar(5) null,
	"process_name" 				varchar(50) null,
	"is_auto_transmit" 			boolean null default true
);

CREATE TABLE IF NOT EXISTS "dbo"."oa_national_customers"
( 
	"national_customer_id" 		int primary key,
	"national_customer_name" 	varchar(255) null,
	"create_user" 				varchar(255) null default user,
	"create_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date" 			timestamp without time zone null,
	"expiry_date" 				timestamp without time zone null default '9999-12-31',
	"abbr" 						varchar(5) null
);


----------------- End of dimensional tables -----------------

---------------- Start of archive tables --------------------
CREATE TABLE IF NOT EXISTS "archive"."stg_dg_scans"
( 
	"archive_id" 		serial primary key,
	"id" 		 		int not null,
	"f_year" 	 		int null,
	"f_week" 	 		int null,
	"date" 		 		date null,
	"store" 	 		int null,
	"upc" 		 		varchar(20) null,
	"sku" 		 		varchar(20) null,
	"item_description"  varchar(255) null,
	"class" 	 		int null,
	"t0" 		 		varchar(50) null,
	"t1" 		 		varchar(50) null
);

CREATE TABLE IF NOT EXISTS "archive"."stg_fd_scans"
( 
	"archive_id" 		serial primary key,
	"row_id" 			bigint not null,
	"sku" 				varchar(20) null, -- Need to check data and decide data type
	"store" 			varchar(100) null, -- Need to check data and decide data type
	"transaction_date" 	date null,
	"units" 			int null,
	"extract_date" 		date null
);

CREATE TABLE IF NOT EXISTS "archive"."stg_oa_deliveries"
( 
	"distributor_name" 			varchar(255) not null, -- PK need to decide post checking data
	"po_number" 				varchar(255) null,
	"distributor_number" 		int not null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	bigint null,
	"distributor_item_number" 	bigint null,
	"delivery_date" 			date null,
	"quantity" 					int null,
	"unit_price" 				numeric(18, 4) null,
	"is_credit" 				boolean null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)	
);

CREATE TABLE IF NOT EXISTS "archive"."stg_oa_delivery_schedule"
( 
	"distributor_name" 			varchar(255) not null, -- PK need to decide post checking the data
	"distributor_number" 		int not null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	bigint null,
	"category_name" 			varchar(255) null,
	"sunday" 					boolean null,
	"monday" 					boolean null,
	"tuesday" 					boolean null,
	"wednesday" 				boolean null,
	"thursday" 					boolean null,
	"friday" 					boolean null,	
	"saturday" 					boolean null,
	"force_weekly" 				varchar(10) null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)	
);

CREATE TABLE IF NOT EXISTS "archive"."stg_oa_distributors"
( 
	"distributor_name" 			varchar(255) not null,
	"distributor_number" 		int not null,
	"master_distributor_name" 	varchar(255) null,
	"distributor_address_1" 	varchar(255) null,
	"distributor_address_2" 	varchar(255) null,
	"distributor_city" 			varchar(255) null,
	"distributor_state" 		varchar(25) null,
	"distributor_zip" 			varchar(25) null,
	"distributor_phone" 		varchar(25) null,
	"contact_name" 				varchar(25) null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)
);


CREATE TABLE IF NOT EXISTS "archive"."stg_oa_items"
( 
	"distributor_name" 			varchar(255) not null,
	"store_name" 				varchar(255) null,
	"category_name" 			varchar(255) null,
	"distributor_item_number" 	bigint null,
	"upc" 						varchar(20) null,
	"sku" 						varchar(20) null,
	"item_description" 			varchar(255) null,
	"size_units" 				int null, -- need to check data and assign data type
	"size_type" 				varchar(200) null,
	"units_per_case" 			int null,
	"temp_class" 				varchar(255) null,
	"shelf_life" 				int null,
	"is_case_order" 			boolean null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)
);

CREATE TABLE IF NOT EXISTS "archive"."stg_oa_promotions"
( 
	"master_distributor_name" 	varchar(255) null,
	"promotion_description" 	varchar(2000) null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	bigint null,
	"distributor_item_number" 	bigint null,
	"est_lift" 					numeric(10, 8) null,
	"pre_promotion_begin_date" 	date null,
	"promotion_begin_date" 		date null,
	"promotion_end_date" 		date null,
	"promotion_sale_end_date" 	date null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)
);

CREATE TABLE IF NOT EXISTS "archive"."stg_oa_store_routing"
( 
	"distributor_name" 			varchar(255) null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	bigint null,
	"distributor_number" 		int null,
	"category_name" 			varchar(255) null,
	"distributor_route_number" 	int null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)		
);

CREATE TABLE IF NOT EXISTS "archive"."stg_oa_stores"
( 
	"distributor_name" 			varchar(255) null,
	"store_name" 				varchar(255) not null,
	"distributor_store_number" 	bigint null,
	"store_number" 				int null,
	"customer_name" 			varchar(255) null,
	"address_1" 				varchar(255) null,
	"address_2" 				varchar(255) null,
	"store_city" 				varchar(255) null,
	"store_state" 				varchar(25) null,
	"store_zip" 				varchar(25) null,
	"store_classification" 		varchar(255) null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)		
);

CREATE TABLE IF NOT EXISTS "archive"."stg_scan"
(
	 "sku"					varchar(20) null,
	 "store_number"			int null,
	 "transaction_date"		varchar(100) null,
	 "quantity"				int null,
	 "total_cost"			numeric(18, 4) null,
	 "national_customer_id" int references "dbo"."oa_national_customers"(national_customer_id)	 
);

-------------------- End of archive tables ---------------------------

-------------------- Start of staging dbo tables ---------------------

CREATE TABLE IF NOT EXISTS "dbo"."stg_oa_deliveries"
( 
	"distributor_name" 			varchar(255) not null, -- PK need to decide post checking data
	"po_number" 				varchar(255) null,
	"distributor_number" 		varchar(255) not null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	varchar(255) null,
	"distributor_item_number" 	varchar(255) null,
	"delivery_date" 			date null,
	"quantity" 					numeric(18, 4) null,
	"unit_price" 				numeric(18, 4) null,
	"is_credit" 				boolean null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)	
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_oa_delivery_schedule"
( 
	"distributor_name" 			varchar(255) null, -- PK need to decide post checking the data
	"distributor_number" 		varchar(255) null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	varchar(255) null,
	"category_name" 			varchar(255) null,
	"sunday" 					boolean null,
	"monday" 					boolean null,
	"tuesday" 					boolean null,
	"wednesday" 				boolean null,
	"thursday" 					boolean null,
	"friday" 					boolean null,	
	"saturday" 					boolean null,
	"force_weekly" 				varchar(10) null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)	
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_oa_distributors"
( 
	"distributor_name" 			varchar(255) null,
	"distributor_number" 		varchar(255) null,
	"master_distributor_name" 	varchar(255) null,
	"distributor_address_1" 	varchar(255) null,
	"distributor_address_2" 	varchar(255) null,
	"distributor_city" 			varchar(255) null,
	"distributor_state" 		varchar(25) null,
	"distributor_zip" 			varchar(25) null,
	"distributor_phone" 		varchar(25) null,
	"contact_name" 				varchar(25) null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_oa_items"
( 
	"distributor_name" 			varchar(255) not null,
	"store_name" 				varchar(255) null,
	"category_name" 			varchar(255) null,
	"distributor_item_number" 	varchar(255) null,
	"upc" 						varchar(25) null,
	"sku" 						varchar(25) null,
	"item_description" 			varchar(255) null,
	"size_units" 				numeric(18, 4) null, -- need to check data and assign data type
	"size_type" 				varchar(200) null,
	"units_per_case" 			int null,
	"temp_class" 				varchar(255) null,
	"shelf_life" 				int null,
	"is_case_order" 			boolean null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_oa_promotions"
( 
	"master_distributor_name" 	varchar(255) null,
	"promotion_description" 	varchar(2000) null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	varchar(255) null,
	"distributor_item_number" 	varchar(255) null,
	"est_lift" 					numeric(10, 8) null,
	"pre_promotion_begin_date" 	date null,
	"promotion_begin_date" 		date null,
	"promotion_end_date" 		date null,
	"promotion_sale_end_date" 	date null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_oa_store_routing"
( 
	"distributor_name" 			varchar(255) null,
	"store_name" 				varchar(255) null,
	"distributor_store_number" 	varchar(255) null,
	"distributor_number" 		varchar(255) null,
	"category_name" 			varchar(255) null,
	"distributor_route_number" 	varchar(255) null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)		
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_oa_stores"
( 
	"distributor_name" 			varchar(255) null,
	"store_name" 				varchar(255) not null,
	"distributor_store_number" 	bigint null,
	"store_number" 				int null,
	"customer_name" 			varchar(255) null,
	"address_1" 				varchar(255) null,
	"address_2" 				varchar(255) null,
	"store_city" 				varchar(255) null,
	"store_state" 				varchar(25) null,
	"store_zip" 				varchar(25) null,
	"store_classification" 		varchar(255) null,
	"import_file_name" 			varchar(300) null,
	"import_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"national_customer_id"		int references "dbo"."oa_national_customers"(national_customer_id)		
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_auto_operator_adjustment"
(
	 "base_order_id" 		int not null,
	 "operator_adjustment"	int null,
	 "create_date"			timestamp without time zone not null,
	 "source_location"		varchar(100) null,
	 "source_type"			varchar(100) null,
	 "pred_value_null"		int null
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_scan"
(
	 "id"					serial primary key,
	 "sku"					varchar(100) null,
	 "store_number"			varchar(100) null,
	 "transaction_date"		varchar(100) null,
	 "quantity"				varchar(100) null,
	 "total_cost"			varchar(100) null,
	 "national_customer_id" int references "dbo"."oa_national_customers"(national_customer_id)
);
------------------------- End of staging dbo tables --------------------------

-------------------------- Start of RedShift tables --------------------------
CREATE TABLE IF NOT EXISTS "dbo"."oa_contacts"
(
	"oa_contact_id"				int primary key,
	"contact_name"				varchar(255) null, -- Check once again for data type
	"contact_email"				varchar(100) null,
	"contact_phone"				varchar(14) null,
	"create_user"				varchar(255) null default user,
	"create_date"				timestamp without time zone not null default now() :: timestamp without time zone,
	"effective_date"			timestamp without time zone not null default now() :: timestamp without time zone,
	"expiry_date"				timestamp without time zone not null default '9999-12-31',
	"notes"						varchar(255) null
);

CREATE TABLE IF NOT EXISTS "dbo"."categories"
(
	"category_id" 				serial primary key,
	"category_name"				varchar(255) null,
	"category_level_2"			varchar(255) null,
	"category_level_3"			varchar(255) null,
	"category_level_4"			varchar(255) null,
	"create_user"				varchar(255) null default user,
	"create_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"			timestamp without time zone null,
	"expiry_date"				timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."package"
(
	"package_id"				serial primary key,
	"package"					varchar(50) null,
	"create_user"				varchar(255) null default user,
	"create_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"			timestamp without time zone null,
	"expiry_date"				timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."sub_category"
(
	"sub_category_id"			serial primary key,
	"category_id"				int references "dbo"."categories"(category_id),
	"sub_category_name"			varchar(255) null,
	"create_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"create_user"				varchar(255) null default user,
	"expiry_date"				timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."oa_distributors"
(
	"oa_distributor_id"			serial primary key,
	"distributor_name"			varchar(255) not null,
	"distributor_number"		int null,
	"oa_master_distributor_id"	int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"oa_contact_id"				int null references "dbo"."oa_contacts"(oa_contact_id),
	"create_user"				varchar(255) null default user,
	"create_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"			timestamp without time zone null default now() :: timestamp without time zone,
	"short_name"				varchar(255) null default ('')
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_distributor_dyad"
(
	"customer_distributor_dyad_id"	serial primary key,
	"national_customer_id"			int references "dbo"."oa_national_customers"(national_customer_id),
	"oa_distributor_id"				int references "dbo"."oa_distributors"(oa_distributor_id),
	"create_user"					varchar(255) null default user,
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"					timestamp without time zone null default '9999-12-31'
);

ALTER SEQUENCE IF EXISTS  dbo.customer_distributor_dyad_customer_distributor_dyad_id_seq
RENAME TO cust_dist_dyad_cust_dist_dyad_id_seq;

CREATE TABLE IF NOT EXISTS "dbo"."oa_stores"
(
	"oa_store_id" 				serial primary key,
	"national_customer_id" 		int references "dbo"."oa_national_customers"(national_customer_id),
	"pmi_dev_store_id" 			int null,
	"store_number" 				int	 null,
	"address_1" 				varchar(255) null,
	"address_2" 				varchar(255) null,
	"store_city" 				varchar(255) null,
	"store_state"		 		varchar(25) null,
	"store_zip" 				varchar(25) null,
	"create_user" 				varchar(255) null default user,
	"create_date" 				timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date" 			timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date" 				timestamp without time zone null default '9999-12-31',
	"phone_number" 				varchar(30) null
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_store_distributor_triad"
(
	"customer_store_distributor_triad_id"	serial primary key,
	"customer_distributor_dyad_id"			int references "dbo"."customer_distributor_dyad"(customer_distributor_dyad_id),
	"oa_store_id"							int references "dbo"."oa_stores"(oa_store_id),
	"distributor_store_number"				bigint null,
	"distributor_store_name"				varchar(255) null,
	"store_classification"					varchar(255) null,
	"distributor_route_number"				int null,
	"create_user"							varchar(255) null default user,
	"create_date"							timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"						timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"							timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."items"
(
	"item_id"						serial primary key,
	"national_customer_id"			int null references "dbo"."oa_national_customers"(national_customer_id),
	"sku"							varchar(20) null,
	"upc"							varchar(20) null,
	"category_id"					int null references "dbo"."categories"(category_id),
	"create_user"					varchar(255) null default user,
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"					timestamp without time zone null default '9999-12-31',
	"units_per_case"				int null,
	"is_case_order"					boolean null, -- check the data and assign value
	"package_id"					int null references "dbo"."package"(package_id),
	"retailer_item_description"		varchar(255) null
);

CREATE TABLE IF NOT EXISTS "dbo"."distributor_items"
(
	"distributor_item_id"			serial primary key,
	"customer_distributor_dyad_id" 	int references "dbo"."customer_distributor_dyad"(customer_distributor_dyad_id),
	"item_id"						int references "dbo"."items"(item_id),
	"distributor_product_code"		bigint null,
	"create_user"					varchar(255) null default user,
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"				timestamp without time zone null,
	"expiry_date"					timestamp without time zone null default '9999-12-31',
	"distributor_item_description"	varchar(255) null,
	"sub_category_id"				int references "dbo"."sub_category"(sub_category_id)
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_store_item_triad"
(
	"customer_store_item_triad_id"	serial primary key,
	"national_customer_id"			int references "dbo"."oa_national_customers"(national_customer_id),
	"oa_store_id"					int references "dbo"."oa_stores"(oa_store_id),
	"item_id"						int references "dbo"."items"(item_id),
	"create_user"					varchar(255) null default user,
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"					timestamp without time zone null default '9999-12-31',
	unique (oa_store_id, item_id)
);

ALTER SEQUENCE IF EXISTS  dbo.customer_store_distributor_tr_customer_store_distributor_tr_seq
RENAME TO cust_store_dis_triad_cust_store_dist_triad_id_seq;

CREATE TABLE IF NOT EXISTS "dbo"."shipments"
(
	"shipment_id"					serial primary key,
	"customer_store_item_triad_id"	int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"ship_date"						date null,
	"quantity"						int null,
	"is_credit"						boolean null,
	"shipment_source"				varchar(255) null,
	"create_user"					varchar(255) null default user,
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"				timestamp without time zone null,
	"expiry_date"					timestamp without time zone null default '9999-12-31',
	"po_number"						varchar(255) null,
	"oa_distributor_id"				int references "dbo"."oa_distributors"(oa_distributor_id)
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_store_item_distributor_dyad"
(
	"customer_store_item_distributor_dyad_id"	serial primary key,
	"customer_store_item_triad_id" 				int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id) unique,
	"oa_distributor_id"							int references "dbo"."oa_distributors"(oa_distributor_id),
	"effective_date"							timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"								timestamp without time zone null default '9999-12-31',
	"create_user"								varchar(255) null default user,
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"modified_date"								timestamp without time zone null default now() :: timestamp without time zone,
);

ALTER SEQUENCE IF EXISTS dbo.customer_store_item_distribut_customer_store_item_distribut_seq
RENAME TO cust_store_item_dist_dyad_cust_store_item_dist__dyad_id_seq;


CREATE TABLE IF NOT EXISTS "dbo"."oa_scans"
(
	"oa_scan_id"					serial primary key,
	"customer_store_item_triad_id"	int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"transaction_date"				date not null,
	"quantity"						int not null,
	"scans_source"					varchar(255) null,
	"create_user"					varchar(255) null default user,
	"run_date"						date default now()::date
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"					timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."oa_scans_sales"
(
	"oa_scan_sales_id"				serial primary key,
	"oa_scan_id"					int references "dbo"."oa_scans"(oa_scan_id),
	"units"							int null,
	"sales_dollars"					numeric(10, 4) null,
	"transaction_date"				date null,
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone,
	"create_user"					varchar(255) null default user
);

CREATE TABLE IF NOT EXISTS "dbo"."adjustment_granularity"
(
	"granularity_id"			int primary key,
	"granularity_description"	varchar(100) not null,
	"apply_id_name"				varchar(100) not null,
	"create_user"				varchar(255) not null,
	"create_date"				timestamp without time zone not null,
	"expiry_date"				timestamp without time zone not null,
	"order_index"				int null
);

CREATE TABLE IF NOT EXISTS "dbo"."adjustment_types"
(
	"adjustment_type_id"		int primary key,
	"adjustment_description"	varchar(100) not null,
	"create_user"				varchar(20) not null,
	"create_date"				timestamp without time zone not null,
	"expiry_date"				timestamp without time zone not null
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_distributor_category_triad"
(
	"customer_distributor_category_triad_id"	serial primary key,
	"national_customer_id"						int references "dbo"."oa_national_customers"(national_customer_id),
	"oa_distributor_id"							int references "dbo"."oa_distributors"(oa_distributor_id),
	"category_id"								int references "dbo"."categories"(category_id),
	"create_user"								varchar(255) null default user,
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"							timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"								timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."base_order"
(
	"base_order_id"								serial primary key,
	"oa_master_distributor_id"					int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"customer_distributor_dyad_id"				int references "dbo"."customer_distributor_dyad"(customer_distributor_dyad_id),
	"distributor_item_id"						int references "dbo"."distributor_items"(distributor_item_id),
	"customer_store_distributor_triad_id"		int references "dbo"."customer_store_distributor_triad"(customer_store_distributor_triad_id),
	"customer_store_item_distributor_dyad_id"	int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"customer_store_item_triad_id"				int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"customer_distributor_category_triad_id"	int references "dbo"."customer_distributor_category_triad"(customer_distributor_category_triad_id),
	"category_id"								int references "dbo"."categories"(category_id),
	"rec_delivery_date"							date null,
	"actual_scans"								int null,
	"forecasted_scans"							int null,
	"base_order"								int null,
	"run_date"									date null,
	"create_date"								timestamp without time zone null,
	"model_used"								varchar(255) null,
	"inc_in_anomaly"							boolean null,
	"inc_in_file"								boolean null,
	"inc_in_billing"							boolean null,
	"work_group_id"								varchar(15) not null
);


CREATE TABLE IF NOT EXISTS "dbo"."conversion_residual"
(
	"conversion_residual_id"						serial primary key,
	"customer_store_item_distributor_dyad_id"		int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"residual_date"									date null default now() :: timestamp without time zone :: date,
	"residual_quantity"								int null,
	"applied_date"									date null
);

CREATE TABLE IF NOT EXISTS "dbo"."credit_thresholds"
(
	"credit_threshold_id"							serial primary key,
	"customer_distributor_category_triad_id"		int references "dbo"."customer_distributor_category_triad"(customer_distributor_category_triad_id),
	"min_credit_percentage"							double precision not null,
	"max_credit_percentage"							double precision not null,
	"create_user"									varchar(255) null default user,
	"create_date"									timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"									timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."credit_threshold_exceptions"
(
	"credit_threshold_id"							int references "dbo"."credit_thresholds"(credit_threshold_id),
	"customer_store_item_distributor_dyad_id"		int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"min_credit_percentage"							double precision not null,
	"max_credit_percentage"							double precision not null,
	"create_user"									varchar(255) null default user,
	"create_date"									timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"									timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_store_distributor_attributes"
(
	"customer_store_distributor_attribute_id"		serial primary key,
	"customer_store_distributor_triad_id"			int references "dbo"."customer_store_distributor_triad"(customer_store_distributor_triad_id),
	"attribute"										varchar(255) null,
	"attribute_value"								varchar(1000) null,
	"effective_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"									timestamp without time zone null default '9999-12-31',
	"create_date"									timestamp without time zone null default now() :: timestamp without time zone,
	"create_user"									varchar(255) null default user
);

ALTER SEQUENCE IF EXISTS dbo.customer_store_distributor_at_customer_store_distributor_at_seq
RENAME TO cust_store_dis_attrib_cust_store_dist_attrib_id_seq;

CREATE TABLE IF NOT EXISTS "dbo"."delivery_schedules"
(
	"delivery_schedule_id"					serial primary key,
	"sunday"								boolean null,
	"monday"								boolean null,
	"tuesday"								boolean null,
	"wednesday"								boolean null,
	"thursday"								boolean null,
	"friday"								boolean null,
	"saturday"								boolean null,
	"create_user"							varchar(255) null default user,
	"create_date"							timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"						timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"							timestamp without time zone null default '9999-12-31',
	"description"							varchar(255) null,
	"short_description"						varchar(150) null
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_store_distributor_schedule"
(
	"customer_store_distributor_schedule_id"		serial primary key,
	"customer_store_distributor_triad_id"			int references "dbo"."customer_store_distributor_triad"(customer_store_distributor_triad_id),
	"delivery_schedule_id"							int references "dbo"."delivery_schedules"(delivery_schedule_id),
	"category_id"									int references "dbo"."categories"(category_id),
	"create_user"									varchar(255) null default user,
	"create_date"									timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"								timestamp without time zone null,
	"expiry_date"									timestamp without time zone null default '9999-12-31',
	"inc_in_anomaly"								boolean null default true,
	"inc_in_file"									boolean null default true,
	"inc_in_billing"								boolean null default false	
);

ALTER SEQUENCE IF EXISTS dbo.customer_store_distributor_sc_customer_store_distributor_sc_seq
RENAME TO cust_store_dist_sched_cust_store_dist_sched_id_seq;

CREATE TABLE IF NOT EXISTS "dbo"."last_deliveries"
(
	"customer_store_distributor_triad_id"			int references "dbo"."customer_store_distributor_triad"(customer_store_distributor_triad_id),
	"category_id"									int references "dbo"."categories"(category_id),
	"last_delivery"									date null,
	"create_date"									date null
);

CREATE TABLE IF NOT EXISTS "dbo"."lead_times"
(
	"lead_time_id"									serial primary key,
	"customer_distributor_category_triad_id"		int references "dbo"."customer_distributor_category_triad"(customer_distributor_category_triad_id),
	"lead_time_days"								int null,
	"oa_store_id"									int references "dbo"."oa_stores"(oa_store_id),
	"create_date"									timestamp without time zone null default now() :: timestamp without time zone,
	"create_user"									varchar(255) null default user,
	"effective_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"									timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."lead_time_exceptions"
(
	"lead_time_exceptions_id"			serial primary key,
	"oa_master_distributor_id"			int references "dbo"."oa_master_distributor"(oa_master_distributor_id),
	"order_date_dow"					varchar(20) not null,
	"lead_time"							int not null, -- need to check
	"create_date"						timestamp without time zone not null default now() :: timestamp without time zone,
	"expiry_date"						timestamp without time zone not null default '9999-12-31',
	"effective_date"					timestamp without time zone not null default now() :: timestamp without time zone,
	"create_user"						varchar(255) not null default user
);

CREATE TABLE IF NOT EXISTS "dbo"."mass_adjustments_table"
(
	"adjustment_id"			int not null,
	"adjustment_type_id"	int references "dbo"."adjustment_types"(adjustment_type_id),
	"granularity_id"		int references "dbo"."adjustment_granularity"(granularity_id),
	"apply_id"				int not null,
	"adjustment_value"		double precision not null,
	"create_user"			varchar(255) not null,
	"create_date"			timestamp without time zone not null,
	"begin_date"			timestamp without time zone not null,
	"expiry_date"			timestamp without time zone not null,
	"notes"					varchar null
);

CREATE TABLE IF NOT EXISTS "dbo"."operator_adjustments_reasons"
(
	"operator_adjustments_reason_id"			serial primary key,
	"operator_adjustments_reason_description"	varchar(255) null,
	"is_active"									boolean null default true,
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"create_user"								varchar(255) null default user
);

CREATE TABLE IF NOT EXISTS "dbo"."operator_adjustments"
(
	"adjustment_id"								serial primary key,
	"customer_store_item_distributor_dyad_id"	int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"adjustment_quantity"						int null,
	"rec_delivery_date"							date not null,
	"create_user"								varchar(255) null default user,
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"operator_adjustments_reason_id"			int references "dbo"."operator_adjustments_reasons"(operator_adjustments_reason_id)
);

CREATE TABLE IF NOT EXISTS "dbo"."orders"
(
	"order_id"									serial primary key,
	"customer_store_distributor_triad_id"		int references "dbo"."customer_store_distributor_triad"(customer_store_distributor_triad_id),
	"po_number"									varchar(255) null,
	"create_user"								varchar(255) null default user,
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"rec_delivery_date"							date null,
	"category_id"								int references "dbo"."categories"(category_id),
	"inc_in_billing"							boolean null
);

CREATE TABLE IF NOT EXISTS "dbo"."order_status_type"
(
	"order_status_type_id"		serial primary key,
	"order_status_type"			varchar(255) null,
	"create_user"				varchar(255) null default user,
	"create_date"				timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"			timestamp without time zone null,
	"expiry_date"				timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."order_status"
(
	"order_status_id"		serial primary key,
	"order_id"				int references "dbo"."orders"(order_id),
	"order_status_type_id"	int references "dbo"."order_status_type"(order_status_type_id),
	"create_user"			varchar(255) null default user,
	"create_date"			timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"		timestamp without time zone null,
	"expiry_date"			timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."order_details"
(
	"order_detail_id"		serial primary key,
	"order_status_id"		int references "dbo"."order_status"(order_status_id),
	"item_id"				int references "dbo"."items"(item_id),
	"package_id"			int references "dbo"."package"(package_id),
	"order_quantity"		int not null,
	"generation_date"		date not null,
	"modified_date"			date not null,
	"should_order"			boolean null,
	"create_user"			varchar(255) null default user,
	"is_set_to_average"		boolean null default false
);

CREATE TABLE IF NOT EXISTS "dbo"."order_transfer_method"
(
	"order_transfer_method_id"		serial primary key,
	"order_transfer_method"			varchar(50) not null,
	"create_user"					varchar(255) null,
	"create_date"					timestamp without time zone null,
	"effective_date"				timestamp without time zone null,
	"expiry_date"					timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."stg_order_forecasts"
(
	"customer_store_item_triad_id"	int null,
	"units"							int null,
	"forecast_date"					date null,
	"forecast_source"				varchar(255) null,
	"create_date"					timestamp without time zone null
);

CREATE TABLE IF NOT EXISTS "dbo"."override_adjustments"
(
	"adjustment_id"									serial primary key,
	"customer_store_item_distributor_dyad_id"		int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"override_quantity"								int not null,
	"is_used"										boolean null default false,
	"used_date"										date null,
	"create_user"									varchar(255) null default user,
	"create_date"									timestamp without time zone null default now() :: timestamp without time zone,
	"notes"											varchar null
);

CREATE TABLE IF NOT EXISTS "dbo"."store_lead_time_exceptions"
(
	"store_lead_time_exception_id"					serial primary key,
	"customer_store_distributor_triad_id"			int references "dbo"."customer_store_distributor_triad"(customer_store_distributor_triad_id),
	"order_date_dow"								varchar(20) not null, -- need to check data and change type if needed
	"lead_time"										int not null,
	"create_date"									timestamp without time zone not null default now() :: timestamp without time zone,
	"expiry_date"									timestamp without time zone not null default '9999-12-31',
	"effective_date"								timestamp without time zone not null default now() :: timestamp without time zone,
	"create_user"									varchar(255) not null default user
);

CREATE TABLE IF NOT EXISTS "dbo"."oa_sku_upc_conversion"
(
	"oa_sku_upc_conversion_id"			serial primary key,
	"national_customer_id"				int references "dbo"."oa_national_customers"(national_customer_id),
	"sku"								varchar(20) null,
	"upc"								varchar(20) null,
	"date_updated"						timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"						timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."retailer_last_scan_date"
(
	"customer_store_item_triad_id" 	int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"max_date"						date null,
	"refresh_date"					date null,
	"work_group_id"					varchar(15) not null
);

CREATE TABLE IF NOT EXISTS "dbo"."etl_log"
(
	"id"					serial primary key,
	"notes"					varchar(250) null,
	"run_date_time"			timestamp without time zone null,
	"national_customer_id"	int null
);

CREATE TABLE IF NOT EXISTS "dbo"."customer_gap"
(
	"national_customer_id"	int references "dbo"."oa_national_customers"(national_customer_id),
	"days_since_scans"		int null,
	"refresh_date"			date null
);

CREATE TABLE IF NOT EXISTS "dbo"."dim_date"
(
	"date_key"					int not null,
	"client_key"				int not null,
	"calendar_date"				date not null,
	"day_number_of_week"		smallint not null,
	"day_name_of_week"			varchar(15) not null,
	"day_number_of_month"		smallint not null,
	"day_number_of_year"		smallint not null,
	"week_number_of_year"		smallint not null,
	"month_name"				varchar(15) not null,
	"month_number_of_year"		smallint not null,
	"quarter_number_of_year"	smallint not null,
	"calendar_year"				smallint not null,
	"fiscal_quarter"			smallint null,
	"fiscal_year"				smallint null,
	"fiscal_week"				smallint null,
	primary key (date_key, client_key)
);

CREATE TABLE IF NOT EXISTS "dbo"."conversion_factors"
(
	"conversion_factor_id"						serial primary key,
	"customer_store_item_distributor_dyad_id"	int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"conversion_factor"							varchar(200) null,
	"conversion_units"							int null,
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"							timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"								timestamp without time zone null default '9999-12-31'
);

CREATE TABLE IF NOT EXISTS "dbo"."spoils_adjustments"
(
	"spoils_adjustment_id"						serial primary key,
	"customer_store_item_triad_id"				int not null references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"spoils_inc"								int not null default 4,
	"spoils_dec"								int not null default -2,
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"effective_date"							timestamp without time zone null default now() :: timestamp without time zone,
	"expiry_date"								timestamp without time zone null default '9999-12-31',
	"create_user"								varchar(255) null default user
);

CREATE TABLE IF NOT EXISTS "dbo"."order_forecasts"
(
	"order_forecast_id"				serial primary key,
	"customer_store_item_triad_id"	int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"units"							int null,
	"forecast_date"					date null,
	"forecast_source"				varchar(255) null,
	"run_date"						date null,
	"create_date"					timestamp without time zone null default now() :: timestamp without time zone
);

CREATE TABLE IF NOT EXISTS "dbo"."credit_perc"
(
	"credit_perc_id"							serial primary key,
	"oa_distributor_id"							int references "dbo"."oa_distributors"(oa_distributor_id),
	"customer_store_item_triad_id"				int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"ttl_del"									int null,
	"ttl_cr"									int null,
	"cr_perc"									double precision null,
	"create_date"								date null default now() :: timestamp without time zone,
	"customer_store_item_distributor_dyad_id"	int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id)	
);

CREATE TABLE IF NOT EXISTS "dbo"."weight_data"
(
	"weight_data_id"							serial primary key,
	"customer_store_item_triad_id"				int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"oa_distributor_id"							int references "dbo"."oa_distributors"(oa_distributor_id),
	"category_id"								int references "dbo"."categories"(category_id),
	"spoils"									double precision not null,
	"over_under"								int not null,
	"refresh_date"								date null,
	"customer_store_item_distributor_dyad_id"	int references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"create_date"								timestamp without time zone not null default now() :: timestamp without time zone
);


CREATE TABLE IF NOT EXISTS "dbo"."true_up_adjustments"
(
	"true_up_adjustment_id"						serial primary key,
	"customer_store_item_triad_id"				int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"scan_date"									date null,
	"units_scanned"								int null default 0,
	"forecast_units"							int null default 0,
	"variance"									int null default 0,
	"model_used"								varchar(255) null,
	"base_order_id"								int references "dbo"."base_order"(base_order_id),
	"create_date"								timestamp without time zone null default now() :: timestamp without time zone,
	"create_user"								varchar(255) null default user,
	"correction"								int not null default 0
);

CREATE TABLE IF NOT EXISTS "dbo"."avg_scans_customer_store_item_wk_day"
(
	"avg_scans_customer_store_item_wk_day_id"	serial primary key,
	"customer_store_item_triad_id"				int references "dbo"."customer_store_item_triad"(customer_store_item_triad_id),
	"sunday"									decimal(8, 2) null,
	"monday"									decimal(8, 2) null,
	"tuesday"									decimal(8, 2) null,
	"wednesday"									decimal(8, 2) null,
	"thursday"									decimal(8, 2) null,
	"friday"									decimal(8, 2) null,
	"saturday"									decimal(8, 2) null,
	"avg_weekly"								decimal(8, 2) null,
	"std_dev_min"								decimal(8, 2) null,
	"std_dev_max"								decimal(8, 2) null,
	"last_update_date"							timestamp without time zone null 	
);

ALTER SEQUENCE IF EXISTS dbo.avg_scans_customer_store_item_avg_scans_customer_store_item_seq
RENAME TO avg_scn_cust_store_item_avg_scn_cust_store_item_wk_day_id_seq;

CREATE TABLE IF NOT EXISTS "dbo"."applied_override_adjustments"
(
	"override_id"								serial primary key,
	"customer_store_item_distributor_dyad_id"	int null references "dbo"."customer_store_item_distributor_dyad"(customer_store_item_distributor_dyad_id),
	"override_quantity"							int null,
	"referenced_create_date"					timestamp without time zone null,
	"create_date"								timestamp without time zone null
);

CREATE TABLE IF NOT EXISTS "dbo"."applied_operator_adjustments"
(
	"adjustment_id"				serial primary key,
	"base_order_id"				int not null references "dbo"."base_order"(base_order_id),
	"adjustment_quantity"		int not null,
	"create_date"				timestamp without time zone not null,
	"adjusted_by"				varchar(50) null
);

CREATE TABLE IF NOT EXISTS "dbo"."adjustments_calculation"
(
	 "base_order_id" 			int null references "dbo"."base_order"(base_order_id)
	,"adjustment_type_id"		int null references "dbo"."adjustment_types"(adjustment_type_id)
	,"adjustment_value"			double precision null
	,"create_date"				timestamp without time zone
);

CREATE TABLE IF NOT EXISTS dbo.so_log
(
     id serial primary key
    ,process varchar(100) null
    ,notes varchar(255) null
    ,run_date_time timestamp without time zone default now() :: timestamp without time zone
);

CREATE TABLE IF NOT EXISTS "dbo"."ml_poor_performers"
(
	 "customer_store_item_triad_id"		integer null
	,"is_acc"							boolean null
	,"ttl_ct"							integer null
	,"perc_corr"						double precision null
	,"ct_threshold"						int null
	,"acc_threshold"					double precision null
	,"date_time_created"				timestamp without time zone null	
);

CREATE TABLE IF NOT EXISTS "dbo"."store_start_dates_ml"
(
	 "rec_id"						serial primary key
	,"master_distributor_name"		varchar(255) not null
	,"distributor_name"				varchar(255) not null
	,"category_name"				varchar(255) not null
	,"national_customer_id"			integer null
	,"store_number"					integer not null
	,"customer_store_item_triad_id"	integer null
	,"start_date"					date not null
	,"end_date"						date not null default ('9999-12-31')
);