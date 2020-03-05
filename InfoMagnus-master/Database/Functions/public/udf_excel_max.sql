CREATE OR REPLACE FUNCTION public.udf_excel_max
(
	 p_value1 integer
	,p_value2 integer
)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

declare v_results int ;

begin

	v_results = 
	(
		select 
			case
				when p_value1 > p_value2 
					then p_value1
				when p_value2 > p_value1 	
					then p_value2
				else 
					p_value1
			end
	);
	
	return v_results;

end;
$BODY$;
