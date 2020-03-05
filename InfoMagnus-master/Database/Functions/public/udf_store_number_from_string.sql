CREATE OR REPLACE FUNCTION public.udf_store_number_from_string(
	p_storenumstring character varying)
    RETURNS character varying
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
declare v_outstring varchar(255) = '';
declare v_start int;
declare v_string varchar(255);
declare v_end int;

begin
	v_start = patindex('%[0-9]%',p_storeNumString); --Find first number

	if v_start <> 0 then
		v_string = substring(p_storeNumString, v_start, length(p_storeNumString) - v_start + 1);
	
		v_end = patindex('%[^0-9]%', v_string) - 1;  -- Find first space after number

		if v_end <= 0 then
			v_end = length(v_string);
		end if;

		v_outstring = substring(p_storeNumString, v_start, v_end);
	else
		v_outstring = null;
	end if;

	return v_outstring;
end;
$BODY$;