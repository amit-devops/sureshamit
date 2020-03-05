CREATE OR REPLACE FUNCTION public.patindex(
	pattern character varying,
	expression character varying)
    RETURNS integer
    LANGUAGE 'sql'

    COST 100
    IMMUTABLE 
AS $BODY$
SELECT
    COALESCE(
        STRPOS(
             $2
            ,(
                SELECT
                    ( REGEXP_MATCHES(
                        $2
                        ,'(' || REPLACE( REPLACE( TRIM( $1, '%' ), '%', '.*?' ), '_', '.' ) || ')'
                        ,'i'
                    ) )[ 1 ]
                LIMIT 1
            )
        )
        ,0
    )
;
$BODY$;
