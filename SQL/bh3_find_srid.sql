-- FUNCTION: public.bh3_find_srid(name, name, name)

-- DROP FUNCTION public.bh3_find_srid(name, name, name);

CREATE OR REPLACE FUNCTION public.bh3_find_srid(
	tableschema name,
	tablename name,
	geomcolumn name DEFAULT 'the_geom'::name)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
	cursorSrid refcursor;
	srid int;

BEGIN
	srid := 0;

	IF length(coalesce(tableSchema)) > 0 AND length(coalesce(tableName)) > 0 AND length(coalesce(geomColumn)) > 0 THEN
		EXECUTE format('SELECT Find_SRID($1::text, $2::text, $3::text)')
		INTO srid
		USING tableSchema, tableName, geomColumn;

		IF srid = 0 THEN
			OPEN cursorSrid FOR EXECUTE format('SELECT DISTINCT ST_Srid(%I) FROM %I.%I',
											   geomColumn, tableSchema, tableName);
			FETCH cursorSrid INTO srid;

			IF FOUND THEN
				MOVE cursorSrid;
				IF FOUND THEN
					 srid := 0;
				END IF;
			END IF;

			CLOSE cursorSrid;
		END IF;
	END IF;

	RETURN srid;
END;
$BODY$;

ALTER FUNCTION public.bh3_find_srid(name, name, name)
    OWNER TO postgres;
