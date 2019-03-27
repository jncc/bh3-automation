-- FUNCTION: public.bh3_find_srid(name, name, name)

-- DROP FUNCTION public.bh3_find_srid(name, name, name);

CREATE OR REPLACE FUNCTION public.bh3_find_srid(
	table_schema name,
	table_name name,
	geom_column name DEFAULT 'the_geom'::name)
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

	IF length(coalesce(table_schema)) > 0 AND length(coalesce(table_name)) > 0 AND length(coalesce(geom_column)) > 0 THEN
		EXECUTE format('SELECT Find_SRID($1::text, $2::text, $3::text)')
		INTO srid
		USING table_schema, table_name, geom_column;

		IF srid = 0 THEN
			OPEN cursorSrid FOR EXECUTE format('SELECT DISTINCT ST_Srid(%I) FROM %I.%I',
											   geom_column, table_schema, table_name);
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

COMMENT ON FUNCTION public.bh3_find_srid(name, name, name)
    IS 'Purpose:
Finds the SRID of the geometries in the specified geometry column of the specified geometry table.
If the geometries do not have the same single SRID zero is returned.

Approach:
Uses a select query to obtain distinct SRIDs directly from the geometries in the speficied table and geometry column.

Prameters:
table_schema	name	Schema  of the geometry table.
table_name		name	Name of the geometry table.
geom_column		name	Name of the geometry column. Defaults to ''the_geom''.

Returns:
The table SRID if all geometries share the same SRID, otherwise 0.

Calls:
No nested calls';
