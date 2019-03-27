-- PROCEDURE: public.bh3_repair_geometries(name, name, name)

-- DROP PROCEDURE public.bh3_repair_geometries(name, name, name);

CREATE OR REPLACE PROCEDURE public.bh3_repair_geometries(
	schema_name name,
	table_name name,
	geom_column name DEFAULT 'the_geom'::name)
LANGUAGE 'plpgsql'

AS $BODY$
BEGIN
	IF length(coalesce(table_name, '')) > 0 AND length(coalesce(geom_column, '')) > 0 THEN
		IF length(coalesce(schema_name, '')) > 0 THEN
			EXECUTE format('UPDATE %1$I.%2$I '
						   'SET %3$I = ST_Multi(ST_Buffer('
							   'CASE '
								   'WHEN ST_IsCollection(%3$I) THEN ST_CollectionExtract(%3$I,3) '
								   'ELSE %3$I '
							   'END, 0)) '
						   'WHERE NOT ST_IsValid(%3$I) OR NOT ST_IsSimple(%3$I) OR ST_IsCollection(%3$I)',
						   schema_name, table_name, geom_column);
		ELSE
			EXECUTE format('UPDATE %1$I '
						   'SET %2$I = ST_Multi(ST_Buffer('
							   'CASE '
								   'WHEN ST_IsCollection(%2$I) THEN ST_CollectionExtract(%2$I,3) '
								   'ELSE %2$I '
							   'END, 0)) '
						   'WHERE NOT ST_IsValid(%2$I) OR NOT ST_IsSimple(%2$I) OR ST_IsCollection(%2$I)',
						   table_name, geom_column);
		END IF;
	END IF;
END;
$BODY$;

COMMENT ON PROCEDURE public.bh3_repair_geometries
    IS 'Purpose:
Repairs the geometries in the specified column of a table and converts them to multi geometries. 

Approach:
Uses a zero buffer to repair geometries. Geometries that are part of a collection are repaired individually.
Repaired geometries are converted to their corresponding multi types. 

Parameters:
schema_name		name	Schema name of geometry table.
table_name		name	Table name of geometry table.
geom_column		name	Geometry column name. Defaults to ''the_geom''.

Calls:
No nested calls';
