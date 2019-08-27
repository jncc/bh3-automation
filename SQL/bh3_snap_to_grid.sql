-- PROCEDURE: public.bh3_snap_to_grid(name, name, name, double precision)

-- DROP PROCEDURE public.bh3_snap_to_grid(name, name, name, double precision);

CREATE OR REPLACE PROCEDURE public.bh3_snap_to_grid(
	schema_name name,
	table_name name,
	geom_column name DEFAULT 'the_geom'::name,
	grid_size double precision DEFAULT (
	0.0001)::double precision)
LANGUAGE 'plpgsql'

AS $BODY$
BEGIN
	IF length(coalesce(table_name, '')) > 0 AND length(coalesce(geom_column, '')) > 0 THEN
		IF length(coalesce(schema_name, '')) > 0 THEN
			EXECUTE format('UPDATE %1$I.%2$I '
						   'SET %3$I = ST_SnapToGrid(%3$I,%4s)',
						   schema_name, table_name, geom_column, grid_size);
			EXECUTE format('UPDATE %1$I.%2$I '
						   'SET %3$I = ST_MakeValid(ST_Multi(ST_Buffer('
							   'CASE '
								   'WHEN ST_IsCollection(%3$I) THEN ST_CollectionExtract(ST_MakeValid(%3$I),3) '
								   'ELSE ST_MakeValid(%3$I) '
							   'END, 0))) '
						   'WHERE NOT ST_IsValid(%3$I) OR NOT ST_IsSimple(%3$I) OR ST_IsCollection(%3$I)',
						   schema_name, table_name, geom_column);
		ELSE
			EXECUTE format('UPDATE %1$I '
						   'SET %2$I = ST_SnapToGrid(%2$I, %3s)',
						   table_name, geom_column, grid_size);
			EXECUTE format('UPDATE %1$I '
						   'SET %2$I = ST_MakeValid(ST_Multi(ST_Buffer('
							   'CASE '
								   'WHEN ST_IsCollection(%2$I) THEN ST_CollectionExtract(ST_MakeValid(%2$I),3) '
								   'ELSE ST_MakeValid(%2$I) '
							   'END, 0))) '
						   'WHERE NOT ST_IsValid(%2$I) OR NOT ST_IsSimple(%2$I) OR ST_IsCollection(%2$I)',
						   table_name, geom_column);
		END IF;
	END IF;
END;
$BODY$;

COMMENT ON PROCEDURE public.bh3_snap_to_grid
    IS 'Purpose:
Snaps the geometries in the specified column of a table to a grid of the specified size. 

Approach:
Calls the ST_SnapToGrid function. 

Parameters:
schema_name		name	Schema name of geometry table.
table_name		name	Table name of geometry table.
geom_column		name	Geometry column name. Defaults to ''the_geom''.
grid_size		float	Size of the grid cell to snap to. Defaults to 0.0001.

Calls:
No nested calls';
