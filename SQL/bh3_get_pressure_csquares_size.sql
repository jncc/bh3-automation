-- FUNCTION: public.bh3_get_pressure_csquares_size(name, timestamp without time zone, timestamp without time zone, name, name, integer)

-- DROP FUNCTION public.bh3_get_pressure_csquares_size(name, timestamp without time zone, timestamp without time zone, name, name, integer);

CREATE OR REPLACE FUNCTION public.bh3_get_pressure_csquares_size(
	pressure_schema name,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(
	),
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	output_srid integer DEFAULT 4326)
    RETURNS numeric
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
	exc_text text;
	exc_detail text;
	exc_hint text;
	geom_fld character varying;
	start_year integer;
	end_year integer;
	srid int;
	geom_exp text;
	rel_cursor refcursor;
	rel_cursor_row record;
	sqlstmt text;
	cell_size double precision;

BEGIN
	cell_size := -1;

	BEGIN
		geom_fld := 'the_geom';
		sqlstmt := '';

		start_year := extract(year from date_start)::integer;
		end_year := extract(year from date_end)::integer;

		OPEN rel_cursor FOR EXECUTE format(
			'SELECT c.relname '
			'FROM pg_class c '
				'JOIN pg_namespace n ON c.relnamespace = n.oid '
				'JOIN pg_attribute a ON c.oid = a.attrelid '
			'WHERE c.relkind = ''r'' '
				'AND a.attname IN ($1,$2,$3,$4,$5) '
				'AND n.nspname = $6 '
			'GROUP BY c.relname '
			'HAVING count(*) = 5 '
			'ORDER BY c.relname')
		USING 'c_square', 'year', sar_surface_column, sar_subsurface_column, geom_fld, pressure_schema;

  		LOOP
  			FETCH rel_cursor INTO rel_cursor_row;
  			EXIT WHEN NOT FOUND;

			srid := bh3_find_srid(pressure_schema, rel_cursor_row.relname);
			IF srid != output_srid AND srid != 0 AND output_srid != 0 THEN
				geom_exp := format('ST_Transform(%1$I,%2$s)', geom_fld, output_srid);
			ELSE
				geom_exp := format('%1$I', geom_fld);
			END IF;

			sqlstmt := sqlstmt || format('UNION '
										 'SELECT %1$s AS %2$I '
										 'FROM %3$I.%4$I '
										 'WHERE %5$I >= $1 '
											 'AND %5$I <= $2 ', 
										 geom_exp, geom_fld, pressure_schema, rel_cursor_row.relname, 'year');
  		END LOOP;

		CLOSE rel_cursor;

		IF length(sqlstmt) > 6 THEN
			sqlstmt := format('WITH cte_union AS '
							  '(' 
								  '%1$s'
							  '),'
							  'cte_comp AS '
							  '('
								  'SELECT ST_XMax(%2$I) - ST_XMin(%2$I) AS width'
									  ',ST_YMax(%2$I) - ST_YMin(%2$I) AS height '
								  'FROM cte_union'
							  '),'
							  'cte_agg AS '
							  '('
								  'SELECT round(avg(width)::numeric,8) AS width'
									  ',round(avg(height)::numeric,8) AS height'
									  ',stddev(width) AS width_stdev'
									  ',stddev(height) AS height_stdev '
								  'FROM cte_comp '
								  'HAVING stddev(width) < 0.00000001 '
									  'AND stddev(height) < 0.00000001'
							  ') '
							  'SELECT height AS cell_size '
							  'FROM cte_agg '
							  'WHERE width = height '
								  'AND width_stdev < 0.00000001 '
								  'AND height_stdev < 0.00000001',
							  substring(sqlstmt FROM 7), geom_fld);
		
			RAISE INFO 'bh3_get_pressure_csquares_size: sqlstmt: %', sqlstmt;

			EXECUTE sqlstmt INTO cell_size USING start_year, end_year;
		END IF;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_get_pressure_csquares_size: Exception. Message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
		
	RETURN cell_size;
END;
$BODY$;

ALTER FUNCTION public.bh3_get_pressure_csquares_size(name, timestamp without time zone, timestamp without time zone, name, name, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_get_pressure_csquares_size(name, timestamp without time zone, timestamp without time zone, name, name, integer)
    IS 'Purpose:
Obtains the size of c-squares from the geometries of the tables in the selected pressure schema.

Approach:
A union query averages the width and height of [multi-]polygon geometries in all tables in the selected pressure schema.
The query is the same as used in bh3_get_pressure_csquares except for the boundary filter, which is unnecessary and skipped for performance reasons.
The polygons are expected to be squares of equal size. The average width/height is returned as long as their standard deviation is less
than 0.00000001.

Parameters:
pressure_schema			name							Name of the schema that holds the pressure tables.
date_start				timestamp without time zone		Earliest date for squares to be included.
date_end				timestamp without time zone		Latest  date for squares to be included. Defaults to current date and time.
sar_surface_column		name							Name of the surface SAR column. Defaults to ''sar_surface''.
sar_subsurface_column	name							Name of the sub-surface SAR column. Defaults to ''sar_subsurface''.
output_srid				integer 						SRID of spatial reference system in which c-squares are to be measured. Defaults to 4326.

Returns:
Cell size in the units of the spatial reference system identified by output_srid (normally degrees).

Calls:
bh3_find_srid';
