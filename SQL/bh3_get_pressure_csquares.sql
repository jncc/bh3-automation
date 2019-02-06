-- FUNCTION: public.bh3_get_pressure_csquares(integer, name, timestamp without time zone, timestamp without time zone, name, name, name, name, boolean, integer)

-- DROP FUNCTION public.bh3_get_pressure_csquares(integer, name, timestamp without time zone, timestamp without time zone, name, name, name, name, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_get_pressure_csquares(
	boundary_filter integer,
	pressure_schema name,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(),
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
	boundary_filter_negate boolean DEFAULT false,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(
		gid bigint,
		c_square character varying,
		n bigint, sar_surface_min double precision,
		sar_surface_max double precision,
		sar_surface_avg double precision,
		sar_surface_cat_min integer,
		sar_surface_cat_max integer,
		sar_surface_cat_range integer,
		sar_surface_variable boolean,
		sar_surface double precision,
		sar_surface_cat_comb integer,
		sar_subsurface_min double precision,
		sar_subsurface_max double precision,
		sar_subsurface_avg double precision,
		sar_subsurface_cat_min integer,
		sar_subsurface_cat_max integer,
		sar_subsurface_cat_range integer,
		sar_subsurface_variable boolean,
		sar_subsurface double precision,
		sar_subsurface_cat_comb integer,
		the_geom geometry) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	exc_text text;
	exc_detail text;
	exc_hint text;
	start_year integer;
	end_year integer;
	negation character varying;
	srid int;
	geom_fld name;
	geom_exp text;
	rel_cursor refcursor;
	rel_cursor_row record;
	sqlstmt text;

BEGIN
	BEGIN
		geom_fld := 'the_geom'::name;
		sqlstmt := '';

		start_year := extract(year from date_start)::integer;
		end_year := extract(year from date_end)::integer;

		IF boundary_filter_negate THEN
			negation = 'NOT';
		ELSE
			negation = '';
		END IF;

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
		USING 'c_square', 'year', sar_surface_column, sar_subsurface_column, 'the_geom', pressure_schema;

  		LOOP
  			FETCH rel_cursor INTO rel_cursor_row;
  			EXIT WHEN NOT FOUND;

			srid := bh3_find_srid(pressure_schema, rel_cursor_row.relname);
			IF srid != output_srid AND srid != 0 AND output_srid != 0 THEN
				geom_exp := format('ST_Transform(prs.%1$I,%2$s)', geom_fld, output_srid);
			ELSE
				geom_exp := format('prs.%1$I', geom_fld);
			END IF;

			sqlstmt := sqlstmt || format('UNION '
										 'SELECT %1$I'
											 ',%2$I'
											 ',%3$I AS sar_surface'
											 ',CASE '
												 'WHEN %3$I > 0.00 AND %3$I <= 0.33 THEN 1 '
												 'WHEN %3$I > 0.33 AND %3$I <= 0.66 THEN 2 '
												 'WHEN %3$I > 0.66 AND %3$I <= 1.00 THEN 3 '
												 'WHEN %3$I > 1.00 AND %3$I <= 3.00 THEN 4 '
												 'WHEN %3$I > 3.00 THEN 5 '
												 'ELSE 0 '
											 'END AS sar_surface_cat'
											 ',%4$I AS sar_subsurface'
											 ',CASE '
												 'WHEN %4$I > 0.00 AND %4$I <= 0.33 THEN 1 '
												 'WHEN %4$I > 0.33 AND %4$I <= 0.66 THEN 2 '
												 'WHEN %4$I > 0.66 AND %4$I <= 1.00 THEN 3 '
												 'WHEN %4$I > 1.00 AND %4$I <= 3.00 THEN 4 '
												 'WHEN %4$I > 3.00 THEN 5 '
												 'ELSE 0 '
											 'END AS sar_subsurface_cat'
											 ',%5$s AS the_geom '
										 'FROM %6$I.%7$I prs '
											 'JOIN %8$I.%9$I bnd ON ST_Intersects(%5$s,bnd.%10$I) '
										 'WHERE %11$s bnd.gid = $1 '
											 'AND %2$I >= $2 AND %2$I <= $3', 
										 'c_square', 'year', sar_surface_column, sar_subsurface_column, geom_exp, 
										 pressure_schema, rel_cursor_row.relname, boundary_schema, boundary_table,
										 geom_fld, negation);
  		END LOOP;

		CLOSE rel_cursor;

		IF length(sqlstmt) > 0 THEN
			sqlstmt := format('WITH cte_union AS '
							  '(' 
								  '%1$s'
							  '),'
							  'cte_agg AS '
							  '('
								  'SELECT c_square'
									  ',count(*) AS N'
									  ',min(sar_surface) AS sar_surface_min'
									  ',max(sar_surface) AS sar_surface_max'
									  ',avg(sar_surface) AS sar_surface_avg'
									  ',min(sar_surface_cat) AS sar_surface_cat_min'
									  ',max(sar_surface_cat) AS sar_surface_cat_max'
									  ',max(sar_surface_cat) - min(sar_surface_cat) AS sar_surface_cat_range'
									  ',(max(sar_surface_cat) - min(sar_surface_cat) >= 4) AS sar_surface_variable'
									  ',CASE '
										  'WHEN max(sar_surface_cat) - min(sar_surface_cat) >= 4 THEN max(sar_surface) '
										  'ELSE avg(sar_surface) '
									  'END AS sar_surface'
									  ',min(sar_subsurface) AS sar_subsurface_min'
									  ',max(sar_subsurface) AS sar_subsurface_max'
									  ',avg(sar_subsurface) AS sar_subsurface_avg'
									  ',min(sar_subsurface_cat) AS sar_subsurface_cat_min'
									  ',max(sar_subsurface_cat) AS sar_subsurface_cat_max'
									  ',max(sar_subsurface_cat) - min(sar_subsurface_cat) AS sar_subsurface_cat_range'
									  ',(max(sar_subsurface_cat) - min(sar_subsurface_cat) >= 4) AS sar_subsurface_variable'
									  ',CASE '
										  'WHEN max(sar_subsurface_cat) - min(sar_subsurface_cat) >= 4 THEN max(sar_subsurface) '
										  'ELSE avg(sar_subsurface) '
									  'END AS sar_subsurface'
									  ',(ST_Accum(the_geom))[1] AS the_geom '
								  'FROM cte_union u '
								  'GROUP BY c_square'
							  ') '
							  'SELECT ROW_NUMBER() OVER() AS gid'
								  ',c_square'
								  ',N'
								  ',sar_surface_min'
								  ',sar_surface_max'
								  ',sar_surface_avg'
								  ',sar_surface_cat_min'
								  ',sar_surface_cat_max'
								  ',sar_surface_cat_range'
								  ',sar_surface_variable'
								  ',sar_surface'
								  ',CASE '
									  'WHEN sar_surface > 0.00 AND sar_surface <= 0.33 THEN 1 '
									  'WHEN sar_surface > 0.33 AND sar_surface <= 0.66 THEN 2 '
									  'WHEN sar_surface > 0.66 AND sar_surface <= 1.00 THEN 3 '
									  'WHEN sar_surface > 1.00 AND sar_surface <= 3.00 THEN 4 '
									  'WHEN sar_surface > 3.00 THEN 5 '
									  'ELSE 0 '
								  'END AS sar_surface_cat_comb'
								  ',sar_subsurface_min'
								  ',sar_subsurface_max'
								  ',sar_subsurface_avg'
								  ',sar_subsurface_cat_min'
								  ',sar_subsurface_cat_max'
								  ',sar_subsurface_cat_range'
								  ',sar_subsurface_variable'
								  ',sar_subsurface'
								  ',CASE '
									  'WHEN sar_subsurface > 0.00 AND sar_subsurface <= 0.33 THEN 1 '
									  'WHEN sar_subsurface > 0.33 AND sar_subsurface <= 0.66 THEN 2 '
									  'WHEN sar_subsurface > 0.66 AND sar_subsurface <= 1.00 THEN 3 '
									  'WHEN sar_subsurface > 1.00 AND sar_subsurface <= 3.00 THEN 4 '
									  'WHEN sar_subsurface > 3.00 THEN 5 '
									  'ELSE 0 '
								  'END AS sar_subsurface_cat_comb'
								  ',the_geom '
							  'FROM cte_agg',
							  substring(sqlstmt FROM 7));
		
			RAISE INFO 'bh3_get_pressure_csquares: sqlstmt: %', sqlstmt;

			RETURN QUERY EXECUTE sqlstmt USING boundary_filter, start_year, end_year;
		END IF;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_get_pressure_csquares: Exception. Message: %. Detail: %. Hint: %', 
										exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_get_pressure_csquares(integer, name, timestamp without time zone, timestamp without time zone, name, name, name, name, boolean, integer)
    OWNER TO postgres;
