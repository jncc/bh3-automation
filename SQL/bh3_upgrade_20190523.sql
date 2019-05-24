CREATE OR REPLACE FUNCTION public.bh3_species_sensitivity_clipped(
	boundary_schema name,
	sensitivity_source_table sensitivity_source,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(),
	boundary_table name DEFAULT 'boundary'::name,
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	habitat_types_filter_negate boolean DEFAULT false,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(gid bigint, the_geom geometry, survey_key character varying, survey_event_key character varying, sample_reference character varying, sample_date timestamp with time zone, biotope_code character varying, biotope_desc text, qualifier character varying, eunis_code_2007 character varying, eunis_name_2007 character varying, annex_i_habitat character varying, characterising_species character varying, sensitivity_ab_su_num smallint, confidence_ab_su_num smallint, sensitivity_ab_ss_num smallint, confidence_ab_ss_num smallint) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	srid_smp int;
	geom_exp_smp text;
	eunis_corr_table_eunis_code_column name;

BEGIN
	eunis_corr_table_eunis_code_column := 'eunis_code_2007';

	srid_smp := bh3_find_srid(boundary_schema, boundary_table, 'the_geom');
	IF srid_smp != output_srid AND srid_smp > 0 AND output_srid > 0 THEN
		geom_exp_smp := format('ST_Transform(smp.%1$I,%2$s) AS %1$I', 'the_geom', output_srid);
	ELSE 
		geom_exp_smp := format('smp.%I', 'the_geom');
	END IF;

	IF habitat_types_filter IS NULL OR array_length(habitat_types_filter, 1) = 0 THEN
		RETURN QUERY EXECUTE format('SELECT ROW_NUMBER() OVER() AS gid'
										',%1$s'
										',srv.survey_key'
										',sve.survey_event_key'
										',smp.sample_reference'
										',smp.sample_date'
										',sba.biotope_code'
										',sba.biotope_desc'
										',sba.qualifier'
										',ect.eunis_code_2007'
										',ect.eunis_name_2007'
										',ect.annex_i_habitat'
										',egr.characterising_species'
										',egr.sensitivity_ab_su_num'
										',egr.confidence_ab_su_num'
										',egr.sensitivity_ab_ss_num'
										',egr.confidence_ab_ss_num '
									'FROM marinerec.survey srv '
										'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
										'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
										'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
										'JOIN bh3_sensitivity($1) egr ON egr.characterising_species = spc.species_name '
										'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
										'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
										'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
									'WHERE smp.sample_date >= $2 '
										'AND smp.sample_date <= $3',
									geom_exp_smp, boundary_schema, boundary_table)
		USING sensitivity_source_table, date_start, date_end;
	ELSIF array_length(habitat_types_filter, 1) = 1 THEN
		RETURN QUERY EXECUTE format('SELECT ROW_NUMBER() OVER() AS gid'
										',%1$s'
										',srv.survey_key'
										',sve.survey_event_key'
										',smp.sample_reference'
										',smp.sample_date'
										',sba.biotope_code'
										',sba.biotope_desc'
										',sba.qualifier'
										',ect.eunis_code_2007'
										',ect.eunis_name_2007'
										',ect.annex_i_habitat'
										',egr.characterising_species'
										',egr.sensitivity_ab_su_num'
										',egr.confidence_ab_su_num'
										',egr.sensitivity_ab_ss_num'
										',egr.confidence_ab_ss_num '
									'FROM marinerec.survey srv '
										'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
										'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
										'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
										'JOIN bh3_sensitivity($1) egr ON egr.characterising_species = spc.species_name '
										'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
										'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
										'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
									'WHERE smp.sample_date >= $2 '
										'AND smp.sample_date <= $3 '
										'AND %4$s',
									geom_exp_smp, boundary_schema, boundary_table, 
									CASE
										WHEN habitat_types_filter_negate THEN format('ect.%1$I !~* concat(%2$L, %3$L)', eunis_corr_table_eunis_code_column, '^', habitat_types_filter[1])
										ELSE format('ect.%1$I ~* concat(%2$L, %3$L)', eunis_corr_table_eunis_code_column, '^', habitat_types_filter[1])
									END)
		USING sensitivity_source_table, date_start, date_end;
	ELSE
		IF habitat_types_filter_negate THEN 
			RETURN QUERY EXECUTE format('WITH cte_hab_filter AS '
										'('
											'SELECT unnest($1) AS eunis_l3'
										') '
										'SELECT ROW_NUMBER() OVER() AS gid'
											',%1$s'
											',srv.survey_key'
											',sve.survey_event_key'
											',smp.sample_reference'
											',smp.sample_date'
											',sba.biotope_code'
											',sba.biotope_desc'
											',sba.qualifier'
											',ect.eunis_code_2007'
											',ect.eunis_name_2007'
											',ect.annex_i_habitat'
											',egr.characterising_species'
											',egr.sensitivity_ab_su_num'
											',egr.confidence_ab_su_num'
											',egr.sensitivity_ab_ss_num'
											',egr.confidence_ab_ss_num '
										'FROM marinerec.survey srv '
											'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
											'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
											'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
											'JOIN bh3_sensitivity($2) egr ON egr.characterising_species = spc.species_name '
											'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
											'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
											'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
											'LEFT JOIN cte_hab_filter ON ect.%4$s ~* concat(''^'', cte_hab_filter.eunis_l3) '
										'WHERE smp.sample_date >= $3 '
											'AND smp.sample_date <= $4 '
											'AND cte_hab_filter.eunis_l3 IS NULL',
										geom_exp_smp, boundary_schema, boundary_table, eunis_corr_table_eunis_code_column)
			USING habitat_types_filter, sensitivity_source_table, date_start, date_end;
		ELSE
			RETURN QUERY EXECUTE format('WITH cte_hab_filter AS '
										'('
											'SELECT unnest($1) AS eunis_l3'
										') '
										'SELECT ROW_NUMBER() OVER() AS gid'
											',%1$s'
											',srv.survey_key'
											',sve.survey_event_key'
											',smp.sample_reference'
											',smp.sample_date'
											',sba.biotope_code'
											',sba.biotope_desc'
											',sba.qualifier'
											',ect.eunis_code_2007'
											',ect.eunis_name_2007'
											',ect.annex_i_habitat'
											',egr.characterising_species'
											',egr.sensitivity_ab_su_num'
											',egr.confidence_ab_su_num'
											',egr.sensitivity_ab_ss_num'
											',egr.confidence_ab_ss_num '
										'FROM marinerec.survey srv '
											'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
											'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
											'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
											'JOIN bh3_sensitivity($2) egr ON egr.characterising_species = spc.species_name '
											'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
											'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
											'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
											'JOIN cte_hab_filter ON ect.%4$s ~* concat(''^'', cte_hab_filter.eunis_l3) '
										'WHERE smp.sample_date >= $3 '
											'AND smp.sample_date <= $4',
										geom_exp_smp, boundary_schema, boundary_table, eunis_corr_table_eunis_code_column)
			USING habitat_types_filter, sensitivity_source_table, date_start, date_end;
		END IF;
	END IF;
END;
$BODY$;



CREATE OR REPLACE FUNCTION public.bh3_get_pressure_csquares(
	boundary_schema name,
	pressure_schema name,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(),
	boundary_table name DEFAULT 'boundary'::name,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(gid bigint, c_square character varying, n bigint, sar_surface_min double precision, sar_surface_max double precision, sar_surface_avg double precision, sar_surface_cat_min integer, sar_surface_cat_max integer, sar_surface_cat_range integer, sar_surface_variable boolean, sar_surface double precision, sar_surface_cat_comb integer, sar_subsurface_min double precision, sar_subsurface_max double precision, sar_subsurface_avg double precision, sar_subsurface_cat_min integer, sar_subsurface_cat_max integer, sar_subsurface_cat_range integer, sar_subsurface_variable boolean, sar_subsurface double precision, sar_subsurface_cat_comb integer, the_geom geometry) 
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

		OPEN rel_cursor FOR EXECUTE format(
			'SELECT c.relname '
			'FROM pg_class c '
				'JOIN pg_namespace n ON c.relnamespace = n.oid '
				'JOIN pg_attribute a ON c.oid = a.attrelid '
			'WHERE c.relkind = ''r'' OR c.relkind = ''v'' '
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
										 'WHERE %2$I >= $1 AND %2$I <= $2', 
										 'c_square', 'year', sar_surface_column, sar_subsurface_column, geom_exp, 
										 pressure_schema, rel_cursor_row.relname, boundary_schema, boundary_table,
										 geom_fld);
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

			RETURN QUERY EXECUTE sqlstmt USING start_year, end_year;
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



CREATE OR REPLACE PROCEDURE public.bh3_procedure(
	boundary_filter integer[],
	habitat_types_filter character varying[],
	date_start timestamp without time zone,
	species_sensitivity_source_table sensitivity_source,
	pressure_schema name,
	output_schema name,
	output_owner character varying DEFAULT NULL::character varying,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'unofficial_country_waters_simplified_wgs84'::name,
	habitat_schema name DEFAULT 'static'::name,
	habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name,
	habitat_sensitivity_lookup_schema name DEFAULT 'lut'::name,
	habitat_sensitivity_lookup_table name DEFAULT 'sensitivity_broadscale_habitats'::name,
	habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name,
	habitat_sensitivity_grid_table name DEFAULT 'habitat_sensitivity_grid'::name,
	species_sensitivity_max_table name DEFAULT 'species_sensitivity_max'::name,
	species_sensitivity_mode_table name DEFAULT 'species_sensitivity_mode'::name,
	sensitivity_map_table name DEFAULT 'sensitivity_map'::name,
	pressure_map_table name DEFAULT 'pressure_map'::name,
	disturbance_map_table name DEFAULT 'disturbance_map'::name,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	date_end timestamp without time zone DEFAULT now(),
	boundary_filter_negate boolean DEFAULT false,
	habitat_types_filter_negate boolean DEFAULT false,
	remove_overlaps boolean DEFAULT false,
	output_srid integer DEFAULT 4326)
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	start_time timestamp;
	error_rec record;
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;
	error_curs refcursor;
	error_curs_rec record;
	error_count bigint;
	cell_size_degrees numeric;
	boundary_subdivide_table name;
	boundary_subdivide_union_table name;

BEGIN
	start_time := clock_timestamp();
	error_count := 0;
	boundary_subdivide_table := 'boundary_subdivide'::name;
	boundary_subdivide_union_table := 'boundary'::name;
	
	BEGIN
		/* create output_schema if it doesn't already exist */
		IF output_owner IS NOT NULL THEN
			EXECUTE format('SET ROLE %1$I', output_owner);
			EXECUTE format('DROP SCHEMA IF EXISTS %1$I CASCADE', output_schema);
			EXECUTE format('CREATE SCHEMA IF NOT EXISTS %1$I AUTHORIZATION %2$I', output_schema, output_owner);
		ELSE
			EXECUTE format('CREATE SCHEMA IF NOT EXISTS %1$I', output_schema);
		END IF;

		/* drop any previous error_log table in output_schema */
		EXECUTE format('DROP TABLE IF EXISTS %1$I.error_log', output_schema);

		/* create error_log table in output_schema */
		EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I.error_log '
					   '('
						   'context text,'
						   'record_gid bigint,'
						   'exception_text text,'
						   'exception_detail text,'
						   'exception_hint text'
					   ')',
					   output_schema);

		RAISE INFO 'Calling bh3_get_pressure_csquares_size: %', (clock_timestamp() - start_time);

		/* determine the c-square size from the pressure tables */
		SELECT * 
		FROM bh3_get_pressure_csquares_size(
			pressure_schema
			,date_start
			,date_end
			,sar_surface_column
			,sar_subsurface_column
			,output_srid
		) INTO cell_size_degrees;
		
		IF cell_size_degrees IS NULL OR cell_size_degrees < 0 THEN
			RAISE INFO 'Failed to obtain a consisten cell size from tables in pressure schema %', pressure_schema;
			RETURN;
		END IF;

		RAISE INFO 'Calling bh3_boundary_subdivide: %', (clock_timestamp() - start_time);

		SELECT * 
		FROM bh3_boundary_subdivide(
			boundary_filter,
			output_schema,
			boundary_subdivide_union_table,
			boundary_subdivide_table,
			boundary_schema,
			boundary_table,
			boundary_filter_negate
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_boundary_subdivide', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN;
		END IF;

		RAISE INFO 'Calling bh3_habitat_boundary_clip: %', (clock_timestamp() - start_time);

		/* create habitat_sensitivity for selected AOI in output_schema */
		SELECT * 
		FROM bh3_habitat_boundary_clip(
			habitat_types_filter
			,output_schema
			,habitat_sensitivity_table
			,habitat_schema
			,habitat_table
			,habitat_sensitivity_lookup_schema
			,habitat_sensitivity_lookup_table
			,output_schema
			,boundary_subdivide_table
			,habitat_types_filter_negate
			,true
			,remove_overlaps
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_habitat_boundary_clip', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN;
		END IF;

		RAISE INFO 'Calling bh3_habitat_grid: %', (clock_timestamp() - start_time);

		/* grid habitat_sensitivity into create habitat_sensitivity_grid  */
		FOR error_curs_rec IN 
			SELECT * 
			FROM bh3_habitat_grid(
				output_schema
				,output_schema
				,output_schema
				,boundary_subdivide_union_table
				,habitat_sensitivity_table
				,habitat_sensitivity_grid_table
				,cell_size_degrees
			)
		LOOP
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'record_gid,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4,$5)',
				   output_schema)
			USING 'bh3_habitat_grid', error_curs_rec.gid, error_curs_rec.exc_text, 
				error_curs_rec.exc_detail, error_curs_rec.exc_hint;
			error_count := error_count + 1;
		END LOOP;

		RAISE INFO 'Calling bh3_sensitivity_layer_prep: %', (clock_timestamp() - start_time);

		/* create species_sensitivity_max and species_sensitivity_mode for selected AOI in 
		output_schema using habitat_sensitivity and habitat_sensitivity_grid created in presvious step */
		SELECT * 
		FROM bh3_sensitivity_layer_prep(
			output_schema
			,output_schema
			,output_schema
			,species_sensitivity_source_table
			,date_start
			,date_end
			,boundary_subdivide_union_table
			,habitat_types_filter
			,habitat_types_filter_negate
			,habitat_sensitivity_table
			,habitat_sensitivity_grid_table
			,species_sensitivity_max_table
			,species_sensitivity_mode_table
			,output_srid
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_sensitivity_layer_prep', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN;
		END IF;

		RAISE INFO 'Calling bh3_sensitivity_map: %', (clock_timestamp() - start_time);

		/* create sensitivity_map in output_schema from habitat_sensitivity, species_sensitivity_max 
		and species_sensitivity_mode tables, all located in schema output_schema */
		SELECT * 
		FROM bh3_sensitivity_map(
			output_schema
			,output_schema
			,output_schema
			,habitat_sensitivity_table
			,species_sensitivity_max_table
			,species_sensitivity_mode_table
			,sensitivity_map_table
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_sensitivity_map', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN;
		END IF;

		RAISE INFO 'Calling bh3_disturbance_map: %', (clock_timestamp() - start_time);

		/* create disturbance_map for selected AOI in output_schema from sensitivity_map 
		in output_schema and pressure tables in pressure_schema. */
		FOR error_curs_rec IN 
			SELECT *
			FROM bh3_disturbance_map(
				output_schema
				,pressure_schema
				,output_schema
				,output_schema
				,date_start
				,date_end
				,boundary_subdivide_union_table
				,sensitivity_map_table
				,pressure_map_table
				,disturbance_map_table
				,sar_surface_column
				,sar_subsurface_column
				,output_srid) 
		LOOP
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'record_gid,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4,$5)',
				   output_schema)
			USING 'bh3_disturbance_map', error_curs_rec.gid, error_curs_rec.exc_text, error_curs_rec.exc_detail, error_curs_rec.exc_hint;
			error_count := error_count + 1;
		END LOOP;

		IF error_count = 0 THEN
			EXECUTE format('DROP TABLE IF EXISTS %1$I.error_log', output_schema);
		END IF;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'Error text: %', exc_text;
		RAISE INFO 'Error detail: %', exc_detail;
		RAISE INFO 'Error hint: %', exc_hint;
	END;
	
	EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I', output_schema, boundary_subdivide_table);
	EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I', output_schema, boundary_subdivide_union_table);
	
	IF output_owner IS NOT NULL THEN
		RESET ROLE;
	END IF;

	RAISE INFO 'Completed in %', (clock_timestamp() - start_time);
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.bh3_entry(
	boundary_filter integer[],
	habitat_types_filter character varying[],
	date_start timestamp without time zone,
	species_sensitivity_source_table sensitivity_source,
	pressure_schema name,
	output_schema name,
	output_owner character varying DEFAULT NULL::character varying,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'unofficial_country_waters_simplified_wgs84'::name,
	habitat_schema name DEFAULT 'static'::name,
	habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name,
	habitat_sensitivity_lookup_schema name DEFAULT 'lut'::name,
	habitat_sensitivity_lookup_table name DEFAULT 'sensitivity_broadscale_habitats'::name,
	habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name,
	habitat_sensitivity_grid_table name DEFAULT 'habitat_sensitivity_grid'::name,
	species_sensitivity_max_table name DEFAULT 'species_sensitivity_max'::name,
	species_sensitivity_mode_table name DEFAULT 'species_sensitivity_mode'::name,
	sensitivity_map_table name DEFAULT 'sensitivity_map'::name,
	pressure_map_table name DEFAULT 'pressure_map'::name,
	disturbance_map_table name DEFAULT 'disturbance_map'::name,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	date_end timestamp without time zone DEFAULT now(),
	boundary_filter_negate boolean DEFAULT false,
	habitat_types_filter_negate boolean DEFAULT false,
	remove_overlaps boolean DEFAULT false,
	output_srid integer DEFAULT 4326)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
	start_time timestamp;
	error_rec record;
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;
	error_curs refcursor;
	error_curs_rec record;
	error_count bigint;
	cell_size_degrees numeric;
	boundary_subdivide_table name;
	boundary_subdivide_union_table name;

BEGIN
	start_time := clock_timestamp();
	error_count := 0;
	boundary_subdivide_table := 'boundary_subdivide'::name;
	boundary_subdivide_union_table := 'boundary'::name;

	BEGIN
		/* create output_schema if it doesn't already exist */
		IF output_owner IS NOT NULL THEN
			EXECUTE format('SET ROLE %1$I', output_owner);
			EXECUTE format('DROP SCHEMA IF EXISTS %1$I CASCADE', output_schema);
			EXECUTE format('CREATE SCHEMA IF NOT EXISTS %1$I AUTHORIZATION %2$I', output_schema, output_owner);
		ELSE
			EXECUTE format('CREATE SCHEMA IF NOT EXISTS %1$I', output_schema);
		END IF;

		/* drop any previous error_log table in output_schema */
		EXECUTE format('DROP TABLE IF EXISTS %1$I.error_log', output_schema);

		/* create error_log table in output_schema */
		EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I.error_log '
					   '('
						   'context text,'
						   'record_gid bigint,'
						   'exception_text text,'
						   'exception_detail text,'
						   'exception_hint text'
					   ')',
					   output_schema);

		RAISE INFO 'Calling bh3_get_pressure_csquares_size: %', (clock_timestamp() - start_time);

		/* determine the c-square size from the pressure tables */
		SELECT * 
		FROM bh3_get_pressure_csquares_size(
			pressure_schema
			,date_start
			,date_end
			,sar_surface_column
			,sar_subsurface_column
			,output_srid
		) INTO cell_size_degrees;

		IF cell_size_degrees IS NULL OR cell_size_degrees < 0 THEN
			RAISE INFO 'Failed to obtain a consisten cell size from tables in pressure schema %', pressure_schema;
			RETURN false;
		END IF;

		RAISE INFO 'Calling bh3_boundary_subdivide: %', (clock_timestamp() - start_time);

		SELECT * 
		FROM bh3_boundary_subdivide(
			boundary_filter,
			output_schema,
			boundary_subdivide_union_table,
			boundary_subdivide_table,
			boundary_schema,
			boundary_table,
			boundary_filter_negate
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_boundary_subdivide', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN false;
		END IF;

		RAISE INFO 'Calling bh3_habitat_boundary_clip: %', (clock_timestamp() - start_time);

		/* create habitat_sensitivity for selected AOI in output_schema */
		SELECT * 
		FROM bh3_habitat_boundary_clip(
			habitat_types_filter
			,output_schema
			,habitat_sensitivity_table
			,habitat_schema
			,habitat_table
			,habitat_sensitivity_lookup_schema
			,habitat_sensitivity_lookup_table
			,output_schema
			,boundary_subdivide_table
			,habitat_types_filter_negate
			,true
			,remove_overlaps
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_habitat_boundary_clip', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN false;
		END IF;

		RAISE INFO 'Calling bh3_habitat_grid: %', (clock_timestamp() - start_time);

		/* grid habitat_sensitivity into create habitat_sensitivity_grid  */
		FOR error_curs_rec IN 
			SELECT * 
			FROM bh3_habitat_grid(
				output_schema
				,output_schema
				,output_schema
				,boundary_subdivide_union_table
				,habitat_sensitivity_table
				,habitat_sensitivity_grid_table
				,cell_size_degrees
			)
		LOOP
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'record_gid,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4,$5)',
				   output_schema)
			USING 'bh3_habitat_grid', error_curs_rec.gid, error_curs_rec.exc_text, 
				error_curs_rec.exc_detail, error_curs_rec.exc_hint;
			error_count := error_count + 1;
		END LOOP;

		RAISE INFO 'Calling bh3_sensitivity_layer_prep: %', (clock_timestamp() - start_time);

		/* create species_sensitivity_max and species_sensitivity_mode for selected AOI in 
		output_schema using habitat_sensitivity and habitat_sensitivity_grid created in presvious step */
		SELECT * 
		FROM bh3_sensitivity_layer_prep(
			output_schema
			,output_schema
			,output_schema
			,species_sensitivity_source_table
			,date_start
			,date_end
			,boundary_subdivide_union_table
			,habitat_types_filter
			,habitat_types_filter_negate
			,habitat_sensitivity_table
			,habitat_sensitivity_grid_table
			,species_sensitivity_max_table
			,species_sensitivity_mode_table
			,output_srid
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_sensitivity_layer_prep', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN false;
		END IF;

		RAISE INFO 'Calling bh3_sensitivity_map: %', (clock_timestamp() - start_time);

		/* create sensitivity_map in output_schema from habitat_sensitivity, species_sensitivity_max 
		and species_sensitivity_mode tables, all located in schema output_schema */
		SELECT * 
		FROM bh3_sensitivity_map(
			output_schema
			,output_schema
			,output_schema
			,habitat_sensitivity_table
			,species_sensitivity_max_table
			,species_sensitivity_mode_table
			,sensitivity_map_table
		) INTO error_rec;

		IF error_rec IS NOT NULL AND NOT error_rec.success THEN
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4)',
				   output_schema)
			USING 'bh3_sensitivity_map', error_rec.exc_text, error_rec.exc_detail, error_rec.exc_hint;
			error_count := error_count + 1;
			RETURN false;
		END IF;

		RAISE INFO 'Calling bh3_disturbance_map: %', (clock_timestamp() - start_time);

		/* create disturbance_map for selected AOI in output_schema from sensitivity_map 
		in output_schema and pressure tables in pressure_schema. */
		FOR error_curs_rec IN 
			SELECT *
			FROM bh3_disturbance_map(
				output_schema
				,pressure_schema
				,output_schema
				,output_schema
				,date_start
				,date_end
				,boundary_subdivide_union_table
				,sensitivity_map_table
				,pressure_map_table
				,disturbance_map_table
				,sar_surface_column
				,sar_subsurface_column
				,output_srid) 
		LOOP
			EXECUTE format('INSERT INTO %1$I.error_log '
						   '('
							   'context,'
							   'record_gid,'
							   'exception_text,'
							   'exception_detail,'
							   'exception_hint'
						   ') '
						   'VALUES ($1,$2,$3,$4,$5)',
				   output_schema)
			USING 'bh3_disturbance_map', error_curs_rec.gid, error_curs_rec.exc_text, error_curs_rec.exc_detail, error_curs_rec.exc_hint;
			error_count := error_count + 1;
		END LOOP;

		IF error_count = 0 THEN
			EXECUTE format('DROP TABLE IF EXISTS %1$I.error_log', output_schema);
		END IF;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'Error text: %', exc_text;
		RAISE INFO 'Error detail: %', exc_detail;
		RAISE INFO 'Error hint: %', exc_hint;
	END;

	EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I', output_schema, boundary_subdivide_table);
	EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I', output_schema, boundary_subdivide_union_table);

	RAISE INFO 'Completed in %', (clock_timestamp() - start_time);
	
	IF output_owner IS NOT NULL THEN
		RESET ROLE;
	END IF;

	RETURN true;
END;
$BODY$;



ALTER TABLE lut.sensitivity_rock_eco_groups2
	RENAME TO sensitivity_eco_groups_rock;

ALTER TABLE lut.sensitivity_eco_groups_rock
    ADD CONSTRAINT fk_sensitivity_eco_groups_rock_sensitivity_ab_ss_num FOREIGN KEY (sensitivity_ab_ss_num)
    REFERENCES lut.sensitivity_categorical_scores (score) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;

ALTER TABLE lut.sensitivity_eco_groups_rock
    ADD CONSTRAINT fk_sensitivity_eco_groups_rock_sensitivity_ab_su_num FOREIGN KEY (sensitivity_ab_su_num)
    REFERENCES lut.sensitivity_categorical_scores (score) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;

-- DROP INDEX lut.fki_fk_sensitivity_eco_groups_rock_sensitivity_ab_ss_num;

CREATE INDEX fki_fk_sensitivity_eco_groups_rock_sensitivity_ab_ss_num
    ON lut.sensitivity_eco_groups_rock USING btree
    (sensitivity_ab_ss_num)
    TABLESPACE pg_default;

-- Index: fki_fk_sensitivity_eco_groups_rock_sensitivity_ab_su_num

-- DROP INDEX lut.fki_fk_sensitivity_eco_groups_rock_sensitivity_ab_su_num;

CREATE INDEX fki_fk_sensitivity_eco_groups_rock_sensitivity_ab_su_num
    ON lut.sensitivity_eco_groups_rock USING btree
    (sensitivity_ab_su_num)
    TABLESPACE pg_default;

-- Index: ix_sensitivity_eco_groups_rock_characterising_species_unique

-- DROP INDEX lut.ix_sensitivity_eco_groups_rock_characterising_species_unique;

CREATE UNIQUE INDEX ix_sensitivity_eco_groups_rock_characterising_species_unique
    ON lut.sensitivity_eco_groups_rock USING btree
    (characterising_species COLLATE pg_catalog."default")
    TABLESPACE pg_default;

ALTER TABLE lut.sensitivity_eco_groups_rock
    CLUSTER ON ix_sensitivity_eco_groups_rock_characterising_species_unique;



ALTER TYPE public.sensitivity_source
    OWNER TO bh3;

ALTER TYPE public.sensitivity_source
    ADD VALUE 'eco_groups_rock' AFTER 'rock_eco_groups';



CREATE OR REPLACE FUNCTION public.bh3_sensitivity(
	source_table sensitivity_source)
    RETURNS TABLE(eunis_l3_code character varying, eunis_l3_name text, characterising_species character varying, sensitivity_ab_su_num smallint, sensitivity_ab_ss_num smallint, confidence_ab_su_num smallint, confidence_ab_ss_num smallint) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
BEGIN
	CASE source_table
		WHEN 'broadscale_habitats' THEN
			RETURN QUERY 
			SELECT eunis_l3_code
				,NULL::text AS eunis_l3_name
				,NULL::character varying AS characterising_species
				,sensitivity_ab_su_num_max AS sensitivity_ab_su_num
				,sensitivity_ab_ss_num_max AS sensitivity_ab_ss_num
				,confidence_ab_su_num
				,confidence_ab_ss_num
			FROM lut.sensitivity_broadscale_habitats;
		WHEN 'eco_groups' THEN 
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,characterising_species
				,sensitivity_ab_su_num
				,sensitivity_ab_ss_num
				,confidence_ab_su_num
				,confidence_ab_ss_num
			FROM lut.sensitivity_eco_groups;
		WHEN 'eco_groups_rock' THEN
			RETURN QUERY
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,characterising_species
				,sensitivity_ab_su_num
				,sensitivity_ab_ss_num
				,confidence_ab_su_num
				,confidence_ab_ss_num
			FROM lut.sensitivity_eco_groups_rock;
		WHEN 'rock' THEN
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,species_name AS characterising_species
				,sensitivity_ab_su_num
				,NULL::smallint AS sensitivity_ab_ss_num
				,confidence_ab_ss_num
				,NULL::smallint AS confidence_ab_ss_num
			FROM lut.sensitivity_rock;
		WHEN 'rock_eco_groups' THEN
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,coalesce(r.species_name,e.characterising_species) AS characterising_species
				,CASE 
					WHEN r.sensitivity_ab_su_num IS NOT NULL AND e.sensitivity_ab_su_num IS NOT NULL THEN 
						CASE
							WHEN r.confidence_ab_su_num > e.confidence_ab_su_num THEN r.sensitivity_ab_su_num 
							WHEN r.confidence_ab_su_num < e.confidence_ab_su_num THEN e.sensitivity_ab_su_num
							ELSE greatest(r.sensitivity_ab_su_num,e.sensitivity_ab_su_num)
						END
					ELSE
						coalesce(r.sensitivity_ab_su_num,e.sensitivity_ab_su_num) 
				END AS sensitivity_ab_su_num
				,e.sensitivity_ab_ss_num
				,greatest(r.confidence_ab_su_num,e.confidence_ab_su_num) AS confidence_ab_su_num
				,e.confidence_ab_ss_num
			FROM lut.sensitivity_rock r
				FULL JOIN lut.sensitivity_eco_groups e ON r.species_name = e.characterising_species;
		ELSE
			RETURN;
	END CASE;
END;
$BODY$;
