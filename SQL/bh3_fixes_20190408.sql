CREATE OR REPLACE FUNCTION public.bh3_habitat_boundary_clip(
	boundary_filter integer,
	habitat_types_filter character varying[],
	output_schema name,
	output_table name DEFAULT 'habitat_sensitivity'::name,
	habitat_schema name DEFAULT 'static'::name,
	habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name,
	sensitivity_schema name DEFAULT 'lut'::name,
	sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
	boundary_filter_negate boolean DEFAULT false,
	habitat_types_filter_negate boolean DEFAULT false,
	exclude_empty_mismatched_eunis_l3 boolean DEFAULT true,
	remove_overlaps boolean DEFAULT false,
	OUT success boolean,
	OUT exc_text character varying,
	OUT exc_detail character varying,
	OUT exc_hint character varying)
    RETURNS record
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
	start_time timestamp;
	temp_table_boundary_subdivide name;
	temp_table_habitat_boundary_intersect name;
	temp_table_habitat_boundary_intersect_union name;
 	error_rec record;
	rows_affected bigint;
	tn name;
	srid_hab integer;
	srid_bnd integer;
	geom_exp_hab character varying;
	geom_exp_bnd character varying;
	habitat_type_condition text;
	left_join character varying;
	negation text;

BEGIN
	BEGIN
		success := false;

		temp_table_boundary_subdivide := 'boundary_subdivide'::name;
		temp_table_habitat_boundary_intersect := 'habitat_boundary_intersect'::name;
		temp_table_habitat_boundary_intersect_union := 'habitat_boundary_intersect_union'::name;

		start_time := clock_timestamp();

		/* clean up any previous output left behind */
		FOR tn IN 
			EXECUTE format('SELECT c.relname '
						   'FROM pg_class c '
							   'JOIN pg_namespace n ON c.relnamespace = n.oid '
						   'WHERE n.nspname = $1 AND c.relname IN($2)')
			USING output_schema, output_table 
		LOOP
			EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING output_schema, tn;
		END LOOP;

		/* clean up any previous temp table left behind */
		CALL bh3_drop_temp_table(temp_table_boundary_subdivide);
		CALL bh3_drop_temp_table(temp_table_habitat_boundary_intersect);
		CALL bh3_drop_temp_table(temp_table_habitat_boundary_intersect_union);

		RAISE INFO 'bh3_habitat_boundary_clip: Removed any outputs of previous runs: %', (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		srid_bnd := bh3_find_srid(boundary_schema, boundary_table, 'the_geom'::name);
		IF srid_bnd != 4326 AND srid_hab > 0 THEN
			geom_exp_bnd := format('ST_Transform(the_geom,%s)', 4326);
		ELSE
			geom_exp_bnd := 'the_geom';
		END IF;

		IF boundary_filter_negate THEN
			negation = 'NOT';
		ELSE
			negation = '';
		END IF;

		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_subdiv AS '
					   '('
						   'SELECT ST_Subdivide(%2$s) AS the_geom '
						   'FROM %3$I.%4$I '
						   'WHERE %5$s gid = $1'
					   ') '
					   'SELECT ROW_NUMBER() OVER() AS gid'
						   ',the_geom '
					   'FROM cte_subdiv',
					   temp_table_boundary_subdivide, geom_exp_bnd, 
					   boundary_schema, boundary_table, negation)
		USING boundary_filter;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into temporary table %: %', 
			rows_affected, temp_table_boundary_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(NULL, temp_table_boundary_subdivide);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('ALTER TABLE %1$I ADD CONSTRAINT %1$s_pkey PRIMARY KEY(gid)', 
					   temp_table_boundary_subdivide);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', 
					   temp_table_boundary_subdivide);
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   temp_table_boundary_subdivide);

		RAISE INFO 'bh3_habitat_boundary_clip: Indexed temporary table %: %', 
			temp_table_boundary_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		srid_hab := bh3_find_srid(habitat_schema, habitat_table, 'the_geom'::name);
		IF srid_hab != 4326 AND srid_hab > 0 THEN
			geom_exp_hab := format('ST_Transform(hab.the_geom,%s)', 4326);
		ELSE
			geom_exp_hab := 'hab.the_geom';
		END IF;

		habitat_type_condition := '';
		IF habitat_types_filter IS NOT NULL AND array_length(habitat_types_filter, 1) > 0 THEN
			IF array_length(habitat_types_filter, 1) = 1 THEN
				IF habitat_types_filter_negate THEN
					habitat_type_condition := format('hab.%1$I != %2$L', 'eunis_l3', habitat_types_filter[1]);
				ELSE
					habitat_type_condition := format('hab.%1$I = %2$L', 'eunis_l3', habitat_types_filter[1]);
				END IF;
			ELSE
				IF habitat_types_filter_negate THEN
					habitat_type_condition := format('NOT hab.%1$I = ANY ($1)', 'eunis_l3');
				ELSE
					habitat_type_condition := format('hab.%1$I = ANY ($1)', 'eunis_l3');
				END IF;
			END IF;
		ELSIF exclude_empty_mismatched_eunis_l3 THEN
			habitat_type_condition := format('hab.%1$I IS NOT NULL', 'eunis_l3');
		END IF;

		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT hab.gid'
						   ',(ST_Dump(ST_Intersection(%2$s,bnd.the_geom))).geom AS the_geom'
						   ',hab.hab_type'
						   ',hab.eunis_l3 '
					   'FROM %3$I.%4$I hab '
						   'JOIN %5$I bnd ON ST_Intersects(hab.the_geom,bnd.the_geom) '
					   'WHERE %6$s',
					   temp_table_habitat_boundary_intersect, geom_exp_hab,
					   habitat_schema, habitat_table, 
					   temp_table_boundary_subdivide, habitat_type_condition)
		USING habitat_types_filter;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(NULL, temp_table_habitat_boundary_intersect);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT hab.gid'
						   ',ST_Multi(ST_Union(hab.the_geom)) AS the_geom'
						   ',hab.hab_type'
						   ',hab.eunis_l3 '
					   'FROM %2$I hab '
					   'WHERE the_geom IS NOT NULL '
						   'AND NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* %3$L '
					   'GROUP BY hab.gid'
						   ',hab.hab_type'
						   ',hab.eunis_l3',
					   temp_table_habitat_boundary_intersect_union,
					   temp_table_habitat_boundary_intersect,
					   'Polygon');

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect_union, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(NULL, temp_table_habitat_boundary_intersect_union);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect_union, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('ALTER TABLE %1$I ADD CONSTRAINT %1$s_pkey PRIMARY KEY(gid)',
					   temp_table_habitat_boundary_intersect_union);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)',
					   temp_table_habitat_boundary_intersect_union);
		EXECUTE format('CREATE INDEX idx_%1$s_eunis_l3gid ON %1$I USING BTREE(eunis_l3)',
					   temp_table_habitat_boundary_intersect_union);

		RAISE INFO 'bh3_habitat_boundary_clip: Indexed temporary table %: %', 
			temp_table_habitat_boundary_intersect_union, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create habitat sensitivity output table */
		EXECUTE format('CREATE TABLE %1$I.%2$I('
						   'gid bigserial NOT NULL PRIMARY KEY'
						   ',the_geom geometry(MultiPolygon,4326)'
						   ',hab_type character varying'
						   ',eunis_l3 character varying'
						   ',sensitivity_ab_su_num_max smallint'
						   ',confidence_ab_su_num smallint'
						   ',sensitivity_ab_ss_num_max smallint'
						   ',confidence_ab_ss_num smallint)',
					   output_schema, output_table);

		IF exclude_empty_mismatched_eunis_l3 THEN
			left_join := '';
		ELSE
			left_join := 'LEFT ';
		END IF;

		/* populate output table with clipped habitat geometries joined to sensitivity scores */
		EXECUTE format('INSERT INTO %1$I.%2$I '
					   '('
						   'gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
					   ') '
					   'SELECT hab.gid'
						   ',hab.the_geom'
						   ',hab.hab_type'
						   ',hab.eunis_l3'
						   ',sbsh.sensitivity_ab_su_num_max'
						   ',sbsh.confidence_ab_su_num'
						   ',sbsh.sensitivity_ab_ss_num_max'
						   ',sbsh.confidence_ab_ss_num '
					   'FROM %3$I hab '
						   '%4$s JOIN %5$I.%6$I sbsh ON hab.eunis_l3 = sbsh.eunis_l3_code', 
					   output_schema, output_table, temp_table_habitat_boundary_intersect_union,
					   left_join, sensitivity_schema, sensitivity_table);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into output table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* index habitat sensitivity output table */
		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', output_schema, output_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', output_schema, output_table);

		RAISE INFO 'bh3_habitat_boundary_clip: indexed output table %.%: %', 
			output_schema, output_table, (clock_timestamp() - start_time);

		/* if requested remove overlaps from habitat sensitivity output table */
		IF remove_overlaps THEN
			EXECUTE format('SELECT * FROM bh3_habitat_remove_overlaps($1,$2)')
			INTO error_rec
			USING output_schema, output_table;
			IF NOT error_rec.success THEN
				RAISE EXCEPTION 
					USING MESSAGE = error_rec.exc_text,
						DETAIL = error_rec.exc_detail, 
						HINT = error_rec.exc_hint;
			END IF;

			RAISE INFO 'bh3_habitat_boundary_clip: Removed overlaps from output table %.%: %', 
				output_schema, output_table, (clock_timestamp() - start_time);
			
			start_time := clock_timestamp();
		END IF;

		/* remove temp tables */
		CALL bh3_drop_temp_table(temp_table_boundary_subdivide);
		CALL bh3_drop_temp_table(temp_table_habitat_boundary_intersect);
		CALL bh3_drop_temp_table(temp_table_habitat_boundary_intersect_union);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_habitat_boundary_clip: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;





CREATE OR REPLACE FUNCTION public.bh3_species_sensitivity_clipped(
	boundary_schema name,
	boundary_table name,
	boundary_filter integer,
	sensitivity_source_table sensitivity_source,
	date_start timestamp without time zone,
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	date_end timestamp without time zone DEFAULT now(
	),
	boundary_filter_negate boolean DEFAULT false,
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
	negation text;
	geom_exp_smp text;
	habitat_type_condition text;

BEGIN
	IF boundary_filter_negate THEN
		negation = 'NOT';
	ELSE
		negation = '';
	END IF;

	srid_smp := bh3_find_srid(boundary_schema, boundary_table, 'the_geom');
	IF srid_smp != output_srid AND srid_smp > 0 AND output_srid > 0 THEN
		geom_exp_smp := format('ST_Transform(smp.%1$I,%2$s) AS %1$I', 'the_geom', output_srid);
	ELSE 
		geom_exp_smp := format('smp.%I', 'the_geom');
	END IF;

	habitat_type_condition := '';
	IF habitat_types_filter IS NOT NULL AND array_length(habitat_types_filter, 1) > 0 THEN
		IF array_length(habitat_types_filter, 1) = 1 THEN
			IF habitat_types_filter_negate THEN 
				habitat_type_condition := format('AND ect.%1$I != %2$L', 'eunis_code_2004', habitat_types_filter[1]);
			ELSE
				habitat_type_condition := format('AND ect.%1$I = %2$L', 'eunis_code_2004', habitat_types_filter[1]);
			END IF;
		ELSE
			IF habitat_types_filter_negate THEN
				habitat_type_condition := format('AND NOT hab.%1$I = ANY ($5)', 'eunis_code_2004');
			ELSE
				habitat_type_condition := format('AND ect.%1$I = ANY ($5)', 'eunis_code_2004');
			END IF;
		END IF;
	END IF;

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
								'WHERE %4$s bnd.gid = $2 '
									'AND smp.sample_date >= $3 '
									'AND smp.sample_date <= $4 '
									'%5$s',
								geom_exp_smp, boundary_schema, boundary_table, negation, habitat_type_condition)
	USING sensitivity_source_table, boundary_filter, date_start, date_end, habitat_types_filter;
END;
$BODY$;





CREATE OR REPLACE FUNCTION public.bh3_disturbance_map(
	boundary_filter integer,
	date_start timestamp without time zone,
	pressure_schema name,
	sensitivity_map_schema name,
	output_schema name,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
	sensitivity_map_table name DEFAULT 'sensitivity_map'::name,
	output_table name DEFAULT 'disturbance_map'::name,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	boundary_filter_negate boolean DEFAULT false,
	date_end timestamp without time zone DEFAULT now(
	),
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(gid bigint, exc_text character varying, exc_detail character varying, exc_hint character varying) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;
	start_time timestamp;
	pressure_csquares_table name;
	srid_sen integer;
	geom_exp_sen character varying;
	transform_geom boolean;
	area_sqm double precision;
	tn name;
	cand_cursor refcursor;
	cand_row record;
	i int;
	rec_count integer;
	error_count integer;
	error_gids bigint[];
	error_rec record;
	error_texts character varying[];
	error_details character varying[];
	error_hints character varying[];
	geom geometry;
	sqlstmt text;

BEGIN
	BEGIN
		start_time := clock_timestamp();
		
		pressure_csquares_table := 'pressure_csquares';

		RAISE INFO 'Deleting previous output table %.%', output_schema, output_table;
	
		/* clean up any previous output left behind */
		FOR tn IN 
			EXECUTE format('SELECT c.relname '
						   'FROM pg_class c '
							   'JOIN pg_namespace n ON c.relnamespace = n.oid '
						   'WHERE n.nspname = $1 AND c.relname IN($2)')
			USING output_schema, output_table 
		LOOP
			EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING output_schema, tn;
		END LOOP;

		RAISE INFO 'Deleting previous temp table %', pressure_csquares_table;
	
		/* drop any previous pressure c-squares temporary table left behind */
		CALL bh3_drop_temp_table(pressure_csquares_table);

		RAISE INFO 'Creating pressure grid table %', pressure_csquares_table;
	
		/* store pressure c-squares in temporary table */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT * FROM bh3_get_pressure_csquares($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
					   pressure_csquares_table)
		USING boundary_filter, pressure_schema, date_start, date_end, sar_surface_column, sar_subsurface_column, 
			boundary_schema, boundary_table, boundary_filter_negate, output_srid;
		
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', pressure_csquares_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', pressure_csquares_table);

		RAISE INFO 'Creating output table %.%', output_schema, output_table;

		/* create output pressure map table */
		EXECUTE format('CREATE TABLE %1$I.%2$I '
					   '('
						   'gid serial NOT NULL PRIMARY KEY'
						   ',the_geom geometry(MultiPolygon,%3$s)'
						   ',hab_type character varying'
						   ',eunis_l3 character varying'
						   ',sensitivity_ab_su_num smallint'
						   ',sensitivity_ab_ss_num smallint'
						   ',disturbance_ab_su smallint'
						   ',disturbance_ab_ss smallint'
						   ',disturbance_ab smallint'
						   ',area_sqm double precision'
					   ')',
					   output_schema, output_table, output_srid);

		RAISE INFO 'Finding SRID of sensitivity map table %.%', sensitivity_map_schema, sensitivity_map_table;

		/* build geometry expression for habitat aggregation query that projects geometry to 4326 if necessary */
		srid_sen := bh3_find_srid(sensitivity_map_schema, sensitivity_map_table, 'the_geom'::name);
		IF srid_sen != output_srid AND srid_sen > 0 AND output_srid > 0 THEN
			geom_exp_sen := format('ST_Transform(sen.the_geom,%s)', output_srid);
		ELSE
			geom_exp_sen := 'sen.the_geom';
		END IF;

		RAISE INFO 'SRID of sensitivity map %.%: %', sensitivity_map_schema, sensitivity_map_table, srid_sen;

		transform_geom := output_srid != 3035 AND output_srid > 0;

		sqlstmt := format('SELECT ROW_NUMBER() OVER() AS gid'
						   ',sen.hab_type'
						   ',sen.eunis_l3'
						   ',sen.sensitivity_ab_su_num'
						   ',sen.sensitivity_ab_ss_num'
						   ',CASE '
							   'WHEN sen.sensitivity_ab_su_num = 1 AND prs.sar_surface_cat_comb = 1 THEN 1 '
							   'WHEN sen.sensitivity_ab_su_num = 1 AND prs.sar_surface_cat_comb = 2 THEN 1 '
							   'WHEN sen.sensitivity_ab_su_num = 1 AND prs.sar_surface_cat_comb = 3 THEN 1 '
							   'WHEN sen.sensitivity_ab_su_num = 1 AND prs.sar_surface_cat_comb = 4 THEN 1 '
							   'WHEN sen.sensitivity_ab_su_num = 1 AND prs.sar_surface_cat_comb = 5 THEN 2 '
							   'WHEN sen.sensitivity_ab_su_num = 2 AND prs.sar_surface_cat_comb = 1 THEN 2 '
							   'WHEN sen.sensitivity_ab_su_num = 2 AND prs.sar_surface_cat_comb = 2 THEN 2 '
							   'WHEN sen.sensitivity_ab_su_num = 2 AND prs.sar_surface_cat_comb = 3 THEN 3 '
							   'WHEN sen.sensitivity_ab_su_num = 2 AND prs.sar_surface_cat_comb = 4 THEN 4 '
							   'WHEN sen.sensitivity_ab_su_num = 2 AND prs.sar_surface_cat_comb = 5 THEN 4 '
							   'WHEN sen.sensitivity_ab_su_num = 3 AND prs.sar_surface_cat_comb = 1 THEN 3 '
							   'WHEN sen.sensitivity_ab_su_num = 3 AND prs.sar_surface_cat_comb = 2 THEN 4 '
							   'WHEN sen.sensitivity_ab_su_num = 3 AND prs.sar_surface_cat_comb = 3 THEN 5 '
							   'WHEN sen.sensitivity_ab_su_num = 3 AND prs.sar_surface_cat_comb = 4 THEN 6 '
							   'WHEN sen.sensitivity_ab_su_num = 3 AND prs.sar_surface_cat_comb = 5 THEN 7 '
							   'WHEN sen.sensitivity_ab_su_num = 4 AND prs.sar_surface_cat_comb = 1 THEN 4 '
							   'WHEN sen.sensitivity_ab_su_num = 4 AND prs.sar_surface_cat_comb = 2 THEN 6 '
							   'WHEN sen.sensitivity_ab_su_num = 4 AND prs.sar_surface_cat_comb = 3 THEN 7 '
							   'WHEN sen.sensitivity_ab_su_num = 4 AND prs.sar_surface_cat_comb = 4 THEN 8 '
							   'WHEN sen.sensitivity_ab_su_num = 4 AND prs.sar_surface_cat_comb = 5 THEN 9 '
							   'WHEN sen.sensitivity_ab_su_num = 5 AND prs.sar_surface_cat_comb = 1 THEN 6 '
							   'WHEN sen.sensitivity_ab_su_num = 5 AND prs.sar_surface_cat_comb = 2 THEN 7 '
							   'WHEN sen.sensitivity_ab_su_num = 5 AND prs.sar_surface_cat_comb = 3 THEN 9 '
							   'WHEN sen.sensitivity_ab_su_num = 5 AND prs.sar_surface_cat_comb = 4 THEN 9 '
							   'WHEN sen.sensitivity_ab_su_num = 5 AND prs.sar_surface_cat_comb = 5 THEN 9 '
							   'ELSE 0 '
						   'END AS disturbance_ab_su'
						   ',CASE '
							   'WHEN sen.sensitivity_ab_ss_num = 1 AND prs.sar_subsurface_cat_comb = 1 THEN 1 '
							   'WHEN sen.sensitivity_ab_ss_num = 1 AND prs.sar_subsurface_cat_comb = 2 THEN 1 '
							   'WHEN sen.sensitivity_ab_ss_num = 1 AND prs.sar_subsurface_cat_comb = 3 THEN 1 '
							   'WHEN sen.sensitivity_ab_ss_num = 1 AND prs.sar_subsurface_cat_comb = 4 THEN 1 '
							   'WHEN sen.sensitivity_ab_ss_num = 1 AND prs.sar_subsurface_cat_comb = 5 THEN 2 '
							   'WHEN sen.sensitivity_ab_ss_num = 2 AND prs.sar_subsurface_cat_comb = 1 THEN 2 '
							   'WHEN sen.sensitivity_ab_ss_num = 2 AND prs.sar_subsurface_cat_comb = 2 THEN 2 '
							   'WHEN sen.sensitivity_ab_ss_num = 2 AND prs.sar_subsurface_cat_comb = 3 THEN 3 '
							   'WHEN sen.sensitivity_ab_ss_num = 2 AND prs.sar_subsurface_cat_comb = 4 THEN 4 '
							   'WHEN sen.sensitivity_ab_ss_num = 2 AND prs.sar_subsurface_cat_comb = 5 THEN 4 '
							   'WHEN sen.sensitivity_ab_ss_num = 3 AND prs.sar_subsurface_cat_comb = 1 THEN 3 '
							   'WHEN sen.sensitivity_ab_ss_num = 3 AND prs.sar_subsurface_cat_comb = 2 THEN 4 '
							   'WHEN sen.sensitivity_ab_ss_num = 3 AND prs.sar_subsurface_cat_comb = 3 THEN 5 '
							   'WHEN sen.sensitivity_ab_ss_num = 3 AND prs.sar_subsurface_cat_comb = 4 THEN 6 '
							   'WHEN sen.sensitivity_ab_ss_num = 3 AND prs.sar_subsurface_cat_comb = 5 THEN 7 '
							   'WHEN sen.sensitivity_ab_ss_num = 4 AND prs.sar_subsurface_cat_comb = 1 THEN 4 '
							   'WHEN sen.sensitivity_ab_ss_num = 4 AND prs.sar_subsurface_cat_comb = 2 THEN 6 '
							   'WHEN sen.sensitivity_ab_ss_num = 4 AND prs.sar_subsurface_cat_comb = 3 THEN 7 '
							   'WHEN sen.sensitivity_ab_ss_num = 4 AND prs.sar_subsurface_cat_comb = 4 THEN 8 '
							   'WHEN sen.sensitivity_ab_ss_num = 4 AND prs.sar_subsurface_cat_comb = 5 THEN 9 '
							   'WHEN sen.sensitivity_ab_ss_num = 5 AND prs.sar_subsurface_cat_comb = 1 THEN 6 '
							   'WHEN sen.sensitivity_ab_ss_num = 5 AND prs.sar_subsurface_cat_comb = 2 THEN 7 '
							   'WHEN sen.sensitivity_ab_ss_num = 5 AND prs.sar_subsurface_cat_comb = 3 THEN 9 '
							   'WHEN sen.sensitivity_ab_ss_num = 5 AND prs.sar_subsurface_cat_comb = 4 THEN 9 '
							   'WHEN sen.sensitivity_ab_ss_num = 5 AND prs.sar_subsurface_cat_comb = 5 THEN 9 '
							   'ELSE 0 '
						   'END AS disturbance_ab_ss'
						   ',%1$s AS geom_sen'
						   ',prs.the_geom AS geom_prs '
					   'FROM %2$I.%3$I sen '
						   'JOIN %4$I prs ON ST_Intersects(%1$s,prs.the_geom)',
					   geom_exp_sen, sensitivity_map_schema, sensitivity_map_table, pressure_csquares_table);

		RAISE INFO 'Opening cursor for %', sqlstmt;

		/* create cursor spatially joining sensitivity map and pressure c-squares */
		OPEN cand_cursor FOR
		EXECUTE sqlstmt;

		/* loop over cursor, intersecting sensitivity polygons with pressure grid squares (using fast ST_ClipByBox2D function) */
		rec_count := 0;
		
		LOOP
			BEGIN
				FETCH cand_cursor INTO cand_row;
				EXIT WHEN NOT FOUND;

				rec_count := rec_count + 1;
				RAISE INFO 'bh3_disturbance_map: Looping over cursor. Row: %. Runtime: %', rec_count, (clock_timestamp() - start_time);

				geom := ST_Multi(ST_ClipByBox2D(cand_row.geom_sen, cand_row.geom_prs));
				/* repair clipped geometry if necessary */
				IF NOT ST_IsValid(geom) THEN
					geom := ST_Multi(ST_Buffer(geom, 0));
				END IF;

				IF transform_geom THEN
					area_sqm := ST_Area(ST_Transform(geom, 3035));
				ELSE
					area_sqm := ST_Area(geom);
				END IF;

				EXECUTE format('INSERT INTO %1$I.%2$I '
							   '('
								   'gid'
								   ',the_geom'
								   ',hab_type'
								   ',eunis_l3'
								   ',sensitivity_ab_su_num'
								   ',sensitivity_ab_ss_num'
								   ',disturbance_ab_su'
								   ',disturbance_ab_ss'
								   ',disturbance_ab'
								   ',area_sqm'
							   ') '
							   'VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', 
							   output_schema, output_table)
				USING cand_row.gid
					,geom 
					,cand_row.hab_type
					,cand_row.eunis_l3
					,cand_row.sensitivity_ab_su_num
					,cand_row.sensitivity_ab_ss_num
					,cand_row.disturbance_ab_su
					,cand_row.disturbance_ab_ss
					,greatest(cand_row.disturbance_ab_su, cand_row.disturbance_ab_ss)
					,area_sqm;
			EXCEPTION WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
										  exc_detail = PG_EXCEPTION_DETAIL,
										  exc_hint = PG_EXCEPTION_HINT;
				error_count := error_count + 1;
				error_gids := error_gids || cand_row.gid::bigint;
				error_texts := error_texts || exc_text;
				error_details := error_details || exc_detail;
				error_hints := error_hints || exc_hint;
				RAISE INFO 'Error. Text: %. Detail: %. Hint: %.', exc_text, exc_detail, exc_hint;
			END;
		END LOOP;

		CLOSE cand_cursor;

		/* index pressure map output table */
		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', output_schema, output_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', output_schema, output_table);
		
		/* drop pressure c-squares temporary table */
		CALL bh3_drop_temp_table(pressure_csquares_table);

		/* create return table from error keys and messages collected in insert loop */
		i := 1;
		WHILE i <= rec_count LOOP
			IF error_gids[i] IS NOT NULL THEN
				SELECT error_gids[i], error_texts[i], error_details[i], error_hints[i] INTO error_rec;
				RETURN NEXT;
			END IF;
			i := i + 1;
		END LOOP;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'Error. Text: %. Detail: %. Hint: %.', exc_text, exc_detail, exc_hint;
	END;

	RETURN;
END;
$BODY$;
