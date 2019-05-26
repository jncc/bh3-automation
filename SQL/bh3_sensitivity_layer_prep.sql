-- FUNCTION: public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, integer, integer, name, character varying[], boolean, name, name, name, name, integer)

-- DROP FUNCTION public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, integer, integer, name, character varying[], boolean, name, name, name, name, integer);

CREATE OR REPLACE FUNCTION public.bh3_sensitivity_layer_prep(
	boundary_schema name,
	habitat_schema name,
	output_schema name,
	sensitivity_source_table sensitivity_source,
	start_year integer,
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
	boundary_table name DEFAULT 'boundary'::name,
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	habitat_types_filter_negate boolean DEFAULT false,
	habitat_table name DEFAULT 'habitat_sensitivity'::name,
	habitat_table_grid name DEFAULT 'habitat_sensitivity_grid'::name,
	output_table_max name DEFAULT 'species_sensitivity_max'::name,
	output_table_mode name DEFAULT 'species_sensitivity_mode'::name,
	output_srid integer DEFAULT 4326,
	OUT success boolean,
	OUT exc_text text,
	OUT exc_detail text,
	OUT exc_hint text)
    RETURNS record
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
	start_time timestamp;
	rows_affected bigint;
	tn name;
	species_clip_table name;
	srid_hab int;
	geom_exp_hab text;
	transform_exp text;
	sqlstmt text;

BEGIN
	BEGIN
		success := false;
		species_clip_table := 'species_clip'::name;

		start_time := clock_timestamp();

		/* clean up any previous output left behind */
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table_max]::name[]
				,ARRAY[output_schema, output_table_mode]::name[]
			]::name[][]);

		/* remove any previous species clipped temporary table left behind */
		CALL bh3_drop_temp_table(species_clip_table);

		RAISE INFO 'bh3_sensitivity_layer_prep: Dropped any previous outputs left behind: %', (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		srid_hab := bh3_find_srid(habitat_schema, habitat_table, 'the_geom');
		IF srid_hab != output_srid AND srid_hab > 0 AND srid_hab > 0 THEN
			geom_exp_hab := format('ST_Transform(hab.%I,%s)', 'the_geom', output_srid);
		ELSE 
			geom_exp_hab := 'hab.the_geom';
		END IF;
		
		IF srid_hab != 3035 AND srid_hab != 0 THEN
			transform_exp := 'ST_Transform(hab.the_geom,3035)';
		ELSE
			transform_exp := 'hab.the_geom';
		END IF;

		/* create species clipped temporary table used by both queries below */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT gid'
						   ',the_geom'
						   ',sensitivity_ab_su_num'
						   ',sensitivity_ab_ss_num '
					   'FROM bh3_species_sensitivity_clipped($1,$2,$3,$4,$5,$6,$7,$8)',
					   species_clip_table)
		USING boundary_schema, sensitivity_source_table, start_year, end_year, 
			boundary_table, habitat_types_filter, habitat_types_filter_negate, output_srid;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_sensitivity_layer_prep: Inserted % rows into temporary table %: %', 
			rows_affected, species_clip_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_index(NULL, species_clip_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','']
						   ,ARRAY['sensitivity_ab_su_num','']
						   ,ARRAY['sensitivity_ab_ss_num','']
					   ]);

		RAISE INFO 'bh3_sensitivity_layer_prep: Indexed temporary table %: %', 
			species_clip_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create species sensitivity maximum table */
		EXECUTE format('CREATE TABLE %1$I.%2$I '
					   '('
						   'gid serial NOT NULL PRIMARY KEY'
						   ',the_geom geometry(MultiPolygon,4326) NOT NULL'
						   ',"row" integer'
						   ',col integer'
						   ',gid_hab bigint'
						   ',hab_type character varying'
						   ',eunis_l3 character varying'
						   ',sensitivity_ab_su_num_max smallint'
						   ',confidence_ab_su_num smallint'
						   ',sensitivity_ab_ss_num_max smallint'
						   ',confidence_ab_ss_num smallint'
						   ',sensitivity_ab_su_num smallint'
						   ',sensitivity_ab_ss_num smallint'
					  ')',
					   output_schema, output_table_max);

		/* populate species sensitivity maximum table */
		EXECUTE format('WITH cte_spec AS '
					   '('
						   'SELECT hab.gid'
							   ',max(sp.sensitivity_ab_su_num) AS sensitivity_ab_su_num'
							   ',max(sp.sensitivity_ab_ss_num) AS sensitivity_ab_ss_num'
							   ',count(sp.gid) AS num_samples '
						   'FROM %1$I.%2$I hab '
							   'JOIN %3$I sp ON ST_CoveredBy(sp.the_geom,%4$s) '
						   'GROUP BY hab.gid'
					   ') '
					   'INSERT INTO %5$I.%6$I ('
						   'gid'
						   ',"row"'
						   ',the_geom'
						   ',col'
						   ',gid_hab'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
						   ',sensitivity_ab_su_num'
						   ',sensitivity_ab_ss_num) '
					   'SELECT hab.gid'
						   ',hab."row"'
						   ',%4$s'
						   ',hab.col'
						   ',hab.gid_hab'
						   ',hab.hab_type'
						   ',hab.eunis_l3'
						   ',hab.sensitivity_ab_su_num_max'
						   ',hab.confidence_ab_su_num'
						   ',hab.sensitivity_ab_ss_num_max'
						   ',hab.confidence_ab_ss_num'
						   ',sp.sensitivity_ab_su_num'
						   ',sp.sensitivity_ab_ss_num '
					   'FROM %1$I.%2$I hab '
						   'JOIN cte_spec sp ON hab.gid = sp.gid', 
					   habitat_schema, habitat_table_grid, species_clip_table, 
					   geom_exp_hab, output_schema, output_table_max);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_sensitivity_layer_prep: Inserted % rows into output table %.%: %', 
			rows_affected, output_schema, output_table_max, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* index species sensitivity maximum table */
		CALL bh3_index(output_schema, output_table_max, 
					   ARRAY[
						   ARRAY['the_geom','s'] 
						   ,ARRAY['gid','u']
						   ,ARRAY['hab_type','']
						   ,ARRAY['eunis_l3','']
						   ,ARRAY['sensitivity_ab_su_num','']
						   ,ARRAY['sensitivity_ab_ss_num','']
					   ]);

		RAISE INFO 'bh3_sensitivity_layer_prep: Indexed output table %.%: %', 
			output_schema, output_table_max, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create species sensitivity mode table */
		EXECUTE format('CREATE TABLE %1$I.%2$I '
					   '('
						   'gid serial NOT NULL PRIMARY KEY'
						   ',the_geom geometry(MultiPolygon,4326)'
						   ',hab_type character varying'
						   ',eunis_l3 character varying'
						   ',sensitivity_ab_su_num_max smallint'
						   ',confidence_ab_su_num smallint'
						   ',sensitivity_ab_ss_num_max smallint'
						   ',confidence_ab_ss_num smallint'
						   ',sensitivity_ab_su_num smallint'
						   ',sensitivity_ab_ss_num smallint'
						   ',num_samples bigint'
					   ')',
					   output_schema, output_table_mode);

		/* populate species sensitivity mode table */
		EXECUTE format('WITH cte_spec AS '
					   '('
						   'SELECT hab.gid'
							   ',sp.sensitivity_ab_su_num'
							   ',sp.sensitivity_ab_ss_num '
						   'FROM %1$I.%2$I hab '
							   'JOIN %3$I sp ON ST_CoveredBy(sp.the_geom,%4$s)'
					   '),'
					   'cte_count_samples AS '
					   '('
						   'SELECT gid'
							   ',count(*) AS num_samples '
						   'FROM cte_spec '
						   'GROUP BY gid'
					   '),'
					   'cte_count_su AS '
					   '('
						   'SELECT gid'
							   ',sensitivity_ab_su_num'
							   ',count(sensitivity_ab_su_num) AS N '
						   'FROM cte_spec '
						   'GROUP BY gid'
							   ',sensitivity_ab_su_num'
					   '),'
					   'cte_count_ss AS '
					   '('
						   'SELECT gid'
							   ',sensitivity_ab_ss_num'
							   ',count(sensitivity_ab_ss_num) AS N '
						   'FROM cte_spec '
						   'GROUP BY gid'
							   ',sensitivity_ab_ss_num'
					   '),'
					   'cte_rank_su AS '
					   '('
						   'SELECT gid'
							   ',sensitivity_ab_su_num'
							   ',rank() OVER(PARTITION BY gid ORDER BY N DESC,sensitivity_ab_su_num DESC) AS sensitivity_rank '
						   'FROM cte_count_su'
					   '),'
					   'cte_rank_ss AS '
					   '('
						   'SELECT gid'
							   ',sensitivity_ab_ss_num'
							   ',rank() OVER(PARTITION BY gid ORDER BY N DESC,sensitivity_ab_ss_num DESC) AS sensitivity_rank '
						   'FROM cte_count_ss'
					   '),'
					   'cte_join_rankings AS '
					   '('
						   'SELECT coalesce(su.gid,ss.gid) AS gid'
							   ',su.sensitivity_ab_su_num'
							   ',ss.sensitivity_ab_ss_num '
						   'FROM cte_rank_su su '
							   'FULL JOIN cte_rank_ss ss ON su.gid = ss.gid '
						   'WHERE su.sensitivity_rank = 1 '
							   'AND ss.sensitivity_rank = 1'
					   ') '
					   'INSERT INTO %5$I.%6$I ('
						   'gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
						   ',sensitivity_ab_su_num'
						   ',sensitivity_ab_ss_num'
						   ',num_samples) '
					   'SELECT hab.gid'
						   ',%4$s'
						   ',hab.hab_type'
						   ',hab.eunis_l3'
						   ',hab.sensitivity_ab_su_num_max'
						   ',hab.confidence_ab_su_num'
						   ',hab.sensitivity_ab_ss_num_max'
						   ',hab.confidence_ab_ss_num'
						   ',j.sensitivity_ab_su_num'
						   ',j.sensitivity_ab_ss_num'
						   ',s.num_samples '
					   'FROM cte_join_rankings j '
						   'JOIN cte_count_samples s ON j.gid = s.gid '
						   'JOIN %1$I.%2$I hab ON j.gid = hab.gid '
					   'WHERE s.num_samples / ST_Area(%7$s) >= 0.00000005',
					   habitat_schema, habitat_table, species_clip_table, geom_exp_hab, 
					   output_schema, output_table_mode, transform_exp);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_sensitivity_layer_prep: Inserted % rows into output table %.%: %', 
			rows_affected, output_schema, output_table_mode, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* index species sensitivity mode table */
		CALL bh3_index(output_schema, output_table_mode, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
						   ,ARRAY['hab_type','']
						   ,ARRAY['eunis_l3','']
						   ,ARRAY['sensitivity_ab_su_num','']
						   ,ARRAY['sensitivity_ab_ss_num','']
					   ]);

		RAISE INFO 'bh3_sensitivity_layer_prep: Indexed output table %.%: %', 
			output_schema, output_table_mode, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* remove species clipped temporary table */
		CALL bh3_drop_temp_table(species_clip_table);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_sensitivity_layer_prep: Error. Message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, integer, integer, name, character varying[], boolean, name, name, name, name, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, integer, integer, name, character varying[], boolean, name, name, name, name, integer)
    IS 'Purpose:
Creates the sensitivity maximum and mode tables from the habitat table and Marine Recorder species sensitivity rows for the specified time period 
and habitats within the specified boundary polygon(s).

Approach:
Retrieves Marine Recorder species sensitivity rows for the specified time period and habitats and clips them by the boundary polygon(s).
Creates the output sensitivity maximum table and populates it by joining the habitat table to the clipped species sensitivity table, aggregating by 
the habitat table gid, inserting the maximum surface and sub-surface abrasion sensitivity and number of samples plus the habitat table''s maximum 
surface and sub-surface sensitivity and confidence scores into the output sensitivity maximum table.
Creates the output sensitivity mode table and populates it by joining the habitat table to the clipped species sensitivity table, separately counting 
the total number of samples as well as counting and ranking the frequencies of all different surface and sub-surface abrasion sensitivity scores, 
aggregating by habitat table gid, and inserting the top ranked rows plus the habitat table''s maximum surface and sub-surface sensitivity and confidence scores 
into the output sensitivity mode table provided the number of samples divided by the polygon area in square metres is at least 0.00000005.

Parameters:
boundary_schema				name							Schema of table containing single AOI boundary polygon and bounding box.
habitat_schema				name							Schema of input habitat_table.
output_schema				name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'', ''eco_groups_rock'' }).
start_year					integer							Earliest year for Marine Recorder spcies samples to be included.
end_year					integer							Latest year for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_table				name							Name of table containing single AOI boundary polygon and bounding boxs. Defaults to ''boundary''.
habitat_types_filter		character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
habitat_types_filter_negate	boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
habitat_table				name							Name of habitat sensitivity output table. Defaults to ''habitat_sensitivity''.
habitat_table_grid			name							Name of gridded habitat sensitivity output table. Defaults to ''habitat_sensitivity_grid''.
output_table_max			name							Table name of species sensitivity maximum map. Defaults to ''species_sensitivity_max''.
output_table_mode			name							Table name of species sensitivity mode map. Defaults to ''species_sensitivity_mode''.
output_srid					integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table
bh3_find_srid
bh3_species_sensitivity_clipped.';
