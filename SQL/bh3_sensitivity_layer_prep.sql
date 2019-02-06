-- FUNCTION: public.bh3_sensitivity_layer_prep(name, name, integer, sensitivity_source, timestamp without time zone, timestamp without time zone, character varying[], name, name, name, name, name, name, boolean, boolean, integer)

-- DROP FUNCTION public.bh3_sensitivity_layer_prep(name, name, integer, sensitivity_source, timestamp without time zone, timestamp without time zone, character varying[], name, name, name, name, name, name, boolean, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_sensitivity_layer_prep(
	habitat_schema name,
	output_schema name,
	boundary_filter integer,
	sensitivity_source_table sensitivity_source,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(),
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	habitat_table name DEFAULT 'habitat_sensitivity'::name,
	habitat_table_grid name DEFAULT 'habitat_sensitivity_grid'::name,
	output_table_max name DEFAULT 'species_sensitivity_max'::name,
	output_table_mode name DEFAULT 'species_sensitivity_mode'::name,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
	boundary_filter_negate boolean DEFAULT false,
	habitat_types_filter_negate boolean DEFAULT false,
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
		FOR tn IN 
			EXECUTE format('SELECT c.relname '
						   'FROM pg_class c '
							   'JOIN pg_namespace n ON c.relnamespace = n.oid '
						   'WHERE n.nspname = $1 AND c.relname IN($2,$3)')
			USING output_schema, output_table_max, output_table_mode 
		LOOP
			EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING output_schema, tn;
		END LOOP;

		/* remove any previous species clipped temporary table left behind */
		CALL bh3_drop_temp_table(species_clip_table);

		RAISE INFO 'bh3_habitat_boundary_clip: Dropped any previous outputs left behind: %', (clock_timestamp() - start_time);

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
					   'FROM bh3_species_sensitivity_clipped($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
					   species_clip_table)
		USING boundary_schema, boundary_table, boundary_filter, sensitivity_source_table, 
			date_start, habitat_types_filter, date_end, boundary_filter_negate, habitat_types_filter_negate,
			output_srid;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into temporary table %: %', 
			rows_affected, species_clip_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', species_clip_table);
		EXECUTE format('CREATE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', species_clip_table);
		EXECUTE format('CREATE INDEX idx_%1$s_sensitivity_ab_su_num ON %1$I USING BTREE(sensitivity_ab_su_num)', species_clip_table);
		EXECUTE format('CREATE INDEX idx_%1$s_sensitivity_ab_ss_num ON %1$I USING BTREE(sensitivity_ab_ss_num)', species_clip_table);

		RAISE INFO 'bh3_habitat_boundary_clip: Indexed temporary table %: %', 
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
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into output table %.%: %', 
			rows_affected, output_schema, output_table_max, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create indexes on species sensitivity maximum table */
		EXECUTE format('CREATE INDEX sidx_%2$I_the_geom ON %1$I.%2$I USING GIST(the_geom)',
					   output_schema, output_table_max);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$I_gid ON %1$I.%2$I USING BTREE(gid)',
					   output_schema, output_table_max);
		EXECUTE format('CREATE INDEX idx_%2$I_hab_type ON %1$I.%2$I USING BTREE(hab_type)',
					   output_schema, output_table_max);
		EXECUTE format('CREATE INDEX idx_%2$I_eunis_l3 ON %1$I.%2$I USING BTREE(eunis_l3)',
					   output_schema, output_table_max);
		EXECUTE format('CREATE INDEX idx_%2$I_sensitivity_ab_su_num ON %1$I.%2$I USING BTREE(sensitivity_ab_su_num)',
					   output_schema, output_table_max);
		EXECUTE format('CREATE INDEX idx_%2$I_sensitivity_ab_ss_num ON %1$I.%2$I USING BTREE(sensitivity_ab_ss_num)',
					   output_schema, output_table_max);

		RAISE INFO 'bh3_habitat_boundary_clip: Indexed output table %.%: %', 
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
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into output table %.%: %', 
			rows_affected, output_schema, output_table_mode, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create indexes on species sensitivity mode table */
		EXECUTE format('CREATE INDEX sidx_%2$I_the_geom ON %1$I.%2$I USING GIST(the_geom)',
					   output_schema, output_table_mode);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$I_gid ON %1$I.%2$I USING BTREE(gid)',
					   output_schema, output_table_mode);
		EXECUTE format('CREATE INDEX idx_%2$I_hab_type ON %1$I.%2$I USING BTREE(hab_type)',
					   output_schema, output_table_mode);
		EXECUTE format('CREATE INDEX idx_%2$I_eunis_l3 ON %1$I.%2$I USING BTREE(eunis_l3)',
					   output_schema, output_table_mode);
		EXECUTE format('CREATE INDEX idx_%2$I_sensitivity_ab_su_num ON %1$I.%2$I USING BTREE(sensitivity_ab_su_num)',
					   output_schema, output_table_mode);
		EXECUTE format('CREATE INDEX idx_%2$I_sensitivity_ab_ss_num ON %1$I.%2$I USING BTREE(sensitivity_ab_ss_num)',
					   output_schema, output_table_mode);

		RAISE INFO 'bh3_habitat_boundary_clip: Indexed output table %.%: %', 
			output_schema, output_table_mode, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* remove species clipped temporary table */
		CALL bh3_drop_temp_table(species_clip_table);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_sensitivity_layer_prep: Error. Message: %. Detail: %. Hint: %', exc_ext, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_sensitivity_layer_prep(name, name, integer, sensitivity_source, timestamp without time zone, timestamp without time zone, character varying[], name, name, name, name, name, name, boolean, boolean, integer)
    OWNER TO postgres;
