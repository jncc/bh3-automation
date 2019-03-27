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



CREATE OR REPLACE FUNCTION public.bh3_sensitivity_layer_prep(
	habitat_schema name,
	output_schema name,
	boundary_filter integer,
	sensitivity_source_table sensitivity_source,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(
	),
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
					   'FROM bh3_species_sensitivity_clipped($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
					   species_clip_table)
		USING boundary_schema, boundary_table, boundary_filter, sensitivity_source_table, 
			date_start, habitat_types_filter, date_end, boundary_filter_negate, habitat_types_filter_negate,
			output_srid;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_sensitivity_layer_prep: Inserted % rows into temporary table %: %', 
			rows_affected, species_clip_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', species_clip_table);
		EXECUTE format('CREATE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', species_clip_table);
		EXECUTE format('CREATE INDEX idx_%1$s_sensitivity_ab_su_num ON %1$I USING BTREE(sensitivity_ab_su_num)', species_clip_table);
		EXECUTE format('CREATE INDEX idx_%1$s_sensitivity_ab_ss_num ON %1$I USING BTREE(sensitivity_ab_ss_num)', species_clip_table);

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
		RAISE INFO 'bh3_sensitivity_layer_prep: Error. Message: %. Detail: %. Hint: %', exc_ext, exc_detail, exc_hint;
	END;
END;
$BODY$;



CREATE OR REPLACE FUNCTION public.bh3_sensitivity_map(
	habitat_sensitivity_schema name,
	species_sensitivity_schema name,
	output_schema name,
	habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name,
	species_sensitivity_max_table name DEFAULT 'species_sensitivity_max'::name,
	species_sensitivity_mode_table name DEFAULT 'species_sensitivity_mode'::name,
	output_table name DEFAULT 'sensitivity_map'::name,
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
	tn name;
	species_sensitivity_mode_final_table name;
	species_sensitivity_all_areas_table name;
	habitat_sensitivity_final_table name;
	sqlstmt character varying;

BEGIN
	BEGIN
		species_sensitivity_mode_final_table := 'species_sensitivity_mode_final';
		species_sensitivity_all_areas_table := 'species_sensitivity_all_areas';
		habitat_sensitivity_final_table := 'habitat_sensitivity_final';

		/* drop any previous temp tables left behind */
		RAISE INFO 'bh3_sensitivity_map: Dropping temporary table %', species_sensitivity_mode_final_table;
		CALL bh3_drop_temp_table(species_sensitivity_mode_final_table);
		RAISE INFO 'bh3_sensitivity_map: Dropping temporary table %', species_sensitivity_all_areas_table;
		CALL bh3_drop_temp_table(species_sensitivity_all_areas_table);
		RAISE INFO 'bh3_sensitivity_map: Dropping temporary table %', habitat_sensitivity_final_table;
		CALL bh3_drop_temp_table(habitat_sensitivity_final_table);

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

		RAISE INFO 'bh3_sensitivity_map: Creating temporary table %', species_sensitivity_mode_final_table;

		/* species_sensitivity_mode_final = species_sensitivity_mode - species_sensitivity_maximum */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_join AS '
					   '('
						   'SELECT mod.gid'
							   ',mod.hab_type'
							   ',mod.eunis_l3'
							   ',mod.sensitivity_ab_su_num_max'
							   ',mod.confidence_ab_su_num'
							   ',mod.sensitivity_ab_ss_num_max'
							   ',mod.confidence_ab_ss_num'
							   ',mod.sensitivity_ab_su_num'
							   ',mod.sensitivity_ab_ss_num'
							   ',mod.the_geom'
							   ',max.the_geom AS the_geom_erase '
						   'FROM %2$I.%3$I mod '
							   'LEFT JOIN %2$I.%4$I max ON ST_Intersects(mod.the_geom,max.the_geom) ' 
								   'AND NOT ST_Touches(mod.the_geom,max.the_geom)'
					   '),'
					   'cte_diff AS '
					   '('
						   'SELECT gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',sensitivity_ab_su_num'
							   ',sensitivity_ab_ss_num'
							   ',ST_Difference(the_geom,ST_Union(the_geom_erase)) AS the_geom '
						   'FROM cte_join '
						   'WHERE the_geom_erase IS NOT NULL '
						   'GROUP BY gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',sensitivity_ab_su_num'
							   ',sensitivity_ab_ss_num'
							   ',the_geom '
						   'UNION '
						   'SELECT gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',sensitivity_ab_su_num'
							   ',sensitivity_ab_ss_num'
							   ',the_geom '
						   'FROM cte_join '
						   'WHERE the_geom_erase IS NULL'
					   '),'
					   'cte_repair AS '
					   '('
						   'WITH cte_dump AS '
						   '('
							   'SELECT gid'
								   ',(ST_Dump(the_geom)).geom AS the_geom '
							   'FROM cte_diff '
							   'WHERE NOT ST_IsEmpty(the_geom)'
						   ') '
						   'SELECT gid'
							   ',ST_Multi(ST_Union('
								   'CASE '
									   'WHEN ST_IsValid(the_geom) THEN the_geom '
									   'ELSE ST_Buffer(the_geom,0) '
							   'END)) AS the_geom '
						   'FROM cte_dump '
						   'GROUP BY gid'
					   ') '
					   'SELECT d.gid'
						   ',d.hab_type'
						   ',d.eunis_l3'
						   ',d.sensitivity_ab_su_num_max'
						   ',d.confidence_ab_su_num'
						   ',d.sensitivity_ab_ss_num_max'
						   ',d.confidence_ab_ss_num'
						   ',d.sensitivity_ab_su_num'
						   ',d.sensitivity_ab_ss_num'
						   ',r.the_geom '
					   'FROM cte_diff d '
						   'JOIN cte_repair r ON d.gid = r.gid', 
					   species_sensitivity_mode_final_table, species_sensitivity_schema, 
					   species_sensitivity_mode_table, species_sensitivity_max_table);
		
		EXECUTE format('ALTER TABLE %1$I ADD CONSTRAINT %1$s_pkey PRIMARY KEY(gid)', 
					   species_sensitivity_mode_final_table);
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   species_sensitivity_mode_final_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', 
					   species_sensitivity_mode_final_table);

		RAISE INFO 'bh3_sensitivity_map: Creating temporary table %', species_sensitivity_all_areas_table;

		/* species_sensitivity_all_areas = species_sensitivity_mode_final + species_sensitivity_max */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_union AS '
					   '('
						   'SELECT gid AS gid_max'
							   ',NULL AS gid_mode'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',sensitivity_ab_su_num'
							   ',sensitivity_ab_ss_num '
						   'FROM %2$I.%3$I max '
						   'UNION '
						   'SELECT NULL AS gid_max'
							   ',gid AS gid_mode'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',sensitivity_ab_su_num'
							   ',sensitivity_ab_ss_num '
						   'FROM %2$I.%4$I mod'
					   ') '
					   'SELECT ROW_NUMBER() OVER() AS gid'
						   ',gid_max'
						   ',gid_mode'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
						   ',sensitivity_ab_su_num'
						   ',sensitivity_ab_ss_num '
					   'FROM cte_union', 
					   species_sensitivity_all_areas_table, species_sensitivity_schema, 
					   species_sensitivity_max_table, species_sensitivity_mode_final_table);

		EXECUTE format('ALTER TABLE %1$I ADD CONSTRAINT %1$s_pkey PRIMARY KEY(gid)', 
					   species_sensitivity_all_areas_table);
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   species_sensitivity_all_areas_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', 
					   species_sensitivity_all_areas_table);

		RAISE INFO 'bh3_sensitivity_map: Creating temporary table %', habitat_sensitivity_final_table;

		/* habitat_sensitivity_final = habitat_sensitivity - species_sensitivity_all_areas */
		EXECUTE format('CREATE TABLE %1$I AS '
					   'WITH cte_join AS '
					   '('
						   'SELECT hab.gid'
							   ',hab.hab_type'
							   ',hab.eunis_l3'
							   ',hab.sensitivity_ab_su_num_max'
							   ',hab.confidence_ab_su_num'
							   ',hab.sensitivity_ab_ss_num_max'
							   ',hab.confidence_ab_ss_num'
							   ',hab.the_geom'
							   ',saa.the_geom AS the_geom_erase '
						   'FROM %2$I.%3$I hab '
							   'LEFT JOIN %4$I saa '
								   'ON ST_Intersects(hab.the_geom,saa.the_geom) AND NOT ST_Touches(hab.the_geom,saa.the_geom)'
					   '),'
					   'cte_diff AS '
					   '('
						   'SELECT gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',ST_Difference(the_geom,ST_Union(the_geom_erase)) AS the_geom '
						   'FROM cte_join '
						   'WHERE the_geom_erase IS NOT NULL '
						   'GROUP BY gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',the_geom '
						   'UNION '
						   'SELECT gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',the_geom '
						   'FROM cte_join '
						   'WHERE the_geom_erase IS NULL'
					   '),'
					   'cte_repair AS '
					   '('
						   'WITH cte_dump AS '
						   '('
							   'SELECT gid'
								   ',(ST_Dump(the_geom)).geom AS the_geom '
							   'FROM cte_diff '
							   'WHERE NOT ST_IsEmpty(the_geom)'
						   ') '
						   'SELECT gid'
							   ',ST_Multi(ST_Union('
							   'CASE '
								   'WHEN ST_IsValid(the_geom) THEN the_geom '
								   'ELSE ST_Buffer(the_geom,0) '
							   'END)) AS the_geom '
						   'FROM cte_dump '
						   'GROUP BY gid'
					   ') '
					   'SELECT d.gid'
						   ',d.hab_type'
						   ',d.eunis_l3'
						   ',d.sensitivity_ab_su_num_max'
						   ',d.confidence_ab_su_num'
						   ',d.sensitivity_ab_ss_num_max'
						   ',d.confidence_ab_ss_num'
						   ',r.the_geom '
					   'FROM cte_diff d '
						   'JOIN cte_repair r ON d.gid = r.gid', 
					   habitat_sensitivity_final_table, habitat_sensitivity_schema, 
					   habitat_sensitivity_table, species_sensitivity_all_areas_table);

		EXECUTE format('ALTER TABLE %1$I ADD CONSTRAINT %1$s_pkey PRIMARY KEY(gid)', 
					   habitat_sensitivity_final_table);
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   habitat_sensitivity_final_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', 
					   habitat_sensitivity_final_table);

		RAISE INFO 'bh3_sensitivity_map: Creating table %.%', output_schema, output_table;

		EXECUTE format('CREATE TABLE %1$I.%2$I '
					   '('
							'gid serial NOT NULL PRIMARY KEY'
							',the_geom geometry(MultiPolygon,4326)'
							',hab_type character varying'
							',eunis_l3 character varying'
							',sensitivity_ab_su_num smallint'
							',sensitivity_ab_ss_num smallint'
					   ')',
					   output_schema, output_table);

		RAISE INFO 'bh3_sensitivity_map: Populating table %.%', output_schema, output_table;

		/* sensitivity_map = habitat_sensitivity_final + species_sensitivity_all_areas */
		EXECUTE format('WITH cte_union AS '
					   '('
						   'SELECT gid'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',NULL AS sensitivity_ab_su_num'
							   ',NULL AS sensitivity_ab_ss_num '
						   'FROM %1$I '
						   'UNION '
						   'SELECT gid'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',sensitivity_ab_su_num'
							   ',sensitivity_ab_ss_num '
						   'FROM %2$I'
					   ')'
					   'INSERT INTO %3$I.%4$I '
					   '('
						   'gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num'
						   ',sensitivity_ab_ss_num'
					   ') '
					   'SELECT ROW_NUMBER() OVER() AS gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',coalesce(sensitivity_ab_su_num,sensitivity_ab_su_num_max) AS sensitivity_ab_su_num'
						   ',coalesce(sensitivity_ab_ss_num,sensitivity_ab_ss_num_max) AS sensitivity_ab_ss_num '
					   'FROM cte_union',
					   habitat_sensitivity_final_table, species_sensitivity_all_areas_table, 
					   output_schema, output_table);

		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', 
					   output_schema, output_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', 
					   output_schema, output_table);

		/* drop temp tables */
		RAISE INFO 'bh3_sensitivity_map: Dropping temporary table %', species_sensitivity_mode_final_table;
		CALL bh3_drop_temp_table(species_sensitivity_mode_final_table);
		RAISE INFO 'bh3_sensitivity_map: Dropping temporary table %', species_sensitivity_all_areas_table;
		CALL bh3_drop_temp_table(species_sensitivity_all_areas_table);
		RAISE INFO 'bh3_sensitivity_map: Dropping temporary table %', habitat_sensitivity_final_table;
		CALL bh3_drop_temp_table(habitat_sensitivity_final_table);
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_sensitivity_map: Error. Message: %. Detail: %. Hint: %.', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;


-------------------------------------------------------------------------------

COMMENT ON PROCEDURE public.bh3_drop_temp_table IS 
'Purpose:
Drops a temporary table if it exists.

Approach:
Looks up the temporary table''s schema in database metadata and drop it using a qualified name.

Parameters:
table_name		name	Name of temporary table to be dropped.

Calls:
No nested calls.';



COMMENT ON PROCEDURE public.bh3_procedure IS 
'Purpose:
Main entry point that starts a BH3 run. 
This is called by the QGIS user interface and may be executed directly in pgAdmin or on the PostgreSQL command line.

Approach:
Creates the output schema and an error_log table in it if they do not already exist.
Then calls the bh3_get_pressure_csquares_size, bh3_habitat_boundary_clip, bh3_habitat_grid, bh3_sensitivity_layer_prep, 
bh3_sensitivity_map and bh3_disturbance_map functions, inserting any error rows returned into the error_log table.

Parameters:
boundary_filter						integer							gid of AOI polygon in boundary_table to be included (or excluded if boundary_filter_negate is true).
habitat_types_filter				character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
date_start							timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
species_sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
pressure_schema						name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
output_schema						name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
boundary_schema						name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table						name							Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
habitat_schema						name							Habitat table schema. Defaults to ''static''.
habitat_table						name							Habitat table name. Defaults to ''uk_habitat_map_wgs84''.
habitat_sensitivity_lookup_schema	name							Schema of habitat sensitivity lookup table. Defaults to ''lut''.
habitat_sensitivity_lookup_table	name							Name of habitat sensitivity lookup table. Defaults to ''sensitivity_broadscale_habitats''.
habitat_sensitivity_table			name							Name of habitat sensitivity output table. Defaults to ''habitat_sensitivity''.
habitat_sensitivity_grid_table		name							Name of gridded habitat sensitivity output table. Defaults to ''habitat_sensitivity_grid''.
species_sensitivity_max_table		name							Table name of species sensitivity maximum map. Defaults to ''species_sensitivity_max''.
species_sensitivity_mode_table		name							Table name of species sensitivity mode map. Defaults to ''species_sensitivity_mode''.
sensitivity_map_table				name							Table name of output sensitivity map. Defaults to ''sensitivity_map''.
disturbance_map_table				name							Table name of output disturbance map. Defaults to ''disturbance_map''.
sar_surface_column					name							SAR surface column name in pressure source tables. Defaults to ''sar_surface''.
sar_subsurface_column				name							SAR sub-surface column name in pressure source tables. Defaults to ''sar_subsurface''.
date_end							timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_filter_negate				boolean							If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
habitat_types_filter_negate			boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
remove_overlaps						boolean							If true overlaps will be removed from habitat_sensitivity_table (significantly increases processing time). Defaults to false.
output_srid							integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Calls:
bh3_get_pressure_csquares_size
bh3_habitat_boundary_clip
bh3_habitat_grid
bh3_sensitivity_layer_prep
bh3_sensitivity_map
bh3_disturbance_map';



COMMENT ON PROCEDURE public.bh3_repair_geometries IS 
'Purpose:
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



COMMENT ON FUNCTION public.bh3_create_csquares(integer, boolean, name, name, boolean, numeric, integer) IS 
'Purpose:
Creates an in-memory table of c-squares of the specified cell size within the specified polygon boundary.
The output geometries may be clipped by the boundary polygon(s).

Approach:
Calls the bh3_create_finshnet function to create a grid within the bounding box of the unioned boundary geometries
and returns the grid cells intersecting the boundary geometries, optionally clipping them by the boundary.

Parameters:
boundary_filter			integer		gid of boundary polygon in boundary_table.
boundary_clip			boolean		If true grid will be clipped by boundary polygon. Defaults to false.
boundary_schema			name		Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table			name		Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
boundary_filter_negate	boolean		If true condition built with boundary_filter is to be negated, i.e. the boundary is all but the polygon identified by boundary_filter. Defaults to false.
cell_size_degrees		numeric		Cell size in degrees. Defaults to 0.05.
output_srid				integer		SRID of output table. Defaults to 4326.

Returns:
An in-memory table of c-squares of the specified cell size within the specified polygon boundary.

Calls:
bh3_find_srid
bh3_create_fishnet';



COMMENT ON FUNCTION public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer) IS 
'Purpose:
Creates an in-memory table of c-squares of the specified cell of the specified dimensions.

Approach:
Cross joins sequences of row and column numbers generated by calling the generate_sequence function and 
creates boxes calling the ST_MakeBox2D function.

Parameters:
num_rows	integer				Desired number of grid rows.
num_cols	integer				Desired number of grid columns.
size_x		double precision	Desired width of grid cell.
size_y		double precision	Desired height of grid cell.
origin_x	double precision	X coordinate of grid origin. Defaults to 0.
origin_y	double precision	Y coordinate of grid origin. Defaults to 0.
srid		integer				SRID of output grid. Defaults to 4326.

Returns:
An in-memory table of c-squares of the specified cell of the specified dimensions.

Calls:
No nested calls';



COMMENT ON FUNCTION public.bh3_disturbance_map(integer, timestamp without time zone, name, name, name, name, name, name, name, name, name, boolean, timestamp without time zone, integer) IS 
'Purpose:
Creates the disturbance map from sensitivity and pressure maps.

Approach:
Creates a table of pressure c-squares calling the bh3_get_pressure_csquares function.
Then, using a cursor, the disturbance map table is populated computing surface and subsurface disturbance scores 
surface and subsurface abrasion sensitivity scores from the sensitivity map with categorised combined surface and 
sub-surface abrasion scores from the pressure c-squares using case expressions and a geometry as the intersection 
of sensitivity and pressure c-square geometries.

Parameters:
boundary_filter			integer							gid of AOI polygon in boundary_table to be included (or excluded if boundary_filter_negate is true).
date_start				timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
pressure_schema			name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
sensitivity_map_schema	name							Schema in which sensitivity map table is located.
output_schema			name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
boundary_schema			name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table			name							Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
sensitivity_map_table	name							Table name of sensitivity map. Defaults to ''sensitivity_map''.
output_table			name							Table name of output disturbance map. Defaults to ''disturbance_map''.
sar_surface_column		name							SAR surface column name in pressure source tables. Defaults to ''sar_surface''.
sar_subsurface_column	name							SAR sub-surface column name in pressure source tables. Defaults to ''sar_subsurface''.
boundary_filter_negate	boolean							If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
date_end				timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
output_srid				integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
Table of error records from cursor loop.

Calls:
bh3_drop_temp_table
bh3_get_pressure_csquares
bh3_find_srid';



COMMENT ON FUNCTION public.bh3_find_srid(name, name, name) IS 
'Purpose:
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



COMMENT ON FUNCTION public.bh3_get_pressure_csquares(integer, name, timestamp without time zone, timestamp without time zone, name, name, name, name, boolean, integer) IS 
'Purpose:
Creates an in-memory table of categorised pressure c-squares from the tables in the specified pressure_schema. 
All tables in pressure_schema that have the required columns will be included.

Approach:
Computes summary values of disturbance scores for each table in pressure_schema that has the columns 
''c_square'', ''year'', sar_surface_column, sar_subsurface_column and ''the_geom'', aggregating by c_square,
categorises the scores into sar_surface and sar_subsurface scores between one and four and returns the union
of the resulting row sets.

Paramerters:
boundary_filter			integer							gid of AOI polygon in boundary_table to be included (or excluded if boundary_filter_negate is true).
pressure_schema			name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
date_start				timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
date_end				timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time. Defaults to current date and time.
sar_surface_column		name							SAR surface column name in pressure source tables. Defaults to ''sar_surface''.
sar_subsurface_column	name							SAR sub-surface column name in pressure source tables. Defaults to ''sar_subsurface''.
boundary_schema			name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table			name							Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
boundary_filter_negate	boolean							If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
output_srid				integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
An in-memory table of categorised pressure c-squares from the tables in the specified pressure_schema.

Calls:
bh3_find_srid';



COMMENT ON FUNCTION public.bh3_get_pressure_csquares_size IS 
'Purpose:
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



COMMENT ON FUNCTION public.bh3_habitat_boundary_clip IS 
'Purpose:
Creates habitat_sensitivity table for the selected AOI in the selected output schema.

Approach:
The AOI polygon/s is/are split into smaller parts with no more than 256 vertices each. These smaller parts are spatially joined to the habitat table 
and the intersection of overlapping polygons is computed. The resulting polygons are then aggregated by the gid of the original habitat records, 
computing the union of the pieces of the original polygons.
Despite the extra steps and multiple geometry repairs between them, this is substantially faster than computing intersections directly with 
large boundary polygons.

Parameters:
boundary_filter						integer					gid of the boundary polygon that delimits the AOI.
habitat_types_filter				character varying[]		Array of EUNIS L3 codes to be included or excluded.
output_schema						name					Schema of the output habitat sensitivity table.
output_table						name					Name of the output habitat sensitivity table. Defaults to ''habitat_sensitivity''.
habitat_schema						name					Schema of the habitat table. Defaults to ''static''.
habitat_table						name					Name of the habitat table. Defaults to ''uk_habitat_map_wgs84''.
sensitivity_schema					name					Schema of the habitat sensitvity lookup table. Defaults to ''lut''.
sensitivity_table					name					Name of the habitat sensitvity lookup table. Defaults to ''sensitivity_broadscale_habitats''.
boundary_schema						name					Schema of the boundary table defining the AOI. Defaults to ''static''.
boundary_table						name					Name of the boundary table defining the AOI. Defaults to ''official_country_waters_wgs84''.
boundary_filter_negate				boolean					If false, the polygon identified by boundary_filter defines the AOI. Otherwise the AOI is defined by all but that polygon. Defaults to false.
habitat_types_filter_negate			boolean					If false, the EUNIS L3 codes in habitat_types_filter are included, if true they are excluded. Defaults to false.
exclude_empty_mismatched_eunis_l3	boolean					Controls whether habitats whose EUNIS L3 code is not matched in sensitivity_table are excluded (true) or included (false). Defaults to true.
remove_overlaps						boolean					Controls whether bh3_habitat_remove_overlaps is called to remove overlaps from output_table. Defaults to false.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table
bh3_find_srid
bh3_repair_geometries
bh3_habitat_remove_overlaps';



COMMENT ON FUNCTION public.bh3_habitat_grid(integer, name, name, name, name, name, name, boolean, numeric) IS 
'Purpose:
Creates a gridded version of the habitat_sensitivity_table.

Approach:
Creates a c-square grid table within the specified boundary and intersects it with polygons from the previously created, 
ungridded habitat_sensitivity_table, calling the fast ST_ClipByBox2D in a loop over a cursor. 

Parameters:
boundary_filter				integer		gid of the boundary polygon that delimits the AOI.
habitat_sensitivity_schema	name		Schema of the habitat sensitivity table.
output_schema				name		Schema of the output gridded habitat sensitivity table.
habitat_sensitivity_table	name		Name of habitat sensitivity table. Defaults to ''habitat_sensitivity''.
output_table				name		Name of gridded habitat sensitivity output table. Defaults to ''habitat_sensitivity_grid''.
boundary_schema				name		Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table				name		Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
boundary_filter_negate		boolean		If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
cell_size_degrees			numeric		Cell size in degrees. Defaults to 0.05.

Returns:
Table of error records from cursor loop.

Calls:
bh3_drop_temp_table
bh3_find_srid
bh3_create_csquares';



COMMENT ON FUNCTION public.bh3_habitat_remove_overlaps IS
'Purpose:
Removes overlapping polygons from the input habitat sensitivity table.

Approach:
First, the input habitat_sensitivity table is spatially self-joined intersecting overlapping geometries into a temporary table 
combining attributes from both left and right sides of the join. The resulting joined row set is then split into its constituent rows 
each of which is one overlapping geometry with its original attributes. Any duplicate overlaps in this split row set are ranked by 
sensitivity and confidence keeping only top ranked rows. The unique overlapping polygons in this last row set are then erased from the 
input habitat sensitivity table. Finally, the resulting set of input habitat sensitivity table rows with overlaps removed and the set of overlapping 
polygons with the highest sensitivity/confidence are merged using a union query and the resulting rows are inserted into the previously emptied 
input habitat_sensitivity table.

Parameters:
habitat_sensitivity_schema		name		Schema of the input habitat sensitivity table.
habitat_sensitivity_table		name		Name of the input habitat sensitivity table.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table
bh3_repair_geometries';



COMMENT ON FUNCTION public.bh3_sensitivity IS
'Purpose:
Retrieves sensitivity rows from the specified table.

Approach:
Performs a select query against the specified source table standardising the return table''s schema.

Parameters:
source_table	sensitivity_source		Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).

Returns:
An in-memory table of sensitivity rows with standardised schema.

Calls:
No nested calls.';



COMMENT ON FUNCTION public.bh3_sensitivity_layer_prep IS
'Purpose:
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
habitat_schema				name							Schema of input habitat_table.
output_schema				name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
boundary_filter				integer							gid of AOI polygon in boundary_table to be included (or excluded if boundary_filter_negate is true).
sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
date_start					timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
date_end					timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
habitat_types_filter		character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
habitat_table				name							Name of habitat sensitivity output table. Defaults to ''habitat_sensitivity''.
habitat_table_grid			name							Name of gridded habitat sensitivity output table. Defaults to ''habitat_sensitivity_grid''.
output_table_max			name							Table name of species sensitivity maximum map. Defaults to ''species_sensitivity_max''.
output_table_mode			name							Table name of species sensitivity mode map. Defaults to ''species_sensitivity_mode''.
boundary_schema				name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table				name							Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
boundary_filter_negate		boolean							If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
habitat_types_filter_negate	boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
output_srid					integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table
bh3_find_srid
bh3_species_sensitivity_clipped.';



COMMENT ON FUNCTION public.bh3_species_sensitivity_clipped IS
'Purpose:
Creates a table of species sensitivity rows within the specified boundary polygon(s).

Approach:
Selects rows from a hard coded join of Marine Recorder tables (marinerec.survey srv, marinerec.survey_event, marinerec.sample,
marinerec.sample_species, marinerec.sample_biotope_all) and lut.eunis_correlation_table with the specified sensitivity table within
the specified boundary polygon(s).

Parameters:
boundary_schema					name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table					name							Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
boundary_filter					integer							gid of AOI polygon in boundary_table to be included (or excluded if boundary_filter_negate is true).
sensitivity_source_table		sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
date_start						timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
habitat_types_filter			character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
date_end						timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_filter_negate			boolean							If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
habitat_types_filter_negate		boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
output_srid						integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
An in-memory table of species sensitivity rows.

Calls:
bh3_find_srid
bh3_sensitivity';



COMMENT ON FUNCTION public.bh3_sensitivity_map IS
'Purpose:
Creates the sensitivity map from the habitat sensitivity and species sensitivity tables.

Approach:
Creates the species_sensitivity_mode_final_table as the union of a spatial left join between the species_sensitivity_mode and 
species_sensitivity_max tables, erasing the species_sensitivity_max geometry from the species_sensitivity_mode geometry, 
and the unaltered spatial left join between the species_sensitivity_mode and species_sensitivity_max tables.
Creates the species sensitivity all areas table as the union of the species_sensitivity_mode_final and species_sensitivity_max tables.
Creates the habitat_sensitivity_final table as the union of a spatial left join between habitat_sensitivity_table and 
species_sensitivity_all_areas_table, erasing the species_sensitivity_all_areas_table geometry from the habitat_sensitivity_table 
geometry, and the unaltered spatial left join between habitat_sensitivity_table and species_sensitivity_all_areas_table.
Creates the output sensitivity map as the union of the habitat_sensitivity_final and species_sensitivity_all_areas tables.

Parameters:
habitat_sensitivity_schema			name		Schema of the habitat sensitivity table.
species_sensitivity_schema			name		Schema of the species sensitivity table.
output_schema						name		Schema in which output table will be created (will be created if it does not already exist; tables in it will be overwritten).
habitat_sensitivity_table			name		Name of habitat sensitivity table. Defaults to ''habitat_sensitivity''.
species_sensitivity_max_table		name		Table name of species sensitivity maximum map. Defaults to ''species_sensitivity_max''.
species_sensitivity_mode_table		name		Table name of species sensitivity mode map. Defaults to ''species_sensitivity_mode''.
output_table						name		Table name of output sensitivity map. Defaults to ''sensitivity_map''.
				SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table';
