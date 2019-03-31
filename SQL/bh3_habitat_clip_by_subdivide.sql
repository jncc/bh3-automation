-- FUNCTION: public.bh3_habitat_clip_by_subdivide(integer, character varying[], name, name, name, name, name, name, name, name, name, boolean, boolean, boolean, numeric)

-- DROP FUNCTION public.bh3_habitat_clip_by_subdivide(integer, character varying[], name, name, name, name, name, name, name, name, name, boolean, boolean, boolean, numeric);

CREATE OR REPLACE FUNCTION public.bh3_habitat_clip_by_subdivide(
	boundary_filter integer,
	habitat_types_filter character varying[],
	output_schema name,
	output_table name DEFAULT 'habitat_sensitivity'::name,
	output_table_grid name DEFAULT 'habitat_sensitivity_grid'::name,
	habitat_schema name DEFAULT 'static'::name,
	habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name,
	sensitivity_schema name DEFAULT 'lut'::name,
	sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
	boundary_filter_negate boolean DEFAULT false,
	habitat_types_filter_negate boolean DEFAULT false,
	exclude_empty_mismatched_eunis_l3 boolean DEFAULT true,
	cell_size_degrees numeric DEFAULT 0.05,
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
	start_time timestamp;
	rows_affected bigint;
	temp_table_boundary_subdiv name;
	temp_table_habitat_bound_subdiv name;
	temp_table_habitat_wgs84 name;
	temp_table_aoi_grid name;
	negation character varying;
	geom_exp character varying;
	habitat_type_where_clause character varying;
	srid integer;
	left_join character varying;

BEGIN
	BEGIN
		success := false;

		temp_table_boundary_subdiv := 'boundary_subdiv'::name;
		temp_table_habitat_bound_subdiv := 'habitat_bound_subdiv'::name;
		temp_table_habitat_wgs84 := 'habitat_wgs84'::name;
		temp_table_aoi_grid := 'aoi_grid'::name;
		
		start_time := clock_timestamp();

		/* clean up any previous output left behind */
		FOR tn IN 
			EXECUTE format('SELECT c.relname '
						   'FROM pg_class c '
							   'JOIN pg_namespace n ON c.relnamespace = n.oid '
						   'WHERE n.nspname = $1 AND c.relname IN($2,$3)')
			USING output_schema, output_table, output_table_grid 
		LOOP
			EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING output_schema, tn;
		END LOOP;

		/* clean up any previous temp tables left behind */
		CALL bh3_drop_temp_table(temp_table_boundary_subdiv);
		CALL bh3_drop_temp_table(temp_table_habitat_bound_subdiv);
		CALL bh3_drop_temp_table(temp_table_habitat_wgs84);
		CALL bh3_drop_temp_table(temp_table_aoi_grid);

		RAISE INFO 'Removed any outputs of previous runs from output schema %: %', 
			output_schema, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* build geometry expression for boundary query that projects geometry to 4326 if necessary */
		srid := bh3_find_srid(boundary_schema, boundary_table, 'the_geom'::name);
		IF srid != 4326 AND srid > 0 THEN
			geom_exp := format('ST_Transform(%1$I,%s)', 'the_geom', 4326);
		ELSE
			geom_exp := format('%1$I', 'the_geom');
		END IF;

		IF boundary_filter_negate THEN
			negation = 'NOT';
		ELSE
			negation = '';
		END IF;

		/* subdivide unioned AOI polygons into temp table */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_union AS '
					   '('
						   'SELECT ST_Union(%2$s) AS the_geom '
						   'FROM %3$I.%4$I '
						   'WHERE %5$s gid = $1'
					   '),'
					   'cte_repair AS '
					   '('
						   'WITH cte_dump AS '
						   '('
							   'SELECT (ST_Dump(the_geom)).geom AS the_geom '
							   'FROM cte_union '
							   'WHERE NOT ST_IsEmpty(the_geom)'
						   ') '
						   'SELECT ST_Multi(ST_Union('
							   'CASE '
								   'WHEN ST_IsValid(the_geom) THEN the_geom '
								   'ELSE ST_Buffer(the_geom,0) '
							   'END)) AS the_geom '
						   'FROM cte_dump'
					   ') '
					   'SELECT ROW_NUMBER() OVER() AS gid'
						   ',ST_Subdivide(the_geom) AS the_geom '
					   'FROM cte_repair', 
					   temp_table_boundary_subdiv, geom_exp, 
					   boundary_schema, boundary_table, negation)
		USING boundary_filter;
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Subdivided % AOI polygon(s) into temporary table %: %', 
			rows_affected, temp_table_boundary_subdiv, (clock_timestamp() - start_time);

		start_time := clock_timestamp();
		
		/* create spatial index on subdivided AOI polygons */
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   temp_table_boundary_subdiv);

		RAISE INFO 'Created spatial index on temporary table %: %', 
			temp_table_boundary_subdiv, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create temp table to store habitat polygons that intersect AOI */
		EXECUTE format('CREATE TEMP TABLE %1$I '
					   '('
						   'gid bigint NOT NULL PRIMARY KEY' 
						   ',bnd_gid bigint'
						   ',hab_gid bigint'
						   ',hab_type character varying'
						   ',eunis_l3 character varying'
						   ',the_geom_hab geometry'
						   ',the_geom_bnd geometry'
						   ',the_geom geometry(MultiPolygon,4326)'
					   ')',
					   temp_table_habitat_bound_subdiv);

		/* build habitat filter condition avoiding array comparison if habitat_types_filter has single element */
		habitat_type_where_clause := '';
		IF habitat_types_filter IS NOT NULL AND array_length(habitat_types_filter, 1) > 0 THEN
			IF array_length(habitat_types_filter, 1) = 1 THEN
				IF habitat_types_filter_negate THEN
					habitat_type_where_clause := format(' WHERE hab.%1$I != $2', 'eunis_l3', habitat_types_filter[1]);
				ELSE
					habitat_type_where_clause := format(' WHERE hab.%1$I = $2', 'eunis_l3', habitat_types_filter[1]);
				END IF;
			ELSE
				IF habitat_types_filter_negate THEN
					habitat_type_where_clause := format(' WHERE NOT hab.%1$I = ANY ($2)', 'eunis_l3');
				ELSE
					habitat_type_where_clause := format(' WHERE hab.%1$I = ANY ($2)', 'eunis_l3');
				END IF;
			END IF;
		ELSIF exclude_empty_mismatched_eunis_l3 THEN
			habitat_type_where_clause := format(' WHERE hab.%1$I IS NOT NULL', 'eunis_l3');
		END IF;

		/* find SRID of habitat table */
		srid := bh3_find_srid(habitat_schema, habitat_table, 'the_geom'::name);

		/* if habitat table SRID is not 4326 project its geometries into temp table and join that, 
		otherwise join habitat table to boundary table and insert intersecting geometries into temp table */
		IF srid != 4326 AND srid > 0 THEN
			EXECUTE format('CREATE TEMP TABLE %1$I '
						   '('
							   'gid bigint NOT NULL PRIMARY KEY'
							   ',hab_type character varying'
							   ',eunis_l3 character varying'
							   ',the_geom geometry(MultiPolygon,4326)'
						   ')',
						   temp_table_habitat_wgs84);

			EXECUTE format('INSERT INTO %1$I AS '
							   'SELECT hab.gid'
								   ',hab.hab_type'
								   ',hab.eunis_l3'
								   ',ST_Transform(hab.the_geom,4326) AS the_geom '
						   'FROM %2$I.%3$I hab'
						   '%4$s',
						   temp_table_habitat_wgs84, habitat_type_where_clause);

			EXECUTE format('INSERT INTO %1$I '
						   '('
							   'gid'
							   ',bnd_gid'
							   ',hab_gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',the_geom_hab'
							   ',the_geom_bnd '
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',bnd.gid AS bnd_gid'
							   ',hab.gid AS hab_gid'
							   ',hab.hab_type'
							   ',hab.eunis_l3'
							   ',hab.the_geom AS the_geom_hab'
							   ',bnd.the_geom AS the_geom_bnd '
						   'FROM %2$I hab '
							   'JOIN %3$I bnd ON ST_Intersects(hab.the_geom,bnd.the_geom)', 
						   temp_table_habitat_bound_subdiv, temp_table_habitat_wgs84, 
						   temp_table_boundary_subdiv);
		ELSE
			EXECUTE format('INSERT INTO %1$I '
						   '('
							   'gid'
							   ',bnd_gid'
							   ',hab_gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',the_geom_hab'
							   ',the_geom_bnd'
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',bnd.gid AS bnd_gid'
							   ',hab.gid AS hab_gid'
							   ',hab.hab_type'
							   ',hab.eunis_l3'
							   ',hab.the_geom AS the_geom_hab'
							   ',bnd.the_geom AS the_geom_bnd '
						   'FROM %2$I.%3$I hab '
							   'JOIN %4$I bnd ON ST_Intersects(hab.the_geom,bnd.the_geom)'
						   '%5$s', 
						   temp_table_habitat_bound_subdiv, habitat_schema, habitat_table, 
						   temp_table_boundary_subdiv, habitat_type_where_clause);
		END IF;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Inserted % habitat polygons intersecting AOI polygons into temporary table %: %', 
			rows_affected, temp_table_habitat_bound_subdiv, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* perform intersection of intersecting geometries */
		EXECUTE format('UPDATE %1$I '
					   'SET the_geom = ST_Multi(ST_Intersection(the_geom_hab,the_geom_bnd))',
					   temp_table_habitat_bound_subdiv);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Intersected % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_bound_subdiv, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* remove any empty geometries */
		EXECUTE format('DELETE FROM %1$I WHERE ST_IsEmpty(the_geom)', 
					   temp_table_habitat_bound_subdiv);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Deleted % empty geometries from temporary table %: %', 
			rows_affected, temp_table_habitat_bound_subdiv, (clock_timestamp() - start_time);

		/* repair any invalid geometries */
		EXECUTE format('UPDATE %1$I '
					   'SET the_geom = ST_Multi(ST_Buffer(the_geom,0)) '
					   'WHERE NOT ST_IsValid(the_geom) OR NOT ST_IsSimple(the_geom)', 
					   temp_table_habitat_bound_subdiv);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Repaired % invalid geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_bound_subdiv, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create spatial index on intersected geometries */
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   temp_table_habitat_bound_subdiv);

		RAISE INFO 'Created spatial index on temporary table %: %', 
			temp_table_habitat_bound_subdiv, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create habitat_sensitivity output table */
		EXECUTE format('CREATE TABLE %1$I.%2$I '
					   '('
						   'gid bigserial NOT NULL PRIMARY KEY'
						   ',the_geom geometry(MultiPolygon,4326)'
						   ',hab_type character varying'
						   ',eunis_l3 character varying'
						   ',sensitivity_ab_su_num_max smallint'
						   ',confidence_ab_su_num smallint'
						   ',sensitivity_ab_ss_num_max smallint'
						   ',confidence_ab_ss_num smallint'
					   ')', 
					   output_schema, output_table);

		IF exclude_empty_mismatched_eunis_l3 THEN
			left_join := '';
		ELSE
			left_join := 'LEFT ';
		END IF;

		/* insert re-assembled habitat polgyons with sensitivity scores into habitat_sensitivity output table */
		EXECUTE format('WITH cte_hab AS '
					   '('
						   'SELECT hab_gid AS gid'
							   ',hab_type'
							   ',eunis_l3'
							   ',ST_Multi(ST_Union(the_geom)) AS the_geom '
						   'FROM %1$I '
						   'GROUP BY hab_gid'
							   ',hab_type'
							   ',eunis_l3'
					   ') '
					   'INSERT INTO %2$I.%3$I '
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
					   'FROM cte_hab hab '
						   '%4$s JOIN %5$I.%6$I sbsh ON hab.eunis_l3 = sbsh.eunis_l3_code '
					   'WHERE NOT ST_IsEmpty(hab.the_geom)', 
					   temp_table_habitat_bound_subdiv, output_schema, output_table, 
					   left_join, sensitivity_schema, sensitivity_table);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Inserted % unioned geometries with broadscale habitat sensitivty scores into output table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);
																										  
		start_time := clock_timestamp();

		/* repair any invalid geometries */
		EXECUTE format('UPDATE %1$I.%2$I '
					   'SET the_geom = ST_Multi(ST_Buffer(the_geom,0)) '
					   'WHERE NOT ST_IsValid(the_geom) OR NOT ST_IsSimple(the_geom)', 
					   output_schema, output_table);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Repaired % invalid geometries in output table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create spatial and primary key indexes on habitat_sensitivity output table */
		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', 
					   output_schema, output_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', 
					   output_schema, output_table);

		RAISE INFO 'Created spatial and primary key indexes on temporary table %: %', 
			temp_table_habitat_bound_subdiv, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create grid within AOI and store in temporary table */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT * FROM bh3_create_csquares($1,false,$2,$3,$4,$5,4326)', 
					   temp_table_aoi_grid)
		USING boundary_filter, boundary_schema, boundary_table, 
			boundary_filter_negate, cell_size_degrees;

		/* index AOI grid table */
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   temp_table_aoi_grid);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', 
					   temp_table_aoi_grid);

		RAISE INFO 'Created % degree grid for AOI in temporary table %: %', 
			cell_size_degrees, temp_table_aoi_grid, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create gridded habitat sensitivity output table (schema equals that of 
		habitat sensitivity table plus row and col columns identifying grid square)	*/
		EXECUTE format('CREATE TABLE %1$I.%2$I '
					   '('
						   'gid bigserial NOT NULL PRIMARY KEY'
						   ',the_geom geometry(MultiPolygon,4326)'
						   ',"row" integer NOT NULL'
						   ',col integer NOT NULL'
						   ',gid_hab bigint NOT NULL'
						   ',hab_type character varying'
						   ',eunis_l3 character varying'
						   ',sensitivity_ab_su_num_max smallint'
						   ',confidence_ab_su_num smallint'
						   ',sensitivity_ab_ss_num_max smallint'
						   ',confidence_ab_ss_num smallint'
					   ')',
					   output_schema, output_table_grid);

		/* intersect habitat_sensitity output table with AOI grid into habitat_sensitivity_grid output table */
		EXECUTE format('INSERT INTO %1$I.%2$I '
					   '('
						   'gid'
						   ',the_geom'
						   ',"row"'
						   ',col'
						   ',gid_hab'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
					   ') '
					   'SELECT ROW_NUMBER() OVER() AS gid'
						   ',ST_Multi(ST_ClipByBox2D(hab.the_geom,g.the_geom)) AS the_geom'
						   ',g."row"'
						   ',g.col'
						   ',hab.gid AS gid_hab'
						   ',hab.hab_type'
						   ',hab.eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num '
					   'FROM %1$I.%3$I hab '
						   'JOIN %4$I g ON ST_Intersects(hab.the_geom,g.the_geom)', 
					   output_schema, output_table_grid, output_table, temp_table_aoi_grid);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Inserted % gridded geometries into output table %.%: %', 
			rows_affected, output_schema, output_table_grid, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* repair any invalid geometries */
		EXECUTE format('UPDATE %1$I.%2$I '
					   'SET the_geom = ST_Multi(ST_Buffer(the_geom,0)) '
					   'WHERE NOT ST_IsValid(the_geom) OR NOT ST_IsSimple(the_geom)', 
					   output_schema, output_table_grid);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Repaired % invalid geometries in temporary table %.%: %', 
			rows_affected, output_schema, output_table_grid, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* index habitat_sensitivity_grid output table */
		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', 
					   output_schema, output_table_grid);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', 
					   output_schema, output_table_grid);

		RAISE INFO 'Created spatial and primary key indexes on output table %.%: %', 
			output_schema, output_table_grid, (clock_timestamp() - start_time);

		/* drop temp tables */
		CALL bh3_drop_temp_table(temp_table_boundary_subdiv);
		CALL bh3_drop_temp_table(temp_table_habitat_bound_subdiv);
		CALL bh3_drop_temp_table(temp_table_habitat_wgs84);
		CALL bh3_drop_temp_table(temp_table_aoi_grid);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_habitat_clip_by_subdivide(integer, character varying[], name, name, name, name, name, name, name, name, name, boolean, boolean, boolean, numeric)
    OWNER TO postgres;
