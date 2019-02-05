-- FUNCTION: public.bh3_habitat_boundary_clip(integer, character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean, boolean)

-- DROP FUNCTION public.bh3_habitat_boundary_clip(integer, character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean, boolean);

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

		RAISE INFO 'Removed any outputs of previous runs: %', (clock_timestamp() - start_time);

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
		RAISE INFO 'Inserted % rows into temporary table %: %', 
			rows_affected, temp_table_boundary_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('UPDATE %1$I '
					   'SET the_geom = ST_Buffer(the_geom,0) '
					   'WHERE NOT ST_IsValid(the_geom) OR NOT ST_IsSimple(the_geom)',
					   temp_table_boundary_subdivide);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('ALTER TABLE %1$I ADD CONSTRAINT %1$s_pkey PRIMARY KEY(gid)', 
					   temp_table_boundary_subdivide);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)', 
					   temp_table_boundary_subdivide);
		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   temp_table_boundary_subdivide);

		RAISE INFO 'Indexed temporary table %: %', 
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
			IF length(habitat_types) = 1 THEN
				IF habitat_types_filter_negate THEN
					habitat_type_condition := format('hab.%1$I != $2', 'eunis_l3', habitat_types[1]);
				ELSE
					habitat_type_condition := format('hab.%1$I = $2', 'eunis_l3', habitat_types[1]);
				END IF;
			ELSE
				IF habitat_types_filter_negate THEN
					habitat_type_condition := format('NOT hab.%1$I = ANY ($2)', 'eunis_l3');
				ELSE
					habitat_type_condition := format('hab.%1$I = ANY ($2)', 'eunis_l3');
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
					   temp_table_boundary_subdivide, habitat_type_condition);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Inserted % rows into temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('UPDATE %1$I '
					   'SET the_geom = ST_Buffer(the_geom,0) '
					   'WHERE NOT ST_IsValid(the_geom) OR NOT ST_IsSimple(the_geom)',
					   temp_table_habitat_boundary_intersect);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT hab.gid'
						   ',ST_Multi(ST_Union(hab.the_geom)) AS the_geom'
						   ',hab.hab_type'
						   ',hab.eunis_l3 '
					   'FROM %2$I hab '
					   'WHERE the_geom IS NOT NULL '
						   'AND NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1 '
					   'GROUP BY hab.gid'
						   ',hab.hab_type'
						   ',hab.eunis_l3',
					   temp_table_habitat_boundary_intersect_union,
					   temp_table_habitat_boundary_intersect)
		USING 'Polygon';

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Inserted % rows into temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect_union, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('UPDATE %1$I '
					   'SET the_geom = ST_Buffer(the_geom,0) '
					   'WHERE NOT ST_IsValid(the_geom) OR NOT ST_IsSimple(the_geom)',
					   temp_table_habitat_boundary_intersect_union);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect_union, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('ALTER TABLE %1$I ADD CONSTRAINT %1$s_pkey PRIMARY KEY(gid)',
					   temp_table_habitat_boundary_intersect_union);
		EXECUTE format('CREATE UNIQUE INDEX idx_%1$s_gid ON %1$I USING BTREE(gid)',
					   temp_table_habitat_boundary_intersect_union);
		EXECUTE format('CREATE INDEX idx_%1$s_eunis_l3gid ON %1$I USING BTREE(eunis_l3)',
					   temp_table_habitat_boundary_intersect_union);

		RAISE INFO 'Indexed temporary table %: %', 
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
		RAISE INFO 'Inserted % rows into output table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* index habitat sensitivity output table */
		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', output_schema, output_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', output_schema, output_table);

		RAISE INFO 'indexed output table %.%: %', 
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

			RAISE INFO 'Removed overlaps from output table %.%: %', 
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
		RAISE INFO 'Error text: %', exc_text;
		RAISE INFO 'Error detail: %', exc_detail;
		RAISE INFO 'Error hint: %', exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_habitat_boundary_clip(integer, character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean, boolean)
    OWNER TO postgres;
