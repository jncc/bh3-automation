DROP FUNCTION IF EXISTS public.bh3_disturbance_map(name, name, name, name, timestamp without time zone, timestamp without time zone, name, name, name, name, name, integer);

CREATE OR REPLACE FUNCTION public.bh3_disturbance_map(
	boundary_schema name,
	pressure_schema name,
	sensitivity_map_schema name,
	output_schema name,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(),
	boundary_table name DEFAULT 'boundary'::name,
	sensitivity_map_table name DEFAULT 'sensitivity_map'::name,
	pressure_map_table name DEFAULT 'pressure_map'::name,
	output_table name DEFAULT 'disturbance_map'::name,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
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

		RAISE INFO 'Deleting previous output table %.%', output_schema, output_table;
	
		/* clean up any previous output left behind */
		FOR tn IN 
			EXECUTE format('SELECT c.relname '
						   'FROM pg_class c '
							   'JOIN pg_namespace n ON c.relnamespace = n.oid '
						   'WHERE n.nspname = $1 AND c.relname IN($2,$3)')
			USING output_schema, output_table, pressure_map_table 
		LOOP
			EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING output_schema, tn;
		END LOOP;

		RAISE INFO 'Creating pressure grid table %.%', output_schema, pressure_map_table;

		/* store pressure c-squares in temporary table */
		EXECUTE format('CREATE TABLE %1$I.%2$I AS '
					   'SELECT * FROM bh3_get_pressure_csquares($1,$2,$3,$4,$5,$6,$7,$8)',
					   output_schema, pressure_map_table)
		USING boundary_schema, pressure_schema, date_start, date_end, boundary_table, 
			sar_surface_column, sar_subsurface_column, output_srid;
	
		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', output_schema, pressure_map_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', output_schema, pressure_map_table);

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
						   'JOIN %4$I.%5$I prs ON ST_Intersects(%1$s,prs.the_geom)',
					   geom_exp_sen, sensitivity_map_schema, sensitivity_map_table, output_schema, pressure_map_table);

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

ALTER FUNCTION public.bh3_disturbance_map(name, name, name, name, timestamp without time zone, timestamp without time zone, name, name, name, name, name, name, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_disturbance_map(name, name, name, name, timestamp without time zone, timestamp without time zone, name, name, name, name, name, name, integer)
    IS 'Purpose:
Creates the disturbance map from sensitivity and pressure maps.

Approach:
Creates a table of pressure c-squares calling the bh3_get_pressure_csquares function.
Then, using a cursor, the disturbance map table is populated computing surface and subsurface disturbance scores 
surface and subsurface abrasion sensitivity scores from the sensitivity map with categorised combined surface and 
sub-surface abrasion scores from the pressure c-squares using case expressions and a geometry as the intersection 
of sensitivity and pressure c-square geometries.

Parameters:
boundary_schema			name							Schema of table containing single AOI boundary polygon and bounding box.
pressure_schema			name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
sensitivity_map_schema	name							Schema in which sensitivity map table is located.
output_schema			name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
date_start				timestamp without time zone		Earliest date for Marine Recorder species samples to be included.
date_end				timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_table			name							Name of table containing single AOI boundary polygon and bounding box. Defaults to ''boundary''.
sensitivity_map_table	name							Table name of sensitivity map. Defaults to ''sensitivity_map''.
pressure_map_table		name							Table name of pressure map, created in output_schema. Defaults to ''pressure_map''.
output_table			name							Table name of output disturbance map. Defaults to ''disturbance_map''.
sar_surface_column		name							SAR surface column name in pressure source tables. Defaults to ''sar_surface''.
sar_subsurface_column	name							SAR sub-surface column name in pressure source tables. Defaults to ''sar_subsurface''.
output_srid				integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
Table of error records from cursor loop.

Calls:
bh3_drop_temp_table
bh3_get_pressure_csquares
bh3_find_srid';




DROP FUNCTION public.bh3_habitat_boundary_clip(character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean);

CREATE OR REPLACE FUNCTION public.bh3_habitat_boundary_clip(
	habitat_types_filter character varying[],
	output_schema name,
	output_table name DEFAULT 'habitat_sensitivity'::name,
	habitat_schema name DEFAULT 'static'::name,
	habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name,
	sensitivity_schema name DEFAULT 'lut'::name,
	sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name,
	boundary_subdivide_schema name DEFAULT 'static'::name,
	boundary_subdivide_table name DEFAULT 'boundary_subdivide'::name,
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
	cand_cursor refcursor;
	cand_row record;
	error_count bigint;
	geom_union geometry;
	rec_count integer;
	previous_gid bigint;
	previous_hab_type character varying;
	previous_eunis_l3 character varying;
	previous_sensitivity_ab_su_num_max smallint;
	previous_confidence_ab_su_num smallint;
	previous_sensitivity_ab_ss_num_max smallint;
	previous_confidence_ab_ss_num smallint;


BEGIN
	BEGIN
		success := false;

		temp_table_habitat_boundary_intersect := 'habitat_boundary_intersect'::name;
		temp_table_habitat_boundary_intersect_union := 'habitat_boundary_intersect_union'::name;

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
						   'JOIN %5$I.%6$I bnd ON ST_Intersects(hab.the_geom,bnd.the_geom) '
					   'WHERE %7$s',
					   temp_table_habitat_boundary_intersect, geom_exp_hab,
					   habitat_schema, habitat_table, boundary_subdivide_schema, 
					   boundary_subdivide_table, habitat_type_condition)
		USING habitat_types_filter;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(NULL, temp_table_habitat_boundary_intersect);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('CREATE INDEX sidx_%1$s_the_geom ON %1$I USING GIST(the_geom)', 
					   temp_table_habitat_boundary_intersect);

		RAISE INFO 'bh3_habitat_boundary_clip: Created spatial index on temporary table %: %', 
			temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* remove any previous putput table left behind */
		FOR tn IN 
			EXECUTE format('SELECT c.relname '
						   'FROM pg_class c '
							   'JOIN pg_namespace n ON c.relnamespace = n.oid '
						   'WHERE n.nspname = $1 AND c.relname IN($2)')
			USING output_schema, output_table
		LOOP
			EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING output_schema, tn;
		END LOOP;

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

		previous_gid := NULL;
		previous_hab_type := NULL;
		previous_eunis_l3 := NULL;
		previous_sensitivity_ab_su_num_max := NULL;
		previous_confidence_ab_su_num := NULL;
		previous_sensitivity_ab_ss_num_max := NULL;
		previous_confidence_ab_ss_num := NULL;
		geom_union := NULL;
		rec_count := 0;
		rows_affected := 0;

		OPEN cand_cursor FOR 
		EXECUTE format('SELECT ROW_NUMBER() OVER(PARTITION BY hab.gid,hab.hab_type,hab.eunis_l3) AS row_id'
						   ',hab.gid'
						   ',hab.the_geom'
						   ',hab.hab_type'
						   ',hab.eunis_l3'
						   ',sbsh.sensitivity_ab_su_num_max'
						   ',sbsh.confidence_ab_su_num'
						   ',sbsh.sensitivity_ab_ss_num_max'
						   ',sbsh.confidence_ab_ss_num '
					   'FROM %1$I hab '
						   '%2$s JOIN %3$I.%4$I sbsh ON hab.eunis_l3 = sbsh.eunis_l3_code '
					   'WHERE the_geom IS NOT NULL '
						   'AND NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1',
					   temp_table_habitat_boundary_intersect, left_join,
					   sensitivity_schema, sensitivity_table)
		USING 'Polygon';

		FETCH cand_cursor INTO cand_row;
		IF FOUND THEN
			previous_gid := cand_row.gid;
			previous_hab_type := cand_row.hab_type;
			previous_eunis_l3 := cand_row.eunis_l3;
			previous_sensitivity_ab_su_num_max := cand_row.sensitivity_ab_su_num_max;
			previous_confidence_ab_su_num := cand_row.confidence_ab_su_num;
			previous_sensitivity_ab_ss_num_max := cand_row.sensitivity_ab_ss_num_max;
			previous_confidence_ab_ss_num := cand_row.confidence_ab_ss_num;
			geom_union := cand_row.the_geom;
		END IF;

		LOOP
			BEGIN
				FETCH cand_cursor INTO cand_row;

				rec_count := rec_count + 1;

				IF FOUND AND cand_row.gid = previous_gid AND cand_row.hab_type = previous_hab_type AND cand_row.eunis_l3 = previous_eunis_l3 THEN
					IF geom_union IS NULL THEN
						geom_union := cand_row.the_geom;
					ELSE
						geom_union := bh3_safe_union_transfn(geom_union, cand_row.the_geom);
					END IF;
				ELSE
					IF previous_gid IS NOT NULL THEN
						/* repair union geometry if necessary */
						IF NOT ST_IsValid(geom_union) THEN
							geom_union := ST_Multi(ST_Buffer(geom_union, 0));
						ELSE
							geom_union := ST_Multi(geom_union);
						END IF;

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
									   'VALUES($1,$2,$3,$4,$5,$6,$7,$8)', 
									   output_schema, output_table) 
						USING previous_gid,
							geom_union,
							previous_hab_type,
							previous_eunis_l3,
							previous_sensitivity_ab_su_num_max,
							previous_confidence_ab_su_num,
							previous_sensitivity_ab_ss_num_max,
							previous_confidence_ab_ss_num;

						rows_affected := rows_affected + 1;
						RAISE INFO 'bh3_habitat_boundary_clip: Looping over cursor. Row: %. Inserted row: %. Runtime: %', 
							rec_count, rows_affected, (clock_timestamp() - start_time);
					END IF;

					IF FOUND THEN
						/* begin next window */
						previous_gid := cand_row.gid;
						previous_hab_type := cand_row.hab_type;
						previous_eunis_l3 := cand_row.eunis_l3;
						previous_sensitivity_ab_su_num_max := cand_row.sensitivity_ab_su_num_max;
						previous_confidence_ab_su_num := cand_row.confidence_ab_su_num;
						previous_sensitivity_ab_ss_num_max := cand_row.sensitivity_ab_ss_num_max;
						previous_confidence_ab_ss_num := cand_row.confidence_ab_ss_num;
						geom_union := cand_row.the_geom;
					ELSE
						EXIT;
					END IF;
				END IF;
			EXCEPTION WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
										  exc_detail = PG_EXCEPTION_DETAIL,
										  exc_hint = PG_EXCEPTION_HINT;
				error_count := error_count + 1;
				RAISE INFO 'Error. Text: %. Detail: %. Hint: %.', exc_text, exc_detail, exc_hint;
			END;
		END LOOP;

		CLOSE cand_cursor;

		RAISE INFO 'bh3_habitat_boundary_clip: Inserted % rows into table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(output_schema, output_table);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_boundary_clip: Repaired % geometries in table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* index habitat sensitivity output table */
		EXECUTE format('CREATE INDEX sidx_%2$s_the_geom ON %1$I.%2$I USING GIST(the_geom)', output_schema, output_table);
		EXECUTE format('CREATE UNIQUE INDEX idx_%2$s_gid ON %1$I.%2$I USING BTREE(gid)', output_schema, output_table);

		RAISE INFO 'bh3_habitat_boundary_clip: indexed output table %.%: %', 
			output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

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

ALTER FUNCTION public.bh3_habitat_boundary_clip(character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_habitat_boundary_clip(character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean)
    IS 'Purpose:
Creates habitat_sensitivity table for the selected AOI in the selected output schema.

Approach:
The AOI polygon/s is/are split into smaller parts with no more than 256 vertices each. These smaller parts are spatially joined to the habitat table 
and the intersection of overlapping polygons is computed. The resulting polygons are then aggregated by the gid of the original habitat records, 
computing the union of the pieces of the original polygons.
Despite the extra steps and multiple geometry repairs between them, this is substantially faster than computing intersections directly with 
large boundary polygons.

Parameters:
habitat_types_filter				character varying[]		Array of EUNIS L3 codes to be included or excluded.
output_schema						name					Schema of the output habitat sensitivity table.
output_table						name					Name of the output habitat sensitivity table. Defaults to ''habitat_sensitivity''.
habitat_schema						name					Schema of the habitat table. Defaults to ''static''.
habitat_table						name					Name of the habitat table. Defaults to ''uk_habitat_map_wgs84''.
sensitivity_schema					name					Schema of the habitat sensitvity lookup table. Defaults to ''lut''.
sensitivity_table					name					Name of the habitat sensitvity lookup table. Defaults to ''sensitivity_broadscale_habitats''.
boundary_subdivide_schema			name					Schema of the subdivided boundary table defining the AOI. Defaults to ''static''.
boundary_subdivide_table			name					Name of the subdivided boundary table defining the AOI. Defaults to ''official_country_waters_wgs84''.
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





DROP FUNCTION public.bh3_entry(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_entry(
	boundary_filter integer[],
	habitat_types_filter character varying[],
	date_start timestamp without time zone,
	species_sensitivity_source_table sensitivity_source,
	pressure_schema name,
	output_schema name,
	output_owner character varying DEFAULT NULL::character varying,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
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

ALTER FUNCTION public.bh3_entry(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_entry(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer)
    IS 'Purpose:
Main entry point that starts a BH3 run. 
This is called by the QGIS user interface and may be executed directly in pgAdmin or on the PostgreSQL command line.

Approach:
Creates the output schema and an error_log table in it if they do not already exist.
Then calls the bh3_get_pressure_csquares_size, bh3_habitat_boundary_clip, bh3_habitat_grid, bh3_sensitivity_layer_prep, 
bh3_sensitivity_map and bh3_disturbance_map functions, inserting any error rows returned into the error_log table.

Parameters:
boundary_filter						integer[]						Array of primary key values (gid) of AOI polygons in boundary_table to be included (or excluded if boundary_filter_negate is true).
habitat_types_filter				character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
date_start							timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
species_sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
pressure_schema						name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
output_schema						name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
output_owner						character varying				Role that will own output schema and tables. Defaults to NULL, which means the user running the procedure.
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
pressure_map_table					name							Table name of pressure map, created in output_schema. Defaults to ''pressure_map''.
disturbance_map_table				name							Table name of output disturbance map. Defaults to ''disturbance_map''.
sar_surface_column					name							SAR surface column name in pressure source tables. Defaults to ''sar_surface''.
sar_subsurface_column				name							SAR sub-surface column name in pressure source tables. Defaults to ''sar_subsurface''.
date_end							timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_filter_negate				boolean							If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
habitat_types_filter_negate			boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
remove_overlaps						boolean							If true overlaps will be removed from habitat_sensitivity_table (significantly increases processing time). Defaults to false.
output_srid							integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
true on success, false on failure.

Calls:
bh3_get_pressure_csquares_size
bh3_boundary_subdivide
bh3_habitat_boundary_clip
bh3_habitat_grid
bh3_sensitivity_layer_prep
bh3_sensitivity_map
bh3_disturbance_map';




DROP PROCEDURE public.bh3_procedure(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer);

CREATE OR REPLACE PROCEDURE public.bh3_procedure(
	boundary_filter integer[],
	habitat_types_filter character varying[],
	date_start timestamp without time zone,
	species_sensitivity_source_table sensitivity_source,
	pressure_schema name,
	output_schema name,
	output_owner character varying DEFAULT NULL::character varying,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
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

COMMENT ON PROCEDURE public.bh3_procedure
    IS 'Purpose:
Main entry point that starts a BH3 run. 
This is called by the QGIS user interface and may be executed directly in pgAdmin or on the PostgreSQL command line.

Approach:
Creates the output schema and an error_log table in it if they do not already exist.
Then calls the bh3_get_pressure_csquares_size, bh3_habitat_boundary_clip, bh3_habitat_grid, bh3_sensitivity_layer_prep, 
bh3_sensitivity_map and bh3_disturbance_map functions, inserting any error rows returned into the error_log table.

Parameters:
boundary_filter						integer[]						Array of primary key values (gid) of AOI polygons in boundary_table to be included (or excluded if boundary_filter_negate is true).
habitat_types_filter				character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
date_start							timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
species_sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
pressure_schema						name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
output_schema						name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
output_owner						character varying				Role that will own output schema and tables. Defaults to NULL, which means the user running the procedure.
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
pressure_map_table					name							Table name of pressure map, created in output_schema. Defaults to ''pressure_map''.
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
bh3_boundary_subdivide
bh3_habitat_boundary_clip
bh3_habitat_grid
bh3_sensitivity_layer_prep
bh3_sensitivity_map
bh3_disturbance_map';
