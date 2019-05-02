-- FUNCTION: public.bh3_habitat_boundary_clip(character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean)

-- DROP FUNCTION public.bh3_habitat_boundary_clip(character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean);

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

		CALL bh3_index(NULL, temp_table_habitat_boundary_intersect, 
					   ARRAY[
						   ARRAY['the_geom','s']
					   ]);

		RAISE INFO 'bh3_habitat_boundary_clip: Created spatial index on temporary table %: %', 
			temp_table_habitat_boundary_intersect, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* remove any previous putput table left behind */
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table]::name[]
			]::name[][]);

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
		CALL bh3_index(output_schema, output_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

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
