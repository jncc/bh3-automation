-- FUNCTION: public.bh3_habitat_grid(name, name, name, name, name, name, numeric)

-- DROP FUNCTION public.bh3_habitat_grid(name, name, name, name, name, name, numeric);

CREATE OR REPLACE FUNCTION public.bh3_habitat_grid(
	boundary_schema name,
	habitat_sensitivity_schema name,
	output_schema name,
	boundary_table name DEFAULT 'boundary'::name,
	habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name,
	output_table name DEFAULT 'habitat_sensitivity_grid'::name,
	cell_size_degrees numeric DEFAULT 0.05)
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
	aoi_grid_table name;
	srid_hab integer;
	geom_exp_hab character varying;
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

BEGIN
	BEGIN
		start_time := clock_timestamp();

		aoi_grid_table := 'aoi_grid'::name;

		/* clean up any previous output left behind */
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table]::name[]
			]::name[][]);

		/* clean up any previous temp table left behind */
		CALL bh3_drop_temp_table(aoi_grid_table);

		/* build geometry expression for habitat_sensitivity_table query that projects geometry to 4326 if necessary */
		srid_hab := bh3_find_srid(habitat_sensitivity_schema, habitat_sensitivity_table, 'the_geom'::name);
		IF srid_hab != 4326 AND srid_hab > 0 THEN
			geom_exp_hab := format('ST_Transform(hab.the_geom,%s)', 4326);
		ELSE
			geom_exp_hab := 'hab.the_geom';
		END IF;

		rec_count := 0;
		error_count := 0;
		
		/* create gridded habitat sensitivity output table (schema equals that of 
		habitat sensitivity table plus row and col columns identifying grid square)	*/
		EXECUTE format('CREATE TABLE %1$I.%2$I('
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
						   ',confidence_ab_ss_num smallint)',
					   output_schema, output_table);

		/* create grid for AOI in temporary table */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT * FROM bh3_create_csquares($1,$2,false,$3,4326)', 
					   aoi_grid_table)
		USING boundary_schema, boundary_table, cell_size_degrees;

		/* index gridded habitat sensitivity output table */
		CALL bh3_index(NULL, aoi_grid_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

		/* create cursor spatially joining habitat sensitivity output table and c-square grid covering AOI */
		OPEN cand_cursor FOR
		EXECUTE format('SELECT g.row'
						   ',g.col'
						   ',hab.gid'
						   ',hab.hab_type'
						   ',hab.eunis_l3'
						   ',hab.sensitivity_ab_su_num_max'
						   ',hab.confidence_ab_su_num'
						   ',hab.sensitivity_ab_ss_num_max'
						   ',hab.confidence_ab_ss_num'
						   ',%1$s AS geom_hab'
						   ',g.the_geom AS geom_grid ' 
					   'FROM %2$I.%3$I hab '
						   'JOIN %4$I g ON ST_Intersects(%1$s,g.the_geom)',
					   geom_exp_hab, habitat_sensitivity_schema, 
					   habitat_sensitivity_table, aoi_grid_table);

		/* loop over cursor, intersecting habitat polygons with grid squares (using fast ST_ClipByBox2D function) */
		LOOP
			BEGIN
				FETCH cand_cursor INTO cand_row;
				EXIT WHEN NOT FOUND;

				rec_count := rec_count + 1;
				RAISE INFO 'bh3_habitat_grid: Looping over cursor. Row: %. Runtime: %', rec_count, (clock_timestamp() - start_time);

				geom := ST_Multi(ST_ClipByBox2D(cand_row.geom_hab,cand_row.geom_grid));
				/* repair clipped geometry if necessary */
				IF NOT ST_IsValid(geom) THEN
					geom := ST_Multi(ST_Buffer(geom, 0));
				END IF;

				EXECUTE format('INSERT INTO %1$I.%2$I ('
								   'the_geom'
								   ',"row"'
								   ',col'
								   ',gid_hab'
								   ',hab_type'
								   ',eunis_l3'
								   ',sensitivity_ab_su_num_max'
								   ',confidence_ab_su_num'
								   ',sensitivity_ab_ss_num_max'
								   ',confidence_ab_ss_num) '
							   'VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
								output_schema, output_table)
				USING geom 
					,cand_row.row
					,cand_row.col
					,cand_row.gid
					,cand_row.hab_type
					,cand_row.eunis_l3
					,cand_row.sensitivity_ab_su_num_max
					,cand_row.confidence_ab_su_num
					,cand_row.sensitivity_ab_ss_num_max
					,cand_row.confidence_ab_ss_num;
			EXCEPTION WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
										  exc_detail = PG_EXCEPTION_DETAIL,
										  exc_hint = PG_EXCEPTION_HINT;
				error_count := error_count + 1;
				error_gids := error_gids || cand_row.gid::bigint;
				error_texts := error_texts || exc_text;
				error_details := error_details || exc_detail;
				error_hints := error_hints || exc_hint;
				RAISE INFO 'bh3_habitat_grid: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
			END;
		END LOOP;

		CLOSE cand_cursor;

		/* index gridded habitat sensitivity output table */
		CALL bh3_index(output_schema, output_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

		/* drop aoi_grid temp table  */
		CALL bh3_drop_temp_table(aoi_grid_table);

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
		RAISE INFO 'Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_habitat_grid(name, name, name, name, name, name, numeric)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_habitat_grid(name, name, name, name, name, name, numeric)
    IS 'Purpose:
Creates a gridded version of the habitat_sensitivity_table.

Approach:
Creates a c-square grid table within the specified boundary and intersects it with polygons from the previously created, 
ungridded habitat_sensitivity_table, calling the fast ST_ClipByBox2D in a loop over a cursor. 

Parameters:
boundary_schema				name		Schema of table containing single AOI boundary polygon and bounding box.
habitat_sensitivity_schema	name		Schema of the habitat sensitivity table.
output_schema				name		Schema of the output gridded habitat sensitivity table.
boundary_table				name		Name of table containing single AOI boundary polygon and bounding box. Defaults to ''boundary''.
habitat_sensitivity_table	name		Name of habitat sensitivity table. Defaults to ''habitat_sensitivity''.
output_table				name		Name of gridded habitat sensitivity output table. Defaults to ''habitat_sensitivity_grid''.
cell_size_degrees			numeric		Cell size in degrees. Defaults to 0.05.

Returns:
Table of error records from cursor loop.

Calls:
bh3_drop_temp_table
bh3_find_srid
bh3_create_csquares';
