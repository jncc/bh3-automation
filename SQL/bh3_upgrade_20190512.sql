ALTER FUNCTION public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_create_csquares(name, name, boolean, numeric, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_disturbance_map(name, name, name, name, timestamp without time zone, timestamp without time zone, name, name, name, name, name, name, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_entry(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_find_srid(name, name, name)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_get_pressure_csquares(name, name, timestamp without time zone, timestamp without time zone, name, name, name, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_get_pressure_csquares_size(name, timestamp without time zone, timestamp without time zone, name, name, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_habitat_boundary_clip(character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_habitat_grid(name, name, name, name, name, name, numeric)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_habitat_remove_overlaps(name, name)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_safe_difference(geometry, geometry)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_safe_union_transfn(geometry, geometry)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_sensitivity(sensitivity_source)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, timestamp without time zone, timestamp without time zone, name, character varying[], boolean, name, name, name, name, integer)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_sensitivity_map(name, name, name, name, name, name, name)
    OWNER TO bh3;

ALTER FUNCTION public.bh3_species_sensitivity_clipped(name, sensitivity_source, timestamp without time zone, timestamp without time zone, name, character varying[], boolean, integer)
    OWNER TO bh3;

ALTER PROCEDURE public.bh3_drop_spatial_tables(name[]) 
	OWNER TO bh3;

ALTER PROCEDURE public.bh3_drop_temp_table(name) 
	OWNER TO bh3;

ALTER PROCEDURE public.bh3_index(name, name, character varying[]) 
	OWNER TO bh3;

ALTER PROCEDURE public.bh3_procedure(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer) 
	OWNER TO bh3;

ALTER PROCEDURE public.bh3_repair_geometries(name, name, name)
	OWNER TO bh3;





DROP FUNCTION public.bh3_sensitivity_map(name, name, name, name, name, name, name);

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
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table]::name[]
			]::name[][]);

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
							   ',bh3_safe_difference(the_geom,bh3_safe_union(the_geom_erase)) AS the_geom '
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
		CALL bh3_index(NULL, species_sensitivity_mode_final_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

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
						   'FROM %4$I mod'
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
		CALL bh3_index(NULL, species_sensitivity_all_areas_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

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
					   			',bh3_safe_difference(the_geom,bh3_safe_union(the_geom_erase)) AS the_geom '
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
		CALL bh3_index(NULL, habitat_sensitivity_final_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

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

		CALL bh3_index(output_schema, output_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

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

ALTER FUNCTION public.bh3_sensitivity_map(name, name, name, name, name, name, name)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_sensitivity_map(name, name, name, name, name, name, name)
    IS 'Purpose:
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

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table';





DROP FUNCTION public.bh3_disturbance_map(name, name, name, name, timestamp without time zone, timestamp without time zone, name, name, name, name, name, name, integer);

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
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table]::name[]
				,ARRAY[output_schema, pressure_map_table]::name[]
			]::name[][]);

		RAISE INFO 'Creating pressure grid table %.%', output_schema, pressure_map_table;

		/* create pressure grid table */
		EXECUTE format('CREATE TABLE %1$I.%2$I '
					   '('
						   'gid bigint NOT NULL PRIMARY KEY'
						   ',the_geom geometry(MultiPolygon,%3$s)'
						   ',c_square character varying'
						   ',n bigint'
						   ',sar_surface_min double precision'
						   ',sar_surface_max double precision'
						   ',sar_surface_avg double precision'
						   ',sar_surface_cat_min integer'
						   ',sar_surface_cat_max integer'
						   ',sar_surface_cat_range integer'
						   ',sar_surface_variable boolean'
						   ',sar_surface double precision'
						   ',sar_surface_cat_comb integer'
						   ',sar_subsurface_min double precision'
						   ',sar_subsurface_max double precision'
						   ',sar_subsurface_avg double precision'
						   ',sar_subsurface_cat_min integer'
						   ',sar_subsurface_cat_max integer'
						   ',sar_subsurface_cat_range integer'
						   ',sar_subsurface_variable boolean'
						   ',sar_subsurface double precision'
						   ',sar_subsurface_cat_comb integer'
					  ')', 
					   output_schema, pressure_map_table, output_srid);

		RAISE INFO 'Inserting data into pressure grid table %.%', output_schema, pressure_map_table;

		EXECUTE format('INSERT INTO %1$I.%2$I '
					   '('
							'gid'
							',the_geom'
							',c_square'
							',n'
							',sar_surface_min'
							',sar_surface_max'
							',sar_surface_avg'
							',sar_surface_cat_min'
							',sar_surface_cat_max'
							',sar_surface_cat_range'
							',sar_surface_variable'
							',sar_surface'
							',sar_surface_cat_comb'
							',sar_subsurface_min'
							',sar_subsurface_max'
							',sar_subsurface_avg'
							',sar_subsurface_cat_min'
							',sar_subsurface_cat_max'
							',sar_subsurface_cat_range'
							',sar_subsurface_variable'
							',sar_subsurface'
							',sar_subsurface_cat_comb'
						') '
						'SELECT gid'
							',the_geom'
							',c_square'
							',n'
							',sar_surface_min'
							',sar_surface_max'
							',sar_surface_avg'
							',sar_surface_cat_min'
							',sar_surface_cat_max'
							',sar_surface_cat_range'
							',sar_surface_variable'
							',sar_surface'
							',sar_surface_cat_comb'
							',sar_subsurface_min'
							',sar_subsurface_max'
							',sar_subsurface_avg'
							',sar_subsurface_cat_min'
							',sar_subsurface_cat_max'
							',sar_subsurface_cat_range'
							',sar_subsurface_variable'
							',sar_subsurface'
							',sar_subsurface_cat_comb '
						'FROM bh3_get_pressure_csquares($1,$2,$3,$4,$5,$6,$7,$8)', 
						output_schema, pressure_map_table)
				USING boundary_schema, pressure_schema, date_start, date_end, boundary_table, 
					sar_surface_column, sar_subsurface_column, output_srid;
		
		/* index pressure grid table */
		CALL bh3_index(output_schema, pressure_map_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

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
		CALL bh3_index(output_schema, output_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

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
    OWNER TO bh3;

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
