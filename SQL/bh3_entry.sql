-- FUNCTION: public.bh3_entry(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer)

-- DROP FUNCTION public.bh3_entry(integer[], character varying[], timestamp without time zone, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_entry(
	boundary_filter integer[],
	habitat_types_filter character varying[],
	date_start timestamp without time zone,
	species_sensitivity_source_table sensitivity_source,
	pressure_schema name,
	output_schema name,
	output_owner character varying DEFAULT NULL::character varying,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'unofficial_country_waters_simplified_wgs84'::name,
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
	date_end timestamp without time zone DEFAULT now(
	),
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
