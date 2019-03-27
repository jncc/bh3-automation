-- PROCEDURE: public.bh3_procedure(integer, character varying[], timestamp without time zone, sensitivity_source, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer)

-- DROP PROCEDURE public.bh3_procedure(integer, character varying[], timestamp without time zone, sensitivity_source, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, timestamp without time zone, boolean, boolean, boolean, integer);

CREATE OR REPLACE PROCEDURE public.bh3_procedure(
	boundary_filter integer,
	habitat_types_filter character varying[],
	date_start timestamp without time zone,
	species_sensitivity_source_table sensitivity_source,
	pressure_schema name,
	output_schema name,
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
	disturbance_map_table name DEFAULT 'disturbance_map'::name,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	date_end timestamp without time zone DEFAULT now(
	),
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

BEGIN
	start_time := clock_timestamp();
	error_count := 0;
	
	BEGIN
		/* create output_schema if it doesn't already exist */
		EXECUTE format('CREATE SCHEMA IF NOT EXISTS %1$I', output_schema);

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
		FROM public.bh3_get_pressure_csquares_size(
			pressure_schema							--pressure_schema name
			,date_start								--date_start timestamp without time zone
			,date_end								--date_end timestamp without time zone DEFAULT now()
			,sar_surface_column						--sar_surface_column name DEFAULT 'sar_surface'::name
			,sar_subsurface_column					--sar_subsurface_column name DEFAULT 'sar_subsurface'::name
			,output_srid							--output_srid integer DEFAULT 4326
		) INTO cell_size_degrees;
		
		IF cell_size_degrees IS NULL OR cell_size_degrees < 0 THEN
			RAISE INFO 'Failed to obtain a consisten cell size from tables in pressure schema %', pressure_schema;
			RETURN;
		END IF;

		RAISE INFO 'Calling bh3_habitat_boundary_clip: %', (clock_timestamp() - start_time);

		/* create habitat_sensitivity for selected AOI in output_schema */
		SELECT * 
		FROM public.bh3_habitat_boundary_clip(
			boundary_filter							--boundary_filter integer,
			,habitat_types_filter					--habitat_types_filter character varying[]
			,output_schema							--output_schema name
			,habitat_sensitivity_table				--output_table name DEFAULT 'habitat_sensitivity'::name
			,habitat_schema							--habitat_schema name DEFAULT 'static'::name
			,habitat_table							--habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name
			,habitat_sensitivity_lookup_schema		--sensitivity_schema name DEFAULT 'lut'::name
			,habitat_sensitivity_lookup_table		--sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name
			,boundary_schema						--boundary_schema name DEFAULT 'static'::name
			,boundary_table							--boundary_table name DEFAULT 'official_country_waters_wgs84'::name
			,habitat_types_filter_negate			--boundary_filter_negate boolean DEFAULT false
			,habitat_types_filter_negate			--habitat_types_filter_negate boolean DEFAULT false
			,true									--exclude_empty_mismatched_eunis_l3 boolean DEFAULT true
			,remove_overlaps						--remove_overlaps boolean DEFAULT false
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

		RAISE INFO 'Calling bh3_habitat_grid: %', (clock_timestamp() - start_time);

		/* grid habitat_sensitivity into create habitat_sensitivity_grid  
		outputs:	habitat_sensitivity_grid
		calls:		bh3_drop_temp_table
					bh3_find_srid
					bh3_create_csquares
					bh3_create_fishnet
		run time: 	24 secs 422 msec (Wales) */
		FOR error_curs_rec IN 
			SELECT * 
			FROM public.bh3_habitat_grid(
				boundary_filter						--boundary_filter integer
				,output_schema						--,habitat_sensitivity_schema name
				,output_schema						--,output_schema name
				,habitat_sensitivity_table			--,habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name
				,habitat_sensitivity_grid_table		--,output_table name DEFAULT 'habitat_sensitivity_grid'::name
				,boundary_schema					--,boundary_schema name DEFAULT 'static'::name
				,boundary_table						--,boundary_table name DEFAULT 'official_country_waters_wgs84'::name
				,boundary_filter_negate				--,boundary_filter_negate boolean DEFAULT false
				,cell_size_degrees					--,cell_size_degrees numeric DEFAULT 0.05
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
		output_schema using habitat_sensitivity and habitat_sensitivity_grid created in presvious step,
		rock/eco group sensitivity scores and Marine Recorder samples starting 2012-01-01. 
		outputs:	species_sensitivity_max
					species_sensitivity_mode
		calls:		bh3_find_srid
					bh3_drop_temp_table
					bh3_species_sensitivity_clipped
					bh3_sensitivity
		run time:	32 secs 852 msec (Wales) */
		SELECT * 
		FROM bh3_sensitivity_layer_prep(
			output_schema							--habitat_schema name
			,output_schema							--output_schema name
			,boundary_filter						--boundary_filter integer
			,species_sensitivity_source_table		--sensitivity_source_table sensitivity_source
			,date_start								--date_start timestamp without time zone
			,date_end								--date_end timestamp without time zone DEFAULT now()
			,habitat_types_filter					--habitat_types_filter character varying[] DEFAULT NULL
			,habitat_sensitivity_table				--habitat_table name DEFAULT 'habitat_sensitivity'::name
			,habitat_sensitivity_grid_table			--habitat_table_grid name DEFAULT 'habitat_sensitivity_grid'::name
			,species_sensitivity_max_table			--output_table_max name DEFAULT 'species_sensitivity_max'::name
			,species_sensitivity_mode_table			--output_table_mode name DEFAULT 'species_sensitivity_mode'::name
			,boundary_schema						--boundary_schema name DEFAULT 'static'::name
			,boundary_table							--boundary_table name DEFAULT 'official_country_waters_wgs84'::name
			,boundary_filter_negate					--boundary_filter_negate boolean DEFAULT false
			,habitat_types_filter_negate			--habitat_types_filter_negate boolean DEFAULT false
			,output_srid							--output_srid integer DEFAULT 4326
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
		and species_sensitivity_mode tables, all located in schema output_schema
		outputs:	sensitivity_map
		calls:		bh3_drop_temp_table
		run time: 6 min 7 secs (Wales) */
		SELECT * 
		FROM bh3_sensitivity_map(
			output_schema							--habitat_sensitivity_schema name
			,output_schema							--species_sensitivity_schema name
			,output_schema							--output_schema name
			,habitat_sensitivity_table				--habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name
			,species_sensitivity_max_table			--species_sensitivity_max_table name DEFAULT 'species_sensitivity_max'::name
			,species_sensitivity_mode_table			--species_sensitivity_mode_table name DEFAULT 'species_sensitivity_mode'::name
			,sensitivity_map_table					--output_table name DEFAULT 'sensitivity_map'::name
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

		/* create pressure_map for selected AOI in output_schema from sensitivity_map 
		in output_schema and pressure tables in pressure_schema.
		outputs:	pressure_map
		calls:		bh3_drop_temp_table
					bh3_get_pressure_csquares
					bh3_find_srid
		run time:	55 secs 313 msec (Wales) */
		FOR error_curs_rec IN 
			SELECT *
			FROM bh3_disturbance_map(
				boundary_filter						--boundary_filter integer
				,date_start							--date_start timestamp without time zone
				,pressure_schema					--pressure_schema name 
				,output_schema						--sensitivity_map_schema name
				,output_schema						--output_schema name
				,boundary_schema					--boundary_schema name DEFAULT 'static'::name
				,boundary_table						--boundary_table name DEFAULT 'official_country_waters_wgs84'::name
				,sensitivity_map_table				--sensitivity_map_table name DEFAULT 'sensitivity_map'::name
				,disturbance_map_table				--output_table name DEFAULT 'disturbance_map'::name
				,sar_surface_column					--sar_surface_column name DEFAULT 'sar_surface'::name
				,sar_subsurface_column				--sar_subsurface_column name DEFAULT 'sar_subsurface'::name
				,boundary_filter_negate				--boundary_filter_negate boolean DEFAULT false
				,date_end							--date_end timestamp without time zone DEFAULT now()
				,output_srid						--output_srid integer DEFAULT 4326
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
			USING 'bh3_pressure_map', error_curs_rec.gid, error_curs_rec.exc_text, error_curs_rec.exc_detail, error_curs_rec.exc_hint;
			error_count := error_count + 1;
		END LOOP;

		IF error_count = 0 THEN
			EXECUTE format('DROP TABLE IF EXISTS %1$I.error_log', output_schema);
		END IF;

		RAISE INFO 'Completed in %', (clock_timestamp() - start_time);
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

COMMENT ON PROCEDURE public.bh3_procedure
    IS 'Purpose:
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
