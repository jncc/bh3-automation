-- [Re-]installs BH3 types, procedures and functions in a database.





DROP TYPE IF EXISTS public.sensitivity_source;

CREATE TYPE public.sensitivity_source AS ENUM
    ('broadscale_habitats', 'eco_groups', 'rock', 'rock_eco_groups', 'eco_groups_rock');

ALTER TYPE public.sensitivity_source
    OWNER TO bh3;





DROP PROCEDURE IF EXISTS public.bh3_drop_spatial_tables(name[]);

CREATE OR REPLACE PROCEDURE public.bh3_drop_spatial_tables(
	spatial_tables name[])
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	table_count integer;
	stn name[];
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;

BEGIN
	IF spatial_tables IS NOT NULL AND array_length(spatial_tables, 1) > 0 THEN
		BEGIN
			FOREACH stn SLICE 1 IN ARRAY spatial_tables LOOP
				IF array_length(stn, 1) = 2 AND length(coalesce(stn[2], '')) > 0 THEN
					IF length(coalesce(stn[1], '')) > 0 THEN
						EXECUTE format('SELECT count(*) AS N '
									   'FROM pg_class c '
										   'JOIN pg_namespace n ON c.relnamespace = n.oid '
									   'WHERE n.nspname = $1 AND c.relname = $2') 
						INTO table_count 
						USING stn[1], stn[2];

						IF table_count = 1 THEN
							EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING stn[1], stn[2];
						END IF;
					ELSE
						EXECUTE format('SELECT count(*) AS N '
									   'FROM pg_class c '
										   'JOIN pg_namespace n ON c.relnamespace = n.oid '
									   'WHERE c.relname = $1') 
						INTO table_count 
						USING stn[2];

						IF table_count = 1 THEN
							EXECUTE 'SELECT DropGeometryTable($1::text)' USING stn[2];
						END IF;
					END IF;
				END IF;
			END LOOP;
		EXCEPTION
			WHEN others THEN
				GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
										  exc_detail = PG_EXCEPTION_DETAIL,
										  exc_hint = PG_EXCEPTION_HINT;
				RAISE INFO 'bh3_drop_spatial_table: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
		END;
	END IF;
END;
$BODY$;

COMMENT ON PROCEDURE public.bh3_drop_spatial_tables
    IS 'Purpose:
Drops a spatial table and its PostGIS metadata. 

Approach:
Calls the DropGeometryTable function to drop a spatial table along with  its PostGIS metadata.

Parameters:
spatial_tables		name[]			Array of arrays of length two (table schema and table name of table to be dropped).

Calls:
No nested calls';





DROP PROCEDURE IF EXISTS public.bh3_drop_temp_table(name);

CREATE OR REPLACE PROCEDURE public.bh3_drop_temp_table(
	table_name name)
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	temp_table name;

BEGIN
	IF length(coalesce(table_name, '')) > 0 THEN
		EXECUTE format('DROP TABLE IF EXISTS %1$I', table_name);

		EXECUTE format('SELECT quote_ident(n.nspname) || ''.'' || quote_ident(c.relname) '
					   'FROM pg_class c '
						   'JOIN pg_namespace n ON c.relnamespace = n.oid '
					   'WHERE n.nspname ~* $1 AND c.relname = $2'
		)
		INTO temp_table
		USING 'pg_temp', table_name;

		IF length(coalesce(temp_table,'')) > 0 THEN
			EXECUTE 'DROP TABLE ' || temp_table;
		END IF;
	END IF;
END;
$BODY$;

COMMENT ON PROCEDURE public.bh3_drop_temp_table
    IS 'Purpose:
Drops a temporary table if it exists.

Approach:
Looks up the temporary table''s schema in database metadata and drop it using a qualified name.

Parameters:
table_name		name	Name of temporary table to be dropped.

Calls:
No nested calls.';





DROP PROCEDURE IF EXISTS public.bh3_index(name, name, character varying[]);

CREATE OR REPLACE PROCEDURE public.bh3_index(
	schema_name name,
	table_name name,
	column_names_types character varying[])
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	sqlstmt text;
	index_name text;
	index_data record;
	index_count integer;
	cnt character varying[];
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;

BEGIN
	IF length(coalesce(table_name, '')) > 0 AND column_names_types IS NOT NULL AND array_ndims(column_names_types) = 2 THEN
		IF length(coalesce(schema_name, '')) > 0 THEN
			BEGIN
				sqlstmt := format('SELECT count(*) AS N ' 
								  'FROM pg_index i '
									  'JOIN pg_class ci ON i.indexrelid = ci.oid '
									  'JOIN pg_class c ON i.indrelid = c.oid '
									  'JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey) '
									  'JOIN pg_namespace n ON c.relnamespace = n.oid '
								  'WHERE i.indnatts = 1 ' 
									  'AND n.nspname = $1 '
									  'AND c.relname = $2 '
									  'AND a.attname = $3');

				FOREACH cnt SLICE 1 IN ARRAY column_names_types LOOP
					IF array_length(cnt, 1) = 2 THEN
						CASE cnt[2]
							WHEN 's' THEN 
								index_name := format('sidx_%1$s_%2$s', table_name, cnt[1]);
								EXECUTE sqlstmt INTO index_count USING schema_name, table_name, cnt[1];
								IF index_count = 0 THEN
									EXECUTE format('CREATE INDEX %1$I ON %2$I.%3$I USING GIST(%4$I)', index_name, schema_name, table_name, cnt[1]);
								END IF;
							WHEN 'u' THEN 
								index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
								EXECUTE sqlstmt INTO index_count USING schema_name, table_name, cnt[1];
								IF index_count = 0 THEN
									EXECUTE format('CREATE UNIQUE INDEX %1$I ON %2$I.%3$I USING BTREE(%4$I)', index_name, schema_name, table_name, cnt[1]);
								END IF;
							ELSE
								index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
								EXECUTE sqlstmt INTO index_count USING schema_name, table_name, cnt[1];
								IF index_count = 0 THEN
									EXECUTE format('CREATE INDEX %1$I ON %2$I.%3$I USING BTREE(%4$I)', index_name, schema_name, table_name, cnt[1]);
								END IF;
						END CASE;
					END IF;
				END LOOP;

				EXECUTE format('ANALYZE %1$I.%2$I', schema_name, table_name);
			EXCEPTION
				WHEN others THEN
					GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
											  exc_detail = PG_EXCEPTION_DETAIL,
											  exc_hint = PG_EXCEPTION_HINT;
					RAISE INFO 'bh3_index: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
			END;
		ELSE
			BEGIN
				sqlstmt := format('SELECT count(*) AS N ' 
								  'FROM pg_index i '
									  'JOIN pg_class ci ON i.indexrelid = ci.oid '
									  'JOIN pg_class c ON i.indrelid = c.oid '
									  'JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey) '
								  'WHERE i.indnatts = 1 ' 
									  'AND c.relname = $1 '
									  'AND a.attname = $2');

				FOREACH cnt SLICE 1 IN ARRAY column_names_types LOOP
					IF array_length(cnt, 1) = 2 THEN
						CASE cnt[2]
								WHEN 's' THEN 
									index_name := format('sidx_%1$s_%2$s', table_name, cnt[1]);
									EXECUTE sqlstmt INTO index_count USING table_name, cnt[1];
									IF index_count = 0 THEN
										EXECUTE format('CREATE INDEX %1$I ON %2$I USING GIST(%3$I)', index_name, table_name, cnt[1]);
									END IF;
								WHEN 'u' THEN 
									index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
									EXECUTE sqlstmt INTO index_count USING table_name, cnt[1];
									IF index_count = 0 THEN
										EXECUTE format('CREATE UNIQUE INDEX %1$I ON %2$I USING BTREE(%3$I)', index_name, table_name, cnt[1]);
									END IF;
								ELSE
									index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
									EXECUTE sqlstmt INTO index_count USING table_name, cnt[1];
									IF index_count = 0 THEN
										EXECUTE format('CREATE INDEX %1$I ON %2$I USING BTREE(%3$I)', index_name, table_name, cnt[1]);
									END IF;
						END CASE;
					END IF;
				END LOOP;

				EXECUTE format('ANALYZE %1$I', table_name);
			EXCEPTION
				WHEN others THEN
					GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
											  exc_detail = PG_EXCEPTION_DETAIL,
											  exc_hint = PG_EXCEPTION_HINT;
					RAISE INFO 'bh3_index: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
			END;
		END IF;
	END IF;
END;
$BODY$;

COMMENT ON PROCEDURE public.bh3_index
    IS 'Purpose:
Indexes one or more columns of a table and re-computes its statistics. 

Approach:
Creates index(es) with name(s) composed of table and column names if they do not already exist.
Calls analyze to re-compute table statistics. 

Parameters:
schema_name			name					Schema name of table to be indexed.
table_name			name					Table name of table to be indexed.
column_names_types	character varying[][]	Two dimensional array of columns to be indexed. Each column is represented by an array of length two. 
			  								The first element is the column name; the second is the index type. 
			  								Valid types are, where ''s'' for spatial (GIST) and ''u'' for unique (BTREE).
			  								Any other value will be interpreted as requesting a non-unique BTREE index.

Calls:
No nested calls';





DROP PROCEDURE IF EXISTS public.bh3_procedure(integer[], character varying[], integer, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, integer, boolean, boolean, boolean, integer);

CREATE OR REPLACE PROCEDURE public.bh3_procedure(
	boundary_filter integer[],
	habitat_types_filter character varying[],
	start_year integer,
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
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
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
			,start_year
			,end_year
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
			,start_year
			,end_year
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
				,start_year
				,end_year
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
start_year							integer							Earliest year for Marine Recorder spcies samples to be included.
species_sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
pressure_schema						name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
output_schema						name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
output_owner						character varying				Role that will own output schema and tables. Defaults to NULL, which means the user running the procedure.
boundary_schema						name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table						name							Name of table containing AOI boundary polygons. Defaults to ''unofficial_country_waters_simplified_wgs84''.
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
end_year							integer							Latest year for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
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





DROP PROCEDURE IF EXISTS public.bh3_repair_geometries(name, name, name);

CREATE OR REPLACE PROCEDURE public.bh3_repair_geometries(
	schema_name name,
	table_name name,
	geom_column name DEFAULT 'the_geom'::name)
LANGUAGE 'plpgsql'

AS $BODY$
BEGIN
	IF length(coalesce(table_name, '')) > 0 AND length(coalesce(geom_column, '')) > 0 THEN
		IF length(coalesce(schema_name, '')) > 0 THEN
			EXECUTE format('UPDATE %1$I.%2$I '
						   'SET %3$I = ST_MakeValid(ST_Multi(ST_Buffer('
							   'CASE '
								   'WHEN ST_IsCollection(%3$I) THEN ST_CollectionExtract(ST_MakeValid(%3$I),3) '
								   'ELSE ST_MakeValid(%3$I) '
							   'END, 0))) '
						   'WHERE NOT ST_IsValid(%3$I) OR NOT ST_IsSimple(%3$I) OR ST_IsCollection(%3$I)',
						   schema_name, table_name, geom_column);
		ELSE
			EXECUTE format('UPDATE %1$I '
						   'SET %2$I = ST_MakeValid(ST_Multi(ST_Buffer('
							   'CASE '
								   'WHEN ST_IsCollection(%2$I) THEN ST_CollectionExtract(ST_MakeValid(%2$I),3) '
								   'ELSE ST_MakeValid(%2$I) '
							   'END, 0))) '
						   'WHERE NOT ST_IsValid(%2$I) OR NOT ST_IsSimple(%2$I) OR ST_IsCollection(%2$I)',
						   table_name, geom_column);
		END IF;
	END IF;
END;
$BODY$;

COMMENT ON PROCEDURE public.bh3_repair_geometries
    IS 'Purpose:
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





DROP FUNCTION IF EXISTS public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_boundary_subdivide(
	boundary_filter integer[],
	output_schema name,
	output_table name DEFAULT 'boundary'::name,
	output_table_subdivide name DEFAULT 'boundary_subdivide'::name,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'unofficial_country_waters_simplified_wgs84'::name,
	boundary_filter_negate boolean DEFAULT false,
	output_srid integer DEFAULT 4326,
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
	rows_affected bigint;
	tn name;
	srid_bnd integer;
	geom_exp_bnd character varying;
	gid_condition text;
	negation text;

BEGIN
	BEGIN
		success := false;

		start_time := clock_timestamp();

		/* clean up any previous output left behind */
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table]::name[]
				,ARRAY[output_schema, output_table_subdivide]::name[]
			]::name[][]);

		srid_bnd := bh3_find_srid(boundary_schema, boundary_table, 'the_geom'::name);
		IF srid_bnd != 4326 THEN
			geom_exp_bnd := format('ST_Transform(the_geom,%s)', 4326);
		ELSE
			geom_exp_bnd := 'the_geom';
		END IF;

		IF boundary_filter_negate THEN
			negation = 'NOT';
		ELSE
			negation = '';
		END IF;

		IF array_length(boundary_filter, 1) = 1 THEN
			EXECUTE format('CREATE TABLE %1$I.%2$I AS '
						   'WITH cte_subdiv AS '
						   '('
							   'SELECT ST_Subdivide(%3$s) AS the_geom '
							   'FROM %4$I.%5$I '
							   'WHERE %6$s gid = $1'
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',the_geom '
						   'FROM cte_subdiv',
						   output_schema, output_table_subdivide, 
						   geom_exp_bnd, boundary_schema, boundary_table, 
						   negation)
			USING boundary_filter[1];
		ELSIF array_length(boundary_filter, 1) > 1 THEN
			EXECUTE format('CREATE TABLE %1$I.%2$I AS '
						   'WITH cte_subdiv AS '
						   '('
							   'SELECT ST_Subdivide(%3$s) AS the_geom '
							   'FROM %4$I.%5$I '
							   'WHERE %6$s gid = ANY($1)'
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',the_geom '
						   'FROM cte_subdiv',
						   output_schema, output_table_subdivide, 
						   geom_exp_bnd, boundary_schema, boundary_table, 
						   negation)
			USING boundary_filter;
		ELSE
			EXECUTE format('CREATE TABLE %1$I.%2$I AS '
						   'WITH cte_subdiv AS '
						   '('
							   'SELECT ST_Subdivide(%3$s) AS the_geom '
							   'FROM %4$I.%5$I'
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',the_geom '
						   'FROM cte_subdiv',
						   output_schema, output_table_subdivide, 
						   geom_exp_bnd, boundary_schema, boundary_table);
		END IF;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Inserted % rows into table %.%: %', 
			rows_affected, output_schema, output_table_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(output_schema, output_table_subdivide);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Repaired % geometries in table %.%: %', 
			rows_affected, output_schema, output_table_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('ALTER TABLE %1$I.%2$I ADD CONSTRAINT %2$s_pkey PRIMARY KEY(gid)', 
					    output_schema, output_table_subdivide);
		CALL bh3_index(output_schema, output_table_subdivide, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

		RAISE INFO 'bh3_boundary_subdivide: Indexed table %.%: %', 
			output_schema, output_table_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format(
			'CREATE TABLE %1$I.%2$I AS '
			'SELECT ST_Extent(the_geom) AS bbox'
				',bh3_safe_union(the_geom) AS the_geom '
			'FROM %1$I.%3$I',
			output_schema, output_table, output_table_subdivide);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Inserted % rows into table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(output_schema, output_table);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Repaired % geometries in table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_boundary_subdivide: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer)
    IS 'Purpose:
Selects AOI polygons from boundary_schema.boundary_table identified by boundary_filter, splits them into smaller parts with no more than 256 vertices each 
and unions them into a single AOI polygon and its bounding box.

Approach:
Calls the ST_Subdivide function to split the AOI polygons identified by boundary_filter into smaller parts with no more than 256 vertices each.
These polygons are stored in table output_schema.output_table_subdivide and then unioned into a single AOI polygon and its bounding box stored in
table output_schema.output_table.

Parameters:
boundary_filter			integer[]	Array of primary key values (gid) of AOI polygons in boundary_table to be included (or excluded if boundary_filter_negate is true).
output_schema			name		Schema in which output tables are created.
output_table			name		Name of table containing single, unioned AOI polygon and bounding box. Defaults to ''boundary''.
output_table_subdivide	name		Name of table containing split AOI polygons. Defaults to ''boundary_subdivide''.
boundary_schema			name		Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table			name		Name of table containing AOI boundary polygons. Defaults to ''unofficial_country_waters_simplified_wgs84''.
boundary_filter_negate	boolean		Defaults to false.
output_srid				integer		SRID of output tables. Defaults to 4326.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_find_srid
bh3_safe_union';





DROP FUNCTION IF EXISTS public.bh3_create_csquares(name, name, boolean, numeric, integer);

CREATE OR REPLACE FUNCTION public.bh3_create_csquares(
	boundary_schema name,
	boundary_table name DEFAULT 'boundary'::name,
	boundary_clip boolean DEFAULT false,
	cell_size_degrees numeric DEFAULT 0.05,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(gid bigint, "row" integer, col integer, the_geom geometry) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	exc_text text;
	exc_detail text;
	exc_hint text;
	geom_exp_grid text;

BEGIN
	BEGIN
		IF output_srid != 4326 THEN
			IF boundary_clip THEN
				geom_exp_grid := format('ST_Transform(ST_ClipByBox2D(bnd.the_geom,(grid_row).geom),%s)', output_srid);
			ELSE
				geom_exp_grid := format('ST_Transform((grid_row).geom,%s)', output_srid);
			END IF;
		ELSE 
			IF boundary_clip THEN
				geom_exp_grid := 'ST_ClipByBox2D(bnd.the_geom,(grid_row).geom)';
			ELSE
				geom_exp_grid := '(grid_row).geom';
			END IF;
		END IF;

		RETURN QUERY EXECUTE format(
			'WITH cte_bbox_coords AS '
			'('
				'SELECT ST_XMin(bbox) AS xmin'
					',ST_YMin(bbox) AS ymin'
					',ST_XMax(bbox) AS xmax'
					',ST_YMax(bbox) AS ymax '
				'FROM %1$I.%2$I'
			'),'
			'cte_bbox_meas AS '
			'('
				'SELECT (xmin - xmin::numeric %% %3$s - CASE WHEN xmin < 0 THEN 0.05 else 0 END)::double precision AS xmin'
					',(ymin - ymin::numeric %% %3$s - CASE WHEN ymin < 0 THEN 0.05 else 0 END)::double precision AS ymin'
					',(xmax - xmax::numeric %% %3$s) + CASE WHEN xmax < 0 THEN 0 else 0.05 END AS xmax'
					',(ymax - ymax::numeric %% %3$s) + CASE WHEN ymax < 0 THEN 0 else 0.05 END AS ymax '
				'FROM cte_bbox_coords'
			'),'
			'cte_grid AS '
			'('
				'SELECT bh3_create_fishnet('
					'((ymax - ymin) / %3$s)::integer'
					',((xmax - xmin) / %3$s)::integer'
					',%3$s'
					',%3$s'
					',xmin'
					',ymin'
					',4326) AS grid_row '
				'FROM cte_bbox_meas'
			') '
			'SELECT ROW_NUMBER() OVER() AS gid' 
				',(grid_row).row'
				',(grid_row).col' 
				',%4$s AS the_geom '
			'FROM cte_grid g '
				'JOIN %1$I.%2$I bnd ON ST_Intersects((grid_row).geom,bnd.the_geom)',
			boundary_schema, boundary_table, cell_size_degrees, geom_exp_grid);
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_create_csquares: Exception. Message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_create_csquares(name, name, boolean, numeric, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_create_csquares(name, name, boolean, numeric, integer)
    IS 'Purpose:
Creates an in-memory table of c-squares of the specified cell size within the specified polygon boundary.
The output geometries may be clipped by the boundary polygon.

Approach:
Calls the bh3_create_fishnet function to create a grid within the bounding box of the unioned boundary geometries
and returns the grid cells intersecting the boundary geometries, optionally clipping them by the boundary.

Parameters:
boundary_schema			name		Schema of table containing single AOI boundary polygon and bounding box.
boundary_table			name		Name of table containing single AOI boundary polygon and bounding box. Defaults to ''boundary''.
boundary_clip			boolean		If true grid will be clipped by boundary polygon. Defaults to false.
cell_size_degrees		numeric		Cell size in degrees. Defaults to 0.05.
output_srid				integer		SRID of output table. Defaults to 4326.

Returns:
An in-memory table of c-squares of the specified cell size within the specified polygon boundary.

Calls:
bh3_find_srid
bh3_create_fishnet';





DROP FUNCTION IF EXISTS public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer);

CREATE OR REPLACE FUNCTION public.bh3_create_fishnet(
	num_rows integer,
	num_cols integer,
	size_x double precision,
	size_y double precision,
	origin_x double precision DEFAULT 0,
	origin_y double precision DEFAULT 0,
	srid integer DEFAULT 4326)
    RETURNS TABLE("row" integer, col integer, geom geometry) 
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE STRICT 
    ROWS 1000
AS $BODY$
BEGIN
	RETURN QUERY
	SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * size_x + origin_x, i * size_y + origin_y) AS geom
	FROM generate_series(0, num_rows - 1) AS i,
		 generate_series(0, num_cols - 1) AS j,
	(
		SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(0,0),ST_Point(size_x,size_y)),srid) AS cell
	) AS foo;
END;
$BODY$;

ALTER FUNCTION public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer)
    IS 'Purpose:
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





DROP FUNCTION IF EXISTS public.bh3_disturbance_map(name, name, name, name, integer, integer, name, name, name, name, name, name, integer);

CREATE OR REPLACE FUNCTION public.bh3_disturbance_map(
	boundary_schema name,
	pressure_schema name,
	sensitivity_map_schema name,
	output_schema name,
	start_year integer,
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
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
				USING boundary_schema, pressure_schema, start_year, end_year, boundary_table, 
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

ALTER FUNCTION public.bh3_disturbance_map(name, name, name, name, integer, integer, name, name, name, name, name, name, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_disturbance_map(name, name, name, name, integer, integer, name, name, name, name, name, name, integer)
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
start_year				integer							Earliest year for Marine Recorder species samples to be included.
end_year				integer							Latest year for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
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





DROP FUNCTION IF EXISTS public.bh3_entry(integer[], character varying[], integer, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, integer, boolean, boolean, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_entry(
	boundary_filter integer[],
	habitat_types_filter character varying[],
	start_year integer,
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
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
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
			,start_year
			,end_year
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
			,start_year
			,end_year
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
				,start_year
				,end_year
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

ALTER FUNCTION public.bh3_entry(integer[], character varying[], integer, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, integer, boolean, boolean, boolean, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_entry(integer[], character varying[], integer, sensitivity_source, name, name, character varying, name, name, name, name, name, name, name, name, name, name, name, name, name, name, name, integer, boolean, boolean, boolean, integer)
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
start_year							integer							Earliest year for Marine Recorder spcies samples to be included.
species_sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
pressure_schema						name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
output_schema						name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
output_owner						character varying				Role that will own output schema and tables. Defaults to NULL, which means the user running the procedure.
boundary_schema						name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table						name							Name of table containing AOI boundary polygons. Defaults to ''unofficial_country_waters_simplified_wgs84''.
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
end_year							integer							Latest year for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
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





DROP FUNCTION IF EXISTS public.bh3_find_srid(name, name, name);

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

ALTER FUNCTION public.bh3_find_srid(name, name, name)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_find_srid(name, name, name)
    IS 'Purpose:
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





DROP FUNCTION IF EXISTS public.bh3_get_pressure_csquares(name, name, integer, integer, name, name, name, integer);

CREATE OR REPLACE FUNCTION public.bh3_get_pressure_csquares(
	boundary_schema name,
	pressure_schema name,
	start_year integer,
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
	boundary_table name DEFAULT 'boundary'::name,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(gid bigint, c_square character varying, n bigint, sar_surface_min double precision, sar_surface_max double precision, sar_surface_avg double precision, sar_surface_cat_min integer, sar_surface_cat_max integer, sar_surface_cat_range integer, sar_surface_variable boolean, sar_surface double precision, sar_surface_cat_comb integer, sar_subsurface_min double precision, sar_subsurface_max double precision, sar_subsurface_avg double precision, sar_subsurface_cat_min integer, sar_subsurface_cat_max integer, sar_subsurface_cat_range integer, sar_subsurface_variable boolean, sar_subsurface double precision, sar_subsurface_cat_comb integer, the_geom geometry) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	exc_text text;
	exc_detail text;
	exc_hint text;
	negation character varying;
	srid int;
	geom_fld name;
	geom_exp text;
	rel_cursor refcursor;
	rel_cursor_row record;
	sqlstmt text;

BEGIN
	BEGIN
		geom_fld := 'the_geom'::name;
		sqlstmt := '';

		OPEN rel_cursor FOR EXECUTE format(
			'SELECT c.relname '
			'FROM pg_class c '
				'JOIN pg_namespace n ON c.relnamespace = n.oid '
				'JOIN pg_attribute a ON c.oid = a.attrelid '
			'WHERE (c.relkind = ''r'' OR c.relkind = ''v'') '
				'AND a.attname IN ($1,$2,$3,$4,$5) '
				'AND n.nspname = $6 '
			'GROUP BY c.relname '
			'HAVING count(*) = 5 '
			'ORDER BY c.relname')
		USING 'c_square', 'year', sar_surface_column, sar_subsurface_column, 'the_geom', pressure_schema;

  		LOOP
  			FETCH rel_cursor INTO rel_cursor_row;
  			EXIT WHEN NOT FOUND;

			srid := bh3_find_srid(pressure_schema, rel_cursor_row.relname);
			IF srid != output_srid AND srid != 0 AND output_srid != 0 THEN
				geom_exp := format('ST_Transform(prs.%1$I,%2$s)', geom_fld, output_srid);
			ELSE
				geom_exp := format('prs.%1$I', geom_fld);
			END IF;

			sqlstmt := sqlstmt || format('UNION '
										 'SELECT %1$I'
											 ',%2$I'
											 ',%3$I AS sar_surface'
											 ',CASE '
												 'WHEN %3$I > 0.00 AND %3$I <= 0.33 THEN 1 '
												 'WHEN %3$I > 0.33 AND %3$I <= 0.66 THEN 2 '
												 'WHEN %3$I > 0.66 AND %3$I <= 1.00 THEN 3 '
												 'WHEN %3$I > 1.00 AND %3$I <= 3.00 THEN 4 '
												 'WHEN %3$I > 3.00 THEN 5 '
												 'ELSE 0 '
											 'END AS sar_surface_cat'
											 ',%4$I AS sar_subsurface'
											 ',CASE '
												 'WHEN %4$I > 0.00 AND %4$I <= 0.33 THEN 1 '
												 'WHEN %4$I > 0.33 AND %4$I <= 0.66 THEN 2 '
												 'WHEN %4$I > 0.66 AND %4$I <= 1.00 THEN 3 '
												 'WHEN %4$I > 1.00 AND %4$I <= 3.00 THEN 4 '
												 'WHEN %4$I > 3.00 THEN 5 '
												 'ELSE 0 '
											 'END AS sar_subsurface_cat'
											 ',%5$s AS the_geom '
										 'FROM %6$I.%7$I prs '
											 'JOIN %8$I.%9$I bnd ON ST_Intersects(%5$s,bnd.%10$I) '
										 'WHERE %2$I >= $1 AND %2$I <= $2', 
										 'c_square', 'year', sar_surface_column, sar_subsurface_column, geom_exp, 
										 pressure_schema, rel_cursor_row.relname, boundary_schema, boundary_table,
										 geom_fld);
  		END LOOP;

		CLOSE rel_cursor;

		IF length(sqlstmt) > 0 THEN
			sqlstmt := format('WITH cte_union AS '
							  '(' 
								  '%1$s'
							  '),'
							  'cte_agg AS '
							  '('
								  'SELECT c_square'
									  ',count(*) AS N'
									  ',min(sar_surface) AS sar_surface_min'
									  ',max(sar_surface) AS sar_surface_max'
									  ',avg(sar_surface) AS sar_surface_avg'
									  ',min(sar_surface_cat) AS sar_surface_cat_min'
									  ',max(sar_surface_cat) AS sar_surface_cat_max'
									  ',max(sar_surface_cat) - min(sar_surface_cat) AS sar_surface_cat_range'
									  ',(max(sar_surface_cat) - min(sar_surface_cat) >= 4) AS sar_surface_variable'
									  ',CASE '
										  'WHEN max(sar_surface_cat) - min(sar_surface_cat) >= 4 THEN max(sar_surface) '
										  'ELSE avg(sar_surface) '
									  'END AS sar_surface'
									  ',min(sar_subsurface) AS sar_subsurface_min'
									  ',max(sar_subsurface) AS sar_subsurface_max'
									  ',avg(sar_subsurface) AS sar_subsurface_avg'
									  ',min(sar_subsurface_cat) AS sar_subsurface_cat_min'
									  ',max(sar_subsurface_cat) AS sar_subsurface_cat_max'
									  ',max(sar_subsurface_cat) - min(sar_subsurface_cat) AS sar_subsurface_cat_range'
									  ',(max(sar_subsurface_cat) - min(sar_subsurface_cat) >= 4) AS sar_subsurface_variable'
									  ',CASE '
										  'WHEN max(sar_subsurface_cat) - min(sar_subsurface_cat) >= 4 THEN max(sar_subsurface) '
										  'ELSE avg(sar_subsurface) '
									  'END AS sar_subsurface'
									  ',(ST_Accum(the_geom))[1] AS the_geom '
								  'FROM cte_union u '
								  'GROUP BY c_square'
							  ') '
							  'SELECT ROW_NUMBER() OVER() AS gid'
								  ',c_square'
								  ',N'
								  ',sar_surface_min'
								  ',sar_surface_max'
								  ',sar_surface_avg'
								  ',sar_surface_cat_min'
								  ',sar_surface_cat_max'
								  ',sar_surface_cat_range'
								  ',sar_surface_variable'
								  ',sar_surface'
								  ',CASE '
									  'WHEN sar_surface > 0.00 AND sar_surface <= 0.33 THEN 1 '
									  'WHEN sar_surface > 0.33 AND sar_surface <= 0.66 THEN 2 '
									  'WHEN sar_surface > 0.66 AND sar_surface <= 1.00 THEN 3 '
									  'WHEN sar_surface > 1.00 AND sar_surface <= 3.00 THEN 4 '
									  'WHEN sar_surface > 3.00 THEN 5 '
									  'ELSE 0 '
								  'END AS sar_surface_cat_comb'
								  ',sar_subsurface_min'
								  ',sar_subsurface_max'
								  ',sar_subsurface_avg'
								  ',sar_subsurface_cat_min'
								  ',sar_subsurface_cat_max'
								  ',sar_subsurface_cat_range'
								  ',sar_subsurface_variable'
								  ',sar_subsurface'
								  ',CASE '
									  'WHEN sar_subsurface > 0.00 AND sar_subsurface <= 0.33 THEN 1 '
									  'WHEN sar_subsurface > 0.33 AND sar_subsurface <= 0.66 THEN 2 '
									  'WHEN sar_subsurface > 0.66 AND sar_subsurface <= 1.00 THEN 3 '
									  'WHEN sar_subsurface > 1.00 AND sar_subsurface <= 3.00 THEN 4 '
									  'WHEN sar_subsurface > 3.00 THEN 5 '
									  'ELSE 0 '
								  'END AS sar_subsurface_cat_comb'
								  ',the_geom '
							  'FROM cte_agg',
							  substring(sqlstmt FROM 7));
		
			RAISE INFO 'bh3_get_pressure_csquares: sqlstmt: %', sqlstmt;

			RETURN QUERY EXECUTE sqlstmt USING start_year, end_year;
		END IF;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_get_pressure_csquares: Exception. Message: %. Detail: %. Hint: %', 
										exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_get_pressure_csquares(name, name, integer, integer, name, name, name, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_get_pressure_csquares(name, name, integer, integer, name, name, name, integer)
    IS 'Purpose:
Creates an in-memory table of categorised pressure c-squares from the tables in the specified pressure_schema. 
All tables in pressure_schema that have the required columns will be included.

Approach:
Computes summary values of disturbance scores for each table in pressure_schema that has the columns 
''c_square'', ''year'', sar_surface_column, sar_subsurface_column and ''the_geom'', aggregating by c_square,
categorises the scores into sar_surface and sar_subsurface scores between one and four and returns the union
of the resulting row sets.

Paramerters:
boundary_schema			name							Schema of table containing single AOI boundary polygon and bounding box.
pressure_schema			name							Schema in which pressure source tables are located (all tables in this schema that have the required columns will be used).
start_year				integer							Earliest year for Marine Recorder spcies samples to be included.
date_end				integer							Latest year for Marine Recorder species samples and pressure data to be included. Defaults to current date and time. Defaults to current date and time.
boundary_table			name							Name of table containing single AOI boundary polygon and bounding box. Defaults to ''boundary''.
sar_surface_column		name							SAR surface column name in pressure source tables. Defaults to ''sar_surface''.
sar_subsurface_column	name							SAR sub-surface column name in pressure source tables. Defaults to ''sar_subsurface''.
output_srid				integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
An in-memory table of categorised pressure c-squares from the tables in the specified pressure_schema.

Calls:
bh3_find_srid';





DROP FUNCTION IF EXISTS public.bh3_get_pressure_csquares_size(name, integer, integer, name, name, integer);

CREATE OR REPLACE FUNCTION public.bh3_get_pressure_csquares_size(
	pressure_schema name,
	start_year integer,
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
	sar_surface_column name DEFAULT 'sar_surface'::name,
	sar_subsurface_column name DEFAULT 'sar_subsurface'::name,
	output_srid integer DEFAULT 4326)
    RETURNS numeric
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
	exc_text text;
	exc_detail text;
	exc_hint text;
	geom_fld character varying;
	srid int;
	geom_exp text;
	rel_cursor refcursor;
	rel_cursor_row record;
	sqlstmt text;
	cell_size double precision;

BEGIN
	cell_size := -1;

	BEGIN
		geom_fld := 'the_geom';
		sqlstmt := '';

		OPEN rel_cursor FOR EXECUTE format(
			'SELECT c.relname '
			'FROM pg_class c '
				'JOIN pg_namespace n ON c.relnamespace = n.oid '
				'JOIN pg_attribute a ON c.oid = a.attrelid '
			'WHERE c.relkind = ''r'' '
				'AND a.attname IN ($1,$2,$3,$4,$5) '
				'AND n.nspname = $6 '
			'GROUP BY c.relname '
			'HAVING count(*) = 5 '
			'ORDER BY c.relname')
		USING 'c_square', 'year', sar_surface_column, sar_subsurface_column, geom_fld, pressure_schema;

  		LOOP
  			FETCH rel_cursor INTO rel_cursor_row;
  			EXIT WHEN NOT FOUND;

			srid := bh3_find_srid(pressure_schema, rel_cursor_row.relname);
			IF srid != output_srid AND srid != 0 AND output_srid != 0 THEN
				geom_exp := format('ST_Transform(%1$I,%2$s)', geom_fld, output_srid);
			ELSE
				geom_exp := format('%1$I', geom_fld);
			END IF;

			sqlstmt := sqlstmt || format('UNION '
										 'SELECT %1$s AS %2$I '
										 'FROM %3$I.%4$I '
										 'WHERE %5$I >= $1 '
											 'AND %5$I <= $2 ', 
										 geom_exp, geom_fld, pressure_schema, rel_cursor_row.relname, 'year');
  		END LOOP;

		CLOSE rel_cursor;

		IF length(sqlstmt) > 6 THEN
			sqlstmt := format('WITH cte_union AS '
							  '(' 
								  '%1$s'
							  '),'
							  'cte_comp AS '
							  '('
								  'SELECT ST_XMax(%2$I) - ST_XMin(%2$I) AS width'
									  ',ST_YMax(%2$I) - ST_YMin(%2$I) AS height '
								  'FROM cte_union'
							  '),'
							  'cte_agg AS '
							  '('
								  'SELECT round(avg(width)::numeric,8) AS width'
									  ',round(avg(height)::numeric,8) AS height'
									  ',stddev(width) AS width_stdev'
									  ',stddev(height) AS height_stdev '
								  'FROM cte_comp '
								  'HAVING stddev(width) < 0.00000001 '
									  'AND stddev(height) < 0.00000001'
							  ') '
							  'SELECT height AS cell_size '
							  'FROM cte_agg '
							  'WHERE width = height '
								  'AND width_stdev < 0.00000001 '
								  'AND height_stdev < 0.00000001',
							  substring(sqlstmt FROM 7), geom_fld);
		
			RAISE INFO 'bh3_get_pressure_csquares_size: sqlstmt: %', sqlstmt;

			EXECUTE sqlstmt INTO cell_size USING start_year, end_year;
		END IF;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_get_pressure_csquares_size: Exception. Message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
		
	RETURN cell_size;
END;
$BODY$;

ALTER FUNCTION public.bh3_get_pressure_csquares_size(name, integer, integer, name, name, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_get_pressure_csquares_size(name, integer, integer, name, name, integer)
    IS 'Purpose:
Obtains the size of c-squares from the geometries of the tables in the selected pressure schema.

Approach:
A union query averages the width and height of [multi-]polygon geometries in all tables in the selected pressure schema.
The query is the same as used in bh3_get_pressure_csquares except for the boundary filter, which is unnecessary and skipped for performance reasons.
The polygons are expected to be squares of equal size. The average width/height is returned as long as their standard deviation is less
than 0.00000001.

Parameters:
pressure_schema			name							Name of the schema that holds the pressure tables.
start_year				integer							Earliest year for squares to be included.
end_year				integer							Latest year for squares to be included. Defaults to current date and time.
sar_surface_column		name							Name of the surface SAR column. Defaults to ''sar_surface''.
sar_subsurface_column	name							Name of the sub-surface SAR column. Defaults to ''sar_subsurface''.
output_srid				integer 						SRID of spatial reference system in which c-squares are to be measured. Defaults to 4326.

Returns:
Cell size in the units of the spatial reference system identified by output_srid (normally degrees).

Calls:
bh3_find_srid';





DROP FUNCTION IF EXISTS public.bh3_habitat_boundary_clip(character varying[], name, name, name, name, name, name, name, name, boolean, boolean, boolean);

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
		IF array_length(habitat_types_filter, 1) = 1 THEN
			IF habitat_types_filter_negate THEN
				habitat_type_condition := format('hab.%1$I != %2$L', 'eunis_l3', habitat_types_filter[1]);
			ELSE
				habitat_type_condition := format('hab.%1$I = %2$L', 'eunis_l3', habitat_types_filter[1]);
			END IF;
		ELSIF array_length(habitat_types_filter, 1) > 1 THEN
			IF habitat_types_filter_negate THEN
				habitat_type_condition := format('NOT hab.%1$I = ANY ($1)', 'eunis_l3');
			ELSE
				habitat_type_condition := format('hab.%1$I = ANY ($1)', 'eunis_l3');
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
    OWNER TO bh3;

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
boundary_subdivide_table			name					Name of the subdivided boundary table defining the AOI. Defaults to ''boundary_subdivide''.
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





DROP FUNCTION IF EXISTS public.bh3_habitat_grid(name, name, name, name, name, name, numeric);

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
    OWNER TO bh3;

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





DROP FUNCTION IF EXISTS public.bh3_habitat_remove_overlaps(name, name);

CREATE OR REPLACE FUNCTION public.bh3_habitat_remove_overlaps(
	habitat_sensitivity_schema name,
	habitat_sensitivity_table name,
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
	temp_table_habitat_self_join name;
	temp_table_habitat_self_join_union name;
	temp_table_habitat_unique_overlaps name;
	temp_table_habitat_overlaps_removed name;
	temp_table_habitat_overlaps_replaced name;

BEGIN
	BEGIN
		success := false;

		temp_table_habitat_self_join := 'habitat_self_join'::name;
		temp_table_habitat_self_join_union := 'habitat_self_join_union'::name;
		temp_table_habitat_unique_overlaps := 'habitat_unique_overlaps'::name;
		temp_table_habitat_overlaps_removed := 'habitat_overlaps_removed'::name;
		temp_table_habitat_overlaps_replaced := 'habitat_overlaps_replaced'::name;
		
		start_time := clock_timestamp();

		/* clean up any previous temp tables left behind */
		CALL bh3_drop_temp_table(temp_table_habitat_self_join);
		CALL bh3_drop_temp_table(temp_table_habitat_self_join_union);
		CALL bh3_drop_temp_table(temp_table_habitat_unique_overlaps);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_removed);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_replaced);

		RAISE INFO 'bh3_habitat_remove_overlaps: Removed any outputs of previous runs: %', (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* spatially self-join habitat_sensitivity intersecting overlapping geometries  
		into temporary table combining attributes from both left and right sides */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT l.gid AS gid_l'
						   ',l.hab_type AS hab_type_l'
						   ',l.eunis_l3 AS eunis_l3_l'
						   ',l.sensitivity_ab_su_num_max AS sensitivity_ab_su_num_max_l'
						   ',l.confidence_ab_su_num AS confidence_ab_su_num_l'
						   ',l.sensitivity_ab_ss_num_max AS sensitivity_ab_ss_num_max_l'
						   ',l.confidence_ab_ss_num AS confidence_ab_ss_num_l'
						   ',r.gid AS gid_r'
						   ',r.hab_type AS hab_type_r'
						   ',r.eunis_l3 AS eunis_l3_r'
						   ',r.sensitivity_ab_su_num_max AS sensitivity_ab_su_num_max_r'
						   ',r.confidence_ab_su_num AS confidence_ab_su_num_r'
						   ',r.sensitivity_ab_ss_num_max AS sensitivity_ab_ss_num_max_r'
						   ',r.confidence_ab_ss_num	AS confidence_ab_ss_num_r'
						   ',(ST_Dump('
						   'CASE '
							   'WHEN ST_Touches(l.the_geom,r.the_geom) THEN NULL '
							   'ELSE ST_Intersection(l.the_geom, r.the_geom) '
						   'END)).geom AS the_geom '
					   'FROM %2$I.%3$I l '
						   'JOIN %2$I.%3$I r ON l.gid < r.gid AND ST_Intersects(l.the_geom,r.the_geom)',
					   temp_table_habitat_self_join, habitat_sensitivity_schema, habitat_sensitivity_table);
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % rows of self joined %.% into temporary table %: %', 
			rows_affected, habitat_sensitivity_schema, habitat_sensitivity_table, temp_table_habitat_self_join, 
			(clock_timestamp() - start_time);

		start_time := clock_timestamp();
		
		/* split joined set into constituent rows where each row is 
		one overlapping geometry with its original attributes */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT gid_l AS gid'
						   ',gid_l'
						   ',gid_r'
						   ',hab_type_l AS hab_type'
						   ',eunis_l3_l AS eunis_l3'
						   ',sensitivity_ab_su_num_max_l AS sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num_l AS confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max_l AS sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num_l AS confidence_ab_ss_num'
						   ',ST_Multi(ST_Union(the_geom)) AS the_geom '
					   'FROM %2$I '
					   'WHERE the_geom IS NOT NULL '
						   'AND NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1 ' 
					   'GROUP BY gid_l'
						   ',gid_r'
						   ',hab_type_l'
						   ',eunis_l3_l'
						   ',sensitivity_ab_su_num_max_l'
						   ',confidence_ab_su_num_l'
						   ',sensitivity_ab_ss_num_max_l'
						   ',confidence_ab_ss_num_l '
					   'UNION '
					   'SELECT gid_r AS gid'
						   ',gid_l'
						   ',gid_r'
						   ',hab_type_r AS hab_type'
						   ',eunis_l3_r AS eunis_l3'
						   ',sensitivity_ab_su_num_max_r AS sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num_r AS confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max_r AS sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num_r AS confidence_ab_ss_num'
						   ',ST_Multi(ST_Union(the_geom)) AS the_geom '
					   'FROM %2$I '
					   'WHERE the_geom IS NOT NULL '
						   'AND NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1 '
					   'GROUP BY gid_l'
						   ',gid_r'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num',
					   temp_table_habitat_self_join_union, temp_table_habitat_self_join)
		USING 'Polygon';
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % split rows from temporary table % into temporary table %: %', 
			rows_affected, temp_table_habitat_self_join, temp_table_habitat_self_join_union, 
			(clock_timestamp() - start_time);

		start_time := clock_timestamp();
		
		/* repair any invalid geometries */
		CALL bh3_repair_geometries(NULL, temp_table_habitat_self_join_union);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_self_join_union, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* rank duplicate overlaps by sensitivity and confidence keeping only top ranked row */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_ranking AS '
					   '('
						   'SELECT RANK() OVER('
											   'PARTITION BY '
												   'the_geom '
											   'ORDER BY '
												   'sensitivity_ab_su_num_max DESC,'
												   'confidence_ab_su_num DESC,'
												   'sensitivity_ab_ss_num_max DESC,'
												   'confidence_ab_ss_num DESC,'
												   'CASE WHEN gid = gid_l THEN 0 ELSE 1 END) AS ranking'
							   ',gid'
							   ',gid_l'
							   ',gid_r'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',the_geom '
						   'FROM %2$I'
					   ') '
					   'SELECT gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
						   ',ST_Multi(ST_Union(the_geom)) AS the_geom '
					   'FROM cte_ranking '
					   'WHERE ranking = 1 '
					   'GROUP BY gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num',
					   temp_table_habitat_unique_overlaps, temp_table_habitat_self_join_union);
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % unique overlap rows ranked by sensitivity and confidence into temporary table %: %', 
			rows_affected, temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* repair any invalid geometries */
		CALL bh3_repair_geometries(NULL, temp_table_habitat_unique_overlaps);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create spatial index on temp_table_habitat_unique_overlaps */
		CALL bh3_index(NULL, temp_table_habitat_unique_overlaps, 
					   ARRAY[
						   ARRAY['the_geom','s']
					   ]);

		RAISE INFO 'bh3_habitat_remove_overlaps: Created spatial index on temporary table %: %', 
			temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* erase unique_overlaps from input table habitat_sensitivity 
		into temporary table temp_table_habitat_overlaps_removed */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_sel AS '
					   '('
						   'SELECT hab.gid'
							   ',hab.hab_type'
							   ',hab.eunis_l3'
							   ',hab.sensitivity_ab_su_num_max'
							   ',hab.confidence_ab_su_num'
							   ',hab.sensitivity_ab_ss_num_max'
							   ',hab.confidence_ab_ss_num'
							   ',(ST_Dump('
							   'CASE '
								   'WHEN uov.the_geom IS NULL THEN hab.the_geom '
								   'WHEN ST_Touches(hab.the_geom,uov.the_geom) THEN hab.the_geom '
								   'ELSE ST_Difference(hab.the_geom,uov.the_geom) '
							   'END)).geom AS the_geom '
						   'FROM %2$I.%3$I hab '
							   'LEFT JOIN %4$I uov ON hab.gid = uov.gid'
					   ') '
					   'SELECT gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
						   ',ST_Multi(ST_Union('
						   'CASE '
							   'WHEN ST_IsValid(the_geom) THEN the_geom '
							   'ELSE ST_Buffer(the_geom,0) '
						   'END)) AS the_geom '
					   'FROM cte_sel '
					   'WHERE NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1 '
					   'GROUP BY gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num',
					   temp_table_habitat_overlaps_removed, habitat_sensitivity_schema, 
					   habitat_sensitivity_table, temp_table_habitat_unique_overlaps)
		USING 'Polygon';
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % rows with overlaps removed into temporary table %: %', 
			rows_affected, temp_table_habitat_overlaps_removed, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* repair any invalid geometries */
		CALL bh3_repair_geometries(NULL, temp_table_habitat_overlaps_removed);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_overlaps_removed, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create spatial index on temp_table_habitat_overlaps_removed */
		CALL bh3_index(NULL, temp_table_habitat_overlaps_removed, 
					   ARRAY[
						   ARRAY['the_geom','s']
					   ]);

		RAISE INFO 'bh3_habitat_remove_overlaps: Created spatial index on temporary table %: %', 
			temp_table_habitat_overlaps_removed, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* merge temporary tables temp_table_habitat_overlaps_removed (input table habitat_sensitivity rows with 
		overlaps removed) and temp_table_habitat_unique_overlaps (overlaps with highest sensitivity/confidence) */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_union AS '
					   '('
						   'SELECT gid'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num '
						   'FROM %2$I '
						   'UNION '
						   'SELECT gid'
							   ',the_geom '
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num '
						   'FROM %3$I '
						   'ORDER BY gid'
					   ') '
					   'SELECT ROW_NUMBER() OVER() AS gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num '
					   'FROM cte_union',
					   temp_table_habitat_overlaps_replaced, temp_table_habitat_overlaps_removed, 
					   temp_table_habitat_unique_overlaps);
					   
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % rows into temporary table % from temporary tables % and %: %', 
					   rows_affected, temp_table_habitat_overlaps_replaced, temp_table_habitat_overlaps_removed, 
					   temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* empty input table */
		EXECUTE format('TRUNCATE ONLY %1$I.%2$I', habitat_sensitivity_schema, habitat_sensitivity_table);

		RAISE INFO 'bh3_habitat_remove_overlaps: Emptied input table %.%: %', 
			habitat_sensitivity_schema, habitat_sensitivity_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* copy rows from temp_table_habitat_overlaps_removed into input table */
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
					   'SELECT gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num '
					   'FROM %3$I', 
					   habitat_sensitivity_schema, habitat_sensitivity_table, 
					   temp_table_habitat_overlaps_replaced);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Copied % rows from temporary table % into input table %.%: %', 
			rows_affected, temp_table_habitat_overlaps_replaced, habitat_sensitivity_schema, habitat_sensitivity_table, 
			(clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* drop temp tables */
		CALL bh3_drop_temp_table(temp_table_habitat_self_join);
		CALL bh3_drop_temp_table(temp_table_habitat_self_join_union);
		CALL bh3_drop_temp_table(temp_table_habitat_unique_overlaps);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_removed);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_replaced);

		RAISE INFO 'bh3_habitat_remove_overlaps: Dropped temporary tables: %', (clock_timestamp() - start_time);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Error message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_habitat_remove_overlaps(name, name)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_habitat_remove_overlaps(name, name)
    IS 'Purpose:
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





DROP FUNCTION IF EXISTS public.bh3_safe_difference(geometry, geometry);

CREATE OR REPLACE FUNCTION public.bh3_safe_difference(
	geom_from geometry,
	geom_erase geometry)
    RETURNS geometry
    LANGUAGE 'plpgsql'

    COST 100
    STABLE STRICT 
AS $BODY$
BEGIN
	BEGIN
    	RETURN ST_Difference(geom_from, geom_erase);
    EXCEPTION
        WHEN OTHERS THEN
            BEGIN
                RETURN ST_Difference(ST_Buffer(geom_from, 0.00000001), ST_Buffer(geom_erase, 0.00000001));
			EXCEPTION
				WHEN OTHERS THEN
					RETURN ST_GeomFromText('POLYGON EMPTY');
			END;
    END;
END
$BODY$;

ALTER FUNCTION public.bh3_safe_difference(geometry, geometry)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_safe_difference(geometry, geometry)
    IS 'Purpose:
Wrapper around ST_Difference with exception handling.

Approach:
Calls ST_Difference on the unaltered input parameters. 
If it throws an exception it is called again on the input geometries buffered by 0.00000001.
If that also fails an empty polygon is returned.

Parameters:
Name	Type	Description	Default value
geom_from	geometry	Geometry from which to erase geom_erase (first parameter passed into ST_Difference).	
geom_erase	geometry	Geometry to erase from geom_from (second parameter passed into ST_Difference).	

Returns:
ST_Difference of the two (possibly buffered) geometries, or if that fails an empty polygon.

Calls:
No nested calls';





DROP FUNCTION IF EXISTS public.bh3_safe_union_transfn(geometry, geometry);

CREATE OR REPLACE FUNCTION public.bh3_safe_union_transfn(
	agg_state geometry,
	el geometry)
    RETURNS geometry
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE 
AS $BODY$
BEGIN
	IF agg_state IS NULL THEN
    	RETURN el;
	ELSIF el IS NOT NULL THEN
		BEGIN
			RETURN ST_Union(agg_state, el);
		EXCEPTION
			WHEN OTHERS THEN
				BEGIN
					RETURN ST_Union(ST_Buffer(agg_state, 0.00000001), ST_Buffer(el, 0.00000001));
				EXCEPTION
					WHEN OTHERS THEN
						RETURN ST_GeomFromText('POLYGON EMPTY');
				END;
		END;
	ELSE
		RETURN ST_GeomFromText('POLYGON EMPTY');
	END IF;
END;
$BODY$;

ALTER FUNCTION public.bh3_safe_union_transfn(geometry, geometry)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_safe_union_transfn(geometry, geometry)
    IS 'Purpose:
State transition function for bh3_safe_union custom aggregate, which is a wrapper around ST_Union with exception handling.

Approach:
Calls ST_Union on the unaltered input parameter. 
If it throws an exception it is called again on the input geometry buffered by 0.00000001.
If that also fails an empty polygon is returned.

Parameters:
agg_state	geometry	State object. ST_Union of input geometries.
el	geometry	Input geometry.

Returns:
ST_Union of the (possibly buffered) geometries, or if that fails an empty polygon.

Calls:
No nested calls.';





DROP FUNCTION IF EXISTS public.bh3_sensitivity(sensitivity_source);

CREATE OR REPLACE FUNCTION public.bh3_sensitivity(
	source_table sensitivity_source)
    RETURNS TABLE(eunis_l3_code character varying, eunis_l3_name text, characterising_species character varying, sensitivity_ab_su_num smallint, sensitivity_ab_ss_num smallint, confidence_ab_su_num smallint, confidence_ab_ss_num smallint) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
BEGIN
	CASE source_table
		WHEN 'broadscale_habitats' THEN
			RETURN QUERY 
			SELECT eunis_l3_code
				,NULL::text AS eunis_l3_name
				,NULL::character varying AS characterising_species
				,sensitivity_ab_su_num_max AS sensitivity_ab_su_num
				,sensitivity_ab_ss_num_max AS sensitivity_ab_ss_num
				,confidence_ab_su_num
				,confidence_ab_ss_num
			FROM lut.sensitivity_broadscale_habitats;
		WHEN 'eco_groups' THEN 
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,characterising_species
				,sensitivity_ab_su_num
				,sensitivity_ab_ss_num
				,confidence_ab_su_num
				,confidence_ab_ss_num
			FROM lut.sensitivity_eco_groups;
		WHEN 'eco_groups_rock' THEN
			RETURN QUERY
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,characterising_species
				,sensitivity_ab_su_num
				,sensitivity_ab_ss_num
				,confidence_ab_su_num
				,confidence_ab_ss_num
			FROM lut.sensitivity_eco_groups_rock;
		WHEN 'rock' THEN
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,species_name AS characterising_species
				,sensitivity_ab_su_num
				,NULL::smallint AS sensitivity_ab_ss_num
				,confidence_ab_ss_num
				,NULL::smallint AS confidence_ab_ss_num
			FROM lut.sensitivity_rock;
		WHEN 'rock_eco_groups' THEN
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,coalesce(r.species_name,e.characterising_species) AS characterising_species
				,CASE 
					WHEN r.sensitivity_ab_su_num IS NOT NULL AND e.sensitivity_ab_su_num IS NOT NULL THEN 
						CASE
							WHEN r.confidence_ab_su_num > e.confidence_ab_su_num THEN r.sensitivity_ab_su_num 
							WHEN r.confidence_ab_su_num < e.confidence_ab_su_num THEN e.sensitivity_ab_su_num
							ELSE greatest(r.sensitivity_ab_su_num,e.sensitivity_ab_su_num)
						END
					ELSE
						coalesce(r.sensitivity_ab_su_num,e.sensitivity_ab_su_num) 
				END AS sensitivity_ab_su_num
				,e.sensitivity_ab_ss_num
				,greatest(r.confidence_ab_su_num,e.confidence_ab_su_num) AS confidence_ab_su_num
				,e.confidence_ab_ss_num
			FROM lut.sensitivity_rock r
				FULL JOIN lut.sensitivity_eco_groups e ON r.species_name = e.characterising_species;
		ELSE
			RETURN;
	END CASE;
END;
$BODY$;

ALTER FUNCTION public.bh3_sensitivity(sensitivity_source)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_sensitivity(sensitivity_source)
    IS 'Purpose:
Retrieves sensitivity rows from the specified table.

Approach:
Performs a select query against the specified source table standardising the return table''s schema.

Parameters:
source_table	sensitivity_source		Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).

Returns:
An in-memory table of sensitivity rows with standardised schema.

Calls:
No nested calls.';





DROP FUNCTION IF EXISTS public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, integer, integer, name, character varying[], boolean, name, name, name, name, integer);

CREATE OR REPLACE FUNCTION public.bh3_sensitivity_layer_prep(
	boundary_schema name,
	habitat_schema name,
	output_schema name,
	sensitivity_source_table sensitivity_source,
	start_year integer,
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
	boundary_table name DEFAULT 'boundary'::name,
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	habitat_types_filter_negate boolean DEFAULT false,
	habitat_table name DEFAULT 'habitat_sensitivity'::name,
	habitat_table_grid name DEFAULT 'habitat_sensitivity_grid'::name,
	output_table_max name DEFAULT 'species_sensitivity_max'::name,
	output_table_mode name DEFAULT 'species_sensitivity_mode'::name,
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
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table_max]::name[]
				,ARRAY[output_schema, output_table_mode]::name[]
			]::name[][]);

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
					   'FROM bh3_species_sensitivity_clipped($1,$2,$3,$4,$5,$6,$7,$8)',
					   species_clip_table)
		USING boundary_schema, sensitivity_source_table, start_year, end_year, 
			boundary_table, habitat_types_filter, habitat_types_filter_negate, output_srid;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_sensitivity_layer_prep: Inserted % rows into temporary table %: %', 
			rows_affected, species_clip_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_index(NULL, species_clip_table, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','']
						   ,ARRAY['sensitivity_ab_su_num','']
						   ,ARRAY['sensitivity_ab_ss_num','']
					   ]);

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

		/* index species sensitivity maximum table */
		CALL bh3_index(output_schema, output_table_max, 
					   ARRAY[
						   ARRAY['the_geom','s'] 
						   ,ARRAY['gid','u']
						   ,ARRAY['hab_type','']
						   ,ARRAY['eunis_l3','']
						   ,ARRAY['sensitivity_ab_su_num','']
						   ,ARRAY['sensitivity_ab_ss_num','']
					   ]);

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

		/* index species sensitivity mode table */
		CALL bh3_index(output_schema, output_table_mode, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
						   ,ARRAY['hab_type','']
						   ,ARRAY['eunis_l3','']
						   ,ARRAY['sensitivity_ab_su_num','']
						   ,ARRAY['sensitivity_ab_ss_num','']
					   ]);

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
		RAISE INFO 'bh3_sensitivity_layer_prep: Error. Message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, integer, integer, name, character varying[], boolean, name, name, name, name, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_sensitivity_layer_prep(name, name, name, sensitivity_source, integer, integer, name, character varying[], boolean, name, name, name, name, integer)
    IS 'Purpose:
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
boundary_schema				name							Schema of table containing single AOI boundary polygon and bounding box.
habitat_schema				name							Schema of input habitat_table.
output_schema				name							Schema in which output tables will be created (will be created if it does not already exist; tables in it will be overwritten).
sensitivity_source_table	sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'', ''eco_groups_rock'' }).
start_year					integer							Earliest year for Marine Recorder spcies samples to be included.
end_year					integer							Latest year for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_table				name							Name of table containing single AOI boundary polygon and bounding boxs. Defaults to ''boundary''.
habitat_types_filter		character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
habitat_types_filter_negate	boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
habitat_table				name							Name of habitat sensitivity output table. Defaults to ''habitat_sensitivity''.
habitat_table_grid			name							Name of gridded habitat sensitivity output table. Defaults to ''habitat_sensitivity_grid''.
output_table_max			name							Table name of species sensitivity maximum map. Defaults to ''species_sensitivity_max''.
output_table_mode			name							Table name of species sensitivity mode map. Defaults to ''species_sensitivity_mode''.
output_srid					integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table
bh3_find_srid
bh3_species_sensitivity_clipped.';





DROP FUNCTION IF EXISTS public.bh3_sensitivity_map(name, name, name, name, name, name, name);

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





DROP FUNCTION IF EXISTS public.bh3_species_sensitivity_clipped(name, sensitivity_source, integer, integer, name, character varying[], boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_species_sensitivity_clipped(
	boundary_schema name,
	sensitivity_source_table sensitivity_source,
	start_year integer,
	end_year integer DEFAULT (date_part('year'::text, now()))::integer,
	boundary_table name DEFAULT 'boundary'::name,
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	habitat_types_filter_negate boolean DEFAULT false,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(gid bigint, the_geom geometry, survey_key character varying, survey_event_key character varying, sample_reference character varying, sample_date timestamp with time zone, biotope_code character varying, biotope_desc text, qualifier character varying, eunis_code_2007 character varying, eunis_name_2007 character varying, annex_i_habitat character varying, characterising_species character varying, sensitivity_ab_su_num smallint, confidence_ab_su_num smallint, sensitivity_ab_ss_num smallint, confidence_ab_ss_num smallint) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	srid_smp int;
	geom_exp_smp text;
	eunis_corr_table_eunis_code_column name;
	date_start timestamp without time zone;
	date_end timestamp without time zone;

BEGIN
	eunis_corr_table_eunis_code_column := 'eunis_code_2007';

	date_start := to_date(start_year::varchar, 'yyyy')::timestamp without time zone;
	date_end := format('%s-%s-%s', end_year, 1, 1)::timestamp without time zone;

	srid_smp := bh3_find_srid(boundary_schema, boundary_table, 'the_geom');
	IF srid_smp != output_srid AND srid_smp > 0 AND output_srid > 0 THEN
		geom_exp_smp := format('ST_Transform(smp.%1$I,%2$s) AS %1$I', 'the_geom', output_srid);
	ELSE 
		geom_exp_smp := format('smp.%I', 'the_geom');
	END IF;

	IF array_length(habitat_types_filter, 1) = 1 THEN
		RETURN QUERY EXECUTE format('SELECT ROW_NUMBER() OVER() AS gid'
										',%1$s'
										',srv.survey_key'
										',sve.survey_event_key'
										',smp.sample_reference'
										',smp.sample_date'
										',sba.biotope_code'
										',sba.biotope_desc'
										',sba.qualifier'
										',ect.eunis_code_2007'
										',ect.eunis_name_2007'
										',ect.annex_i_habitat'
										',egr.characterising_species'
										',egr.sensitivity_ab_su_num'
										',egr.confidence_ab_su_num'
										',egr.sensitivity_ab_ss_num'
										',egr.confidence_ab_ss_num '
									'FROM marinerec.survey srv '
										'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
										'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
										'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
										'JOIN bh3_sensitivity($1) egr ON egr.characterising_species = spc.species_name '
										'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
										'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
										'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
									'WHERE smp.sample_date >= $2 '
										'AND smp.sample_date <= $3 '
										'AND %4$s',
									geom_exp_smp, boundary_schema, boundary_table, 
									CASE
										WHEN habitat_types_filter_negate THEN format('ect.%1$I !~* concat(%2$L, %3$L)', eunis_corr_table_eunis_code_column, '^', habitat_types_filter[1])
										ELSE format('ect.%1$I ~* concat(%2$L, %3$L)', eunis_corr_table_eunis_code_column, '^', habitat_types_filter[1])
									END)
		USING sensitivity_source_table, date_start, date_end;
	ELSIF array_length(habitat_types_filter, 1) > 1 THEN
		IF habitat_types_filter_negate THEN 
			RETURN QUERY EXECUTE format('WITH cte_hab_filter AS '
										'('
											'SELECT unnest($1) AS eunis_l3'
										') '
										'SELECT ROW_NUMBER() OVER() AS gid'
											',%1$s'
											',srv.survey_key'
											',sve.survey_event_key'
											',smp.sample_reference'
											',smp.sample_date'
											',sba.biotope_code'
											',sba.biotope_desc'
											',sba.qualifier'
											',ect.eunis_code_2007'
											',ect.eunis_name_2007'
											',ect.annex_i_habitat'
											',egr.characterising_species'
											',egr.sensitivity_ab_su_num'
											',egr.confidence_ab_su_num'
											',egr.sensitivity_ab_ss_num'
											',egr.confidence_ab_ss_num '
										'FROM marinerec.survey srv '
											'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
											'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
											'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
											'JOIN bh3_sensitivity($2) egr ON egr.characterising_species = spc.species_name '
											'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
											'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
											'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
											'LEFT JOIN cte_hab_filter ON ect.%4$s ~* concat(''^'', cte_hab_filter.eunis_l3) '
										'WHERE smp.sample_date >= $3 '
											'AND smp.sample_date <= $4 '
											'AND cte_hab_filter.eunis_l3 IS NULL',
										geom_exp_smp, boundary_schema, boundary_table, eunis_corr_table_eunis_code_column)
			USING habitat_types_filter, sensitivity_source_table, date_start, date_end;
		ELSE
			RETURN QUERY EXECUTE format('WITH cte_hab_filter AS '
										'('
											'SELECT unnest($1) AS eunis_l3'
										') '
										'SELECT ROW_NUMBER() OVER() AS gid'
											',%1$s'
											',srv.survey_key'
											',sve.survey_event_key'
											',smp.sample_reference'
											',smp.sample_date'
											',sba.biotope_code'
											',sba.biotope_desc'
											',sba.qualifier'
											',ect.eunis_code_2007'
											',ect.eunis_name_2007'
											',ect.annex_i_habitat'
											',egr.characterising_species'
											',egr.sensitivity_ab_su_num'
											',egr.confidence_ab_su_num'
											',egr.sensitivity_ab_ss_num'
											',egr.confidence_ab_ss_num '
										'FROM marinerec.survey srv '
											'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
											'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
											'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
											'JOIN bh3_sensitivity($2) egr ON egr.characterising_species = spc.species_name '
											'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
											'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
											'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
											'JOIN cte_hab_filter ON ect.%4$s ~* concat(''^'', cte_hab_filter.eunis_l3) '
										'WHERE smp.sample_date >= $3 '
											'AND smp.sample_date <= $4',
										geom_exp_smp, boundary_schema, boundary_table, eunis_corr_table_eunis_code_column)
			USING habitat_types_filter, sensitivity_source_table, date_start, date_end;
		END IF;
	ELSE
		RETURN QUERY EXECUTE format('SELECT ROW_NUMBER() OVER() AS gid'
										',%1$s'
										',srv.survey_key'
										',sve.survey_event_key'
										',smp.sample_reference'
										',smp.sample_date'
										',sba.biotope_code'
										',sba.biotope_desc'
										',sba.qualifier'
										',ect.eunis_code_2007'
										',ect.eunis_name_2007'
										',ect.annex_i_habitat'
										',egr.characterising_species'
										',egr.sensitivity_ab_su_num'
										',egr.confidence_ab_su_num'
										',egr.sensitivity_ab_ss_num'
										',egr.confidence_ab_ss_num '
									'FROM marinerec.survey srv '
										'JOIN marinerec.survey_event sve ON srv.survey_key = sve.survey_key '
										'JOIN marinerec.sample smp ON smp.survey_event_key = sve.survey_event_key '
										'JOIN marinerec.sample_species spc ON smp.sample_reference = spc.sample_reference '
										'JOIN bh3_sensitivity($1) egr ON egr.characterising_species = spc.species_name '
										'JOIN marinerec.sample_biotope_all sba ON smp.sample_reference = sba.sample_reference '
										'JOIN lut.eunis_correlation_table ect ON ect.jncc_15_03_code = sba.biotope_code '
										'JOIN %2$I.%3$I bnd ON ST_CoveredBy(%1$s,bnd.the_geom)'
									'WHERE smp.sample_date >= $2 '
										'AND smp.sample_date <= $3',
									geom_exp_smp, boundary_schema, boundary_table)
		USING sensitivity_source_table, date_start, date_end;
	END IF;
END;
$BODY$;

ALTER FUNCTION public.bh3_species_sensitivity_clipped(name, sensitivity_source, integer, integer, name, character varying[], boolean, integer)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_species_sensitivity_clipped(name, sensitivity_source, integer, integer, name, character varying[], boolean, integer)
    IS 'Purpose:
Creates a table of species sensitivity rows within the specified boundary polygon(s).

Approach:
Selects rows from a hard coded join of Marine Recorder tables (marinerec.survey srv, marinerec.survey_event, marinerec.sample,
marinerec.sample_species, marinerec.sample_biotope_all) and lut.eunis_correlation_table with the specified sensitivity table within
the specified boundary polygon(s).

Parameters:
boundary_schema					name							Schema of table containing single AOI boundary polygon and bounding box.
sensitivity_source_table		sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
start_year						integer							Earliest year for Marine Recorder spcies samples to be included.
end_year						integer							Latest year for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_table					name							Name of table containing single AOI boundary polygon and bounding box. Defaults to ''boundary''.
habitat_types_filter			character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
habitat_types_filter_negate		boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
output_srid						integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
An in-memory table of species sensitivity rows.

Calls:
bh3_find_srid
bh3_sensitivity';
