/*
CREATE EXTENSION "uuid-ossip";
SELECT uuid_generate_v4();
*/

select * from test.habitat_sensitivity where not ST_IsValid(the_geom) or not ST_IsSimple(the_geom) or ST_IsEmpty(the_geom); --N=392 -> 0
select * from test.habitat_sensitivity_grid where not ST_IsValid(the_geom) or not ST_IsSimple(the_geom) or ST_IsEmpty(the_geom); --N=563 -> 0
select * from test.pressure_map where not ST_IsValid(the_geom) or not ST_IsSimple(the_geom) or ST_IsEmpty(the_geom); --N=0

CREATE SCHEMA IF NOT EXISTS wales;

/* determine the c-square size from the pressure tables */
SELECT * 
FROM public.bh3_get_pressure_csquares_size(
	'ices_abrasion'							--pressure_schema name
	,'2012-01-01'::timestamp				--date_start timestamp without time zone
	,now()::timestamp						--date_end timestamp without time zone DEFAULT now()
	,'sar_surface'							--sar_surface_column name DEFAULT 'sar_surface'::name
	,'sar_subsurface'						--sar_subsurface_column name DEFAULT 'sar_subsurface'::name
	,4326									--output_srid integer DEFAULT 4326
);


/* create habitat_sensitivity for selected AOI in output_schema 
outputs:	habitat_sensitivity
calls:		bh3_drop_temp_table
			bh3_find_srid
run time: 	2 min 29 secs w/o overlaps (Wales) */
SELECT * 
FROM public.bh3_habitat_boundary_clip(
	0::integer								--boundary_filter integer,
	,NULL::character varying[]				--habitat_types_filter character varying[]
	,'wales'								--output_schema name
	,'habitat_sensitivity'					--output_table name DEFAULT 'habitat_sensitivity'::name
	,'static'								--habitat_schema name DEFAULT 'static'::name
	,'uk_habitat_map_wgs84'					--habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name
	,'lut'									--sensitivity_schema name DEFAULT 'lut'::name
	,'sensitivity_broadscale_habitats'		--sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name
	,'static'								--boundary_schema name DEFAULT 'static'::name
	,'official_country_waters_wgs84'		--boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	,false									--boundary_filter_negate boolean DEFAULT false
	,false									--habitat_types_filter_negate boolean DEFAULT false
	,true									--exclude_empty_mismatched_eunis_l3 boolean DEFAULT true
	,false									--remove_overlaps boolean DEFAULT false


/* grid habitat_sensitivity into create habitat_sensitivity_grid  
outputs:	habitat_sensitivity_grid
calls:		bh3_drop_temp_table
			bh3_find_srid
			bh3_create_csquares
			bh3_create_fishnet
run time: 	24 secs 422 msec (Wales) */
SELECT * 
FROM public.bh3_habitat_grid(
	0										--boundary_filter integer
	,'wales'								--,habitat_sensitivity_schema name
	,'wales'								--,output_schema name
	,'habitat_sensitivity'					--,habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name
	,'habitat_sensitivity_grid'				--,output_table name DEFAULT 'habitat_sensitivity_grid'::name
	,'static'								--,boundary_schema name DEFAULT 'static'::name
	,'official_country_waters_wgs84'		--,boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	,false									--,boundary_filter_negate boolean DEFAULT false
	,0.05									--,cell_size_degrees numeric DEFAULT 0.05
);



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
	'wales'									--habitat_schema name
	,'wales'								--output_schema name
	,0										--boundary_filter integer
	,'rock_eco_groups'::sensitivity_source	--sensitivity_source_table sensitivity_source
	,'2012-01-01'::timestamp				--date_start timestamp without time zone
	,now()::timestamp						--date_end timestamp without time zone DEFAULT now()
	,NULL::character varying[]				--habitat_types_filter character varying[] DEFAULT NULL
	,'habitat_sensitivity'					--habitat_table name DEFAULT 'habitat_sensitivity'::name
	,'habitat_sensitivity_grid'				--habitat_table_grid name DEFAULT 'habitat_sensitivity_grid'::name
	,species_sensitivity_max_table			--output_table_max name DEFAULT 'species_sensitivity_max'::name
	,'species_sensitivity_max'				--output_table_mode name DEFAULT 'species_sensitivity_mode'::name
	,'static'								--boundary_schema name DEFAULT 'static'::name
	,'official_country_waters_wgs84'		--boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	,false									--boundary_filter_negate boolean DEFAULT false
	,false									--habitat_types_filter_negate boolean DEFAULT false
	,4326									--output_srid integer DEFAULT 4326
);


/* create sensitivity_map in output_schema from habitat_sensitivity, species_sensitivity_max 
and species_sensitivity_mode tables, all located in schema output_schema
outputs:	sensitivity_map
calls:		bh3_drop_temp_table
run time: 6 min 7 secs (Wales) */
SELECT * 
FROM bh3_sensitivity_map(
	'wales'									--habitat_sensitivity_schema name
	,'wales'								--species_sensitivity_schema name
	,'wales'								--output_schema name
	,'habitat_sensitivity'					--habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name
	,'species_sensitivity_max'				--species_sensitivity_max_table name DEFAULT 'species_sensitivity_max'::name
	,'species_sensitivity_mode'				--species_sensitivity_mode_table name DEFAULT 'species_sensitivity_mode'::name
	,'sensitivity_map'						--output_table name DEFAULT 'sensitivity_map'::name
	,4326									--output_srid integer DEFAULT 4326
);


/* create pressure_map for selected AOI in output_schema from sensitivity_map 
in output_schema and pressure tables in pressure_schema.
outputs:	pressure_map
calls:		bh3_drop_temp_table
			bh3_get_pressure_csquares
			bh3_find_srid
run time:	55 secs 313 msec (Wales) */
SELECT *
FROM bh3_disturbance_map(
	0										--boundary_filter integer
	,'2012-01-01'::timestamp				--date_start timestamp without time zone
	,'ices_abrasion'						--pressure_schema name 
	,'wales'								--sensitivity_map_schema name
	,'wales'								--output_schema name
	,'static'								--boundary_schema name DEFAULT 'static'::name
	,'official_country_waters_wgs84'		--boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	,'sensitivity_map'						--sensitivity_map_table name DEFAULT 'sensitivity_map'::name
	,'disturbance_map'						--output_table name DEFAULT 'disturbance_map'::name
	,'sar_surface'							--sar_surface_column name DEFAULT 'sar_surface'::name
	,'sar_subsurface'						--sar_subsurface_column name DEFAULT 'sar_subsurface'::name
	,false									--boundary_filter_negate boolean DEFAULT false
	,now()::timestamp						--date_end timestamp without time zone DEFAULT now()
	,4326									--output_srid integer DEFAULT 4326
);
