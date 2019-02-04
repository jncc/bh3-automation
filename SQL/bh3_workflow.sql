/*
CREATE EXTENSION "uuid-ossip";
SELECT uuid_generate_v4();
*/

select * from test.habitat_sensitivity where not ST_IsValid(the_geom) or not ST_IsSimple(the_geom) or ST_IsEmpty(the_geom); --N=392 -> 0
select * from test.habitat_sensitivity_grid where not ST_IsValid(the_geom) or not ST_IsSimple(the_geom) or ST_IsEmpty(the_geom); --N=563 -> 0
select * from test.pressure_map where not ST_IsValid(the_geom) or not ST_IsSimple(the_geom) or ST_IsEmpty(the_geom); --N=0

CREATE SCHEMA IF NOT EXISTS wales;

/* create habitat_sensitivity and habitat_sensitivity_grid for Wales in schema "wales"
from habitat map static.uk_habitat_map_wgs84 (result of ArcGIS union) 
outputs:	habitat_sensitivity
			habitat_sensitivity_grid
calls:		bh3_drop_temp_table
			bh3_find_srid
			bh3_create_csquares
run time: 	2 min 40 secs */
SELECT * 
FROM public.bh3_habitat_grid(
	0::integer --boundary_filter integer
	,NULL::character varying[] --habitat_types_filter character varying[]
	,'wales' --output_schema name
	,'habitat_sensitivity'::name --,output_table name DEFAULT 'habitat_sensitivity'::name
	,'habitat_sensitivity_grid'::name --,output_table_grid name DEFAULT 'habitat_sensitivity_grid'::name
	,'static'::name --,habitat_schema name DEFAULT 'static'::name
	,'uk_habitat_map_wgs84'::name --,habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name
	,'lut'::name --,sensitivity_schema name DEFAULT 'lut'::name
	,'sensitivity_broadscale_habitats'::name --,sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name
	,'static'::name --,boundary_schema name DEFAULT 'static'::name
	,'official_country_waters_wgs84'::name --,boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	,false --,boundary_filter_negate boolean DEFAULT false
	,false --,habitat_types_filter_negate boolean DEFAULT false
	,true --,exclude_empty_mismatched_eunis_l3 boolean DEFAULT true
	,true --,remove_overlaps boolean DEFAULT false
	,0.05 --,cell_size_degrees numeric DEFAULT 0.05
);
/* create habitat_sensitivity and habitat_sensitivity_grid for Wales in schema "wales"
outputs:	habitat_sensitivity
			habitat_sensitivity_grid
calls:		bh3_drop_temp_table
			bh3_find_srid
			bh3_create_csquares
run time: 	2 min 48 secs (excluding empty/mismatched eunis_l3)
			8 min  4 secs (including empty/mismatched eunis_l3) */
SELECT *
FROM public.bh3_habitat_clip_by_subdivide(
	0::integer --boundary_filter integer
	,NULL::character varying[] --habitat_types_filter character varying[]
	,'wales'::name --output_schema name,
	,'habitat_sensitivity'::name--,output_table name DEFAULT 'habitat_sensitivity'::name
	,'habitat_sensitivity_grid'::name--,output_table_grid name DEFAULT 'habitat_sensitivity_grid'::name
	,'static'::name--,habitat_schema name DEFAULT 'static'::name
	,'uk_habitat_map_wgs84'::name--,habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name
	,'lut'::name--,sensitivity_schema name DEFAULT 'lut'::name
	,'sensitivity_broadscale_habitats'::name--,sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name
	,'static'::name--,boundary_schema name DEFAULT 'static'::name
	,'official_country_waters_wgs84'::name--,boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	,false--,boundary_filter_negate boolean DEFAULT false
	,false--,habitat_types_filter_negate boolean DEFAULT false
	,false--,exclude_empty_mismatched_eunis_l3 boolean DEFAULT true
	,0.05::numeric--,cell_size_degrees numeric DEFAULT 0.05
);



/* create species_sensitivity_max and species_sensitivity_mode for Wales in schema "test" 
using habitat_sensitivity and habitat_sensitivity_grid created in presvious steps 
rock/eco group sensitivity scores and Marine Recorder samples starting 2012-01-01. 
outputs:	species_sensitivity_max
			species_sensitivity_mode
calls:		bh3_find_srid
			bh3_drop_temp_table
			bh3_species_sensitivity_clipped
			bh3_sensitivity
run time:	32 secs 852 msec */
SELECT * 
FROM public.bh3_sensitivity_layer_prep(
	'wales' --habitat_schema name
	,'wales' --output_schema name
	,0 --boundary_filter integer
	,'rock_eco_groups'::sensitivity_source --sensitivity_source_table sensitivity_source
	,'2012-01-01'::timestamp --date_start timestamp without time zone
	--,date_end timestamp without time zone DEFAULT now()
	--,habitat_table name DEFAULT 'habitat_sensitivity'::name
	--,habitat_table_grid name DEFAULT 'habitat_sensitivity_grid'::name
	--,output_table_max name DEFAULT 'species_sensitivity_max'::name
	--,output_table_mode name DEFAULT 'species_sensitivity_mode'::name
	--,boundary_schema name DEFAULT 'static'::name
	--,boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	--,boundary_filter_negate boolean DEFAULT false
	--,output_srid integer DEFAULT 4326
);

/* create sensitivity_map in schema "test" from habitat_sensitivity, species_sensitivity_max 
and species_sensitivity_mode tables, all located in schema "test"
outputs:	sensitivity_map
calls:		bh3_drop_temp_table
run time: 6 min 7 secs */
SELECT * 
FROM public.bh3_sensitivity_map(
	'wales' --habitat_sensitivity_schema name
	,'wales' --species_sensitivity_schema name
	,'wales' --output_schema name
	--,habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name
	--,species_sensitivity_max_table name DEFAULT 'species_sensitivity_max'::name
	--,species_sensitivity_mode_table name DEFAULT 'species_sensitivity_mode'::name
	--,output_table name DEFAULT 'sensitivity_map'::name
);


/* create pressure_map for Wales in schema "test" from sensitivity_map 
in schema "test" and pressure tables in schema "ices_abrasion".
outputs:	disturbance_map
calls:		bh3_drop_temp_table
			bh3_get_pressure_csquares
			bh3_find_srid
run time:	55 secs 313 msec
			(1 min 2 secs [query]) */
SELECT *
FROM public.bh3_disturbance_map(
	0 --boundary_filter integer
	,'2012-01-01'::timestamp --date_start timestamp without time zone
	,'ices_abrasion' --pressure_schema name 
	,'wales' --sensitivity_map_schema name
	,'wales' --output_schema name
	--,boundary_schema name DEFAULT 'static'::name
	--,boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	--,sensitivity_map_table name DEFAULT 'sensitivity_map'::name
	--,output_table name DEFAULT 'pressure_map'::name
	--,sar_surface_column name DEFAULT 'sar_surface'
	--,sar_subsurface_column name DEFAULT 'sar_subsurface'
	--,boundary_filter_negate boolean DEFAULT false
	--,date_end timestamp without time zone DEFAULT now()
	--,output_srid integer DEFAULT 4326
);
