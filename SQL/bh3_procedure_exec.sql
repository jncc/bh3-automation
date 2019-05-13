CALL public.bh3_procedure(
	ARRAY[0]::integer[]							--boundary_filter integer
	,NULL::character varying[]					--habitat_types_filter character varying[]
	,'2012-01-01'::timestamp					--date_start timestamp without time zone
	,'rock_eco_groups'::sensitivity_source		--species_sensitivity_source_table sensitivity_source
	,'ices_abrasion'::name						--pressure_schema name
	,'wales'::name								--output_schema name
	,'bh3'::character varying					--output_owner character varying DEFAULT NULL::character varying,
	,'static'::name								--boundary_schema name DEFAULT 'static'::name
	,'official_country_waters_wgs84'::name		--boundary_table name DEFAULT 'official_country_waters_wgs84'::name
	,'static'::name								--habitat_schema name DEFAULT 'static'::name
	,'uk_habitat_map_wgs84'::name				--habitat_table name DEFAULT 'uk_habitat_map_wgs84'::name
	,'lut'::name								--habitat_sensitivity_lookup_schema name DEFAULT 'lut'::name
	,'sensitivity_broadscale_habitats'::name	--habitat_sensitivity_lookup_table name DEFAULT 'sensitivity_broadscale_habitats'::name
	,'habitat_sensitivity'::name				--habitat_sensitivity_table name DEFAULT 'habitat_sensitivity'::name
	,'habitat_sensitivity_grid'::name			--habitat_sensitivity_grid_table name DEFAULT 'habitat_sensitivity_grid'::name
	,'species_sensitivity_max'::name			--species_sensitivity_max_table name DEFAULT 'species_sensitivity_max'::name
	,'species_sensitivity_mode'::name			--species_sensitivity_mode_table name DEFAULT 'species_sensitivity_mode'::name
	,'sensitivity_map'::name					--sensitivity_map_table name DEFAULT 'sensitivity_map'::name
	,'pressure_map'::name						--pressure_map_table name DEFAULT 'pressure_map'::name,
	,'disturbance_map'::name					--disturbance_map_table name DEFAULT 'disturbance_map'::name
	,'sar_surface'::name						--sar_surface_column name DEFAULT 'sar_surface'::name
	,'sar_subsurface'::name						--sar_subsurface_column name DEFAULT 'sar_subsurface'::name
	,now()::timestamp							--date_end timestamp without time zone DEFAULT now()
	,false										--boundary_filter_negate boolean DEFAULT false
	,false										--habitat_types_filter_negate boolean DEFAULT false
	,false										--remove_overlaps boolean DEFAULT false
	,4326										--output_srid integer DEFAULT 4326
);
