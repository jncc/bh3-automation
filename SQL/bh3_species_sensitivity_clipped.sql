-- FUNCTION: public.bh3_species_sensitivity_clipped(name, name, integer, sensitivity_source, timestamp without time zone, character varying[], timestamp without time zone, boolean, boolean, integer)

-- DROP FUNCTION public.bh3_species_sensitivity_clipped(name, name, integer, sensitivity_source, timestamp without time zone, character varying[], timestamp without time zone, boolean, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_species_sensitivity_clipped(
	boundary_schema name,
	boundary_table name,
	boundary_filter integer,
	sensitivity_source_table sensitivity_source,
	date_start timestamp without time zone,
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	date_end timestamp without time zone DEFAULT now(
	),
	boundary_filter_negate boolean DEFAULT false,
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
	negation text;
	geom_exp_smp text;
	habitat_type_condition text;

BEGIN
	IF boundary_filter_negate THEN
		negation = 'NOT';
	ELSE
		negation = '';
	END IF;

	srid_smp := bh3_find_srid(boundary_schema, boundary_table, 'the_geom');
	IF srid_smp != output_srid AND srid_smp > 0 AND output_srid > 0 THEN
		geom_exp_smp := format('ST_Transform(smp.%1$I,%2$s) AS %1$I', 'the_geom', output_srid);
	ELSE 
		geom_exp_smp := format('smp.%I', 'the_geom');
	END IF;

	habitat_type_condition := '';
	IF habitat_types_filter IS NOT NULL AND array_length(habitat_types_filter, 1) > 0 THEN
		IF length(habitat_types) = 1 THEN
			IF habitat_types_filter_negate THEN 
				habitat_type_condition := format('AND ect.%1$I != $5', 'eunis_code_2004', habitat_types[1]);
			ELSE
				habitat_type_condition := format('AND ect.%1$I = $5', 'eunis_code_2004', habitat_types[1]);
			END IF;
		ELSE
			IF habitat_types_filter_negate THEN
				habitat_type_condition := format('AND NOT hab.%1$I = ANY ($5)', 'eunis_code_2004');
			ELSE
				habitat_type_condition := format('AND ect.%1$I = ANY ($5)', 'eunis_code_2004');
			END IF;
		END IF;
	END IF;

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
								'WHERE %4$s bnd.gid = $2 '
									'AND smp.sample_date >= $3 '
									'AND smp.sample_date <= $4 '
									'%5$s',
								geom_exp_smp, boundary_schema, boundary_table, negation, habitat_type_condition)
	USING sensitivity_source_table, boundary_filter, date_start, date_end, CASE WHEN true THEN habitat_types_filter ELSE NULL END;
END;
$BODY$;

ALTER FUNCTION public.bh3_species_sensitivity_clipped(name, name, integer, sensitivity_source, timestamp without time zone, character varying[], timestamp without time zone, boolean, boolean, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_species_sensitivity_clipped(name, name, integer, sensitivity_source, timestamp without time zone, character varying[], timestamp without time zone, boolean, boolean, integer)
    IS 'Purpose:
Creates a table of species sensitivity rows within the specified boundary polygon(s).

Approach:
Selects rows from a hard coded join of Marine Recorder tables (marinerec.survey srv, marinerec.survey_event, marinerec.sample,
marinerec.sample_species, marinerec.sample_biotope_all) and lut.eunis_correlation_table with the specified sensitivity table within
the specified boundary polygon(s).

Parameters:
boundary_schema					name							Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table					name							Name of table containing AOI boundary polygons. Defaults to ''official_country_waters_wgs84''.
boundary_filter					integer							gid of AOI polygon in boundary_table to be included (or excluded if boundary_filter_negate is true).
sensitivity_source_table		sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
date_start						timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
habitat_types_filter			character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
date_end						timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_filter_negate			boolean							If true condition built with boundary_filter is to be negated, i.e. AOI is all but the polygon identified by boundary_filter. Defaults to false.
habitat_types_filter_negate		boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
output_srid						integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
An in-memory table of species sensitivity rows.

Calls:
bh3_find_srid
bh3_sensitivity';
