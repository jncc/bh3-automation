-- FUNCTION: public.bh3_species_sensitivity_clipped(name, sensitivity_source, timestamp without time zone, timestamp without time zone, name, character varying[], boolean, integer)

-- DROP FUNCTION public.bh3_species_sensitivity_clipped(name, sensitivity_source, timestamp without time zone, timestamp without time zone, name, character varying[], boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_species_sensitivity_clipped(
	boundary_schema name,
	sensitivity_source_table sensitivity_source,
	date_start timestamp without time zone,
	date_end timestamp without time zone DEFAULT now(),
	boundary_table name DEFAULT 'boundary'::name,
	habitat_types_filter character varying[] DEFAULT NULL::character varying[],
	habitat_types_filter_negate boolean DEFAULT false,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(
		gid bigint,
		the_geom geometry,
		survey_key character varying,
		survey_event_key character varying,
		sample_reference character varying,
		sample_date timestamp with time zone,
		biotope_code character varying,
		biotope_desc text,
		qualifier character varying,
		eunis_code_2007 character varying,
		eunis_name_2007 character varying,
		annex_i_habitat character varying,
		characterising_species character varying,
		sensitivity_ab_su_num smallint,
		confidence_ab_su_num smallint,
		sensitivity_ab_ss_num smallint,
		confidence_ab_ss_num smallint) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	srid_smp int;
	geom_exp_smp text;
	eunis_corr_table_eunis_code_column name;

BEGIN
	eunis_corr_table_eunis_code_column := 'eunis_code_2007';

	srid_smp := bh3_find_srid(boundary_schema, boundary_table, 'the_geom');
	IF srid_smp != output_srid AND srid_smp > 0 AND output_srid > 0 THEN
		geom_exp_smp := format('ST_Transform(smp.%1$I,%2$s) AS %1$I', 'the_geom', output_srid);
	ELSE 
		geom_exp_smp := format('smp.%I', 'the_geom');
	END IF;

	IF habitat_types_filter IS NULL AND array_length(habitat_types_filter, 1) > 0 THEN
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
	ELSIF array_length(habitat_types_filter, 1) = 1 THEN
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
	ELSE
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
	END IF;
END;
$BODY$;

ALTER FUNCTION public.bh3_species_sensitivity_clipped(name, sensitivity_source, timestamp without time zone, timestamp without time zone, name, character varying[], boolean, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_species_sensitivity_clipped(name, sensitivity_source, timestamp without time zone, timestamp without time zone, name, character varying[], boolean, integer)
    IS 'Purpose:
Creates a table of species sensitivity rows within the specified boundary polygon(s).

Approach:
Selects rows from a hard coded join of Marine Recorder tables (marinerec.survey srv, marinerec.survey_event, marinerec.sample,
marinerec.sample_species, marinerec.sample_biotope_all) and lut.eunis_correlation_table with the specified sensitivity table within
the specified boundary polygon(s).

Parameters:
boundary_schema					name							Schema of table containing single AOI boundary polygon and bounding box.
sensitivity_source_table		sensitivity_source				Source table for habitat sensitivity scores (enum value one of { ''broadscale_habitats'', ''eco_groups'', ''rock'', ''rock_eco_groups'' }).
date_start						timestamp without time zone		Earliest date for Marine Recorder spcies samples to be included.
date_end						timestamp without time zone		Latest date for Marine Recorder species samples and pressure data to be included. Defaults to current date and time.
boundary_table					name							Name of table containing single AOI boundary polygon and bounding box. Defaults to ''boundary''.
habitat_types_filter			character varying[]				Array of eunis_l3 codes of habitats in habitat_table to be included (or excluded if habitat_types_filter_negate is true).
habitat_types_filter_negate		boolean							If true condition built with habitat_types_filter is to be negated, i.e. EUNIS L3 codes in habitat_types_filter will be excluded. Defaults to false.
output_srid						integer							SRID of output tables (reprojecting greatly affects performance). Defaults to 4326.

Returns:
An in-memory table of species sensitivity rows.

Calls:
bh3_find_srid
bh3_sensitivity';
