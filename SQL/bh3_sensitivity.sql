-- FUNCTION: public.bh3_sensitivity(sensitivity_source)

-- DROP FUNCTION public.bh3_sensitivity(sensitivity_source);

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
			SELECT t.eunis_l3_code
				,NULL::text AS eunis_l3_name
				,NULL::character varying AS characterising_species
				,t.sensitivity_ab_su_num_max AS sensitivity_ab_su_num
				,t.sensitivity_ab_ss_num_max AS sensitivity_ab_ss_num
				,t.confidence_ab_su_num
				,t.confidence_ab_ss_num
			FROM lut.sensitivity_broadscale_habitats t;
		WHEN 'eco_groups' THEN 
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,t.characterising_species
				,t.sensitivity_ab_su_num
				,t.sensitivity_ab_ss_num
				,t.confidence_ab_su_num
				,t.confidence_ab_ss_num
			FROM lut.sensitivity_eco_groups t;
		WHEN 'rock' THEN
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,t.species_name AS characterising_species
				,t.sensitivity_ab_su_num
				,NULL::smallint AS sensitivity_ab_ss_num
				,t.confidence_ab_su_num
				,NULL::smallint AS confidence_ab_ss_num
			FROM lut.sensitivity_rock t;
		WHEN 'rock_eco_groups' THEN
			RETURN QUERY 
			SELECT NULL::character varying AS eunis_l3_code
				,NULL::text AS eunis_l3_name
				,coalesce(r.species_name,e.characterising_species) AS characterising_species
				,coalesce(r.sensitivity_ab_su_num,e.sensitivity_ab_su_num) AS sensitivity_ab_su_num
				,e.sensitivity_ab_ss_num
				,coalesce(r.confidence_ab_su_num,e.confidence_ab_su_num) AS confidence_ab_su_num
				,e.confidence_ab_ss_num
			FROM lut.sensitivity_rock r
				FULL JOIN lut.sensitivity_eco_groups e ON r.species_name = e.characterising_species;
		ELSE
			RETURN;
	END CASE;
END;
$BODY$;

ALTER FUNCTION public.bh3_sensitivity(sensitivity_source)
    OWNER TO postgres;

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
