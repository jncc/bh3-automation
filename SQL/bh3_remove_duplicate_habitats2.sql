-- FUNCTION: public.bh3_remove_duplicate_habitats2(name, name, name, name, name, name, name, name, name, name, name, name, name, boolean)

-- DROP FUNCTION public.bh3_remove_duplicate_habitats2(name, name, name, name, name, name, name, name, name, name, name, name, name, boolean);

CREATE OR REPLACE FUNCTION public.bh3_remove_duplicate_habitats2(
	output_schema name DEFAULT 'static'::name,
	output_table name DEFAULT 'uk_habitat_map_wgs84_unique'::name,
	input_schema name DEFAULT 'static'::name,
	input_table name DEFAULT 'uk_habitat_map_wgs84'::name,
	sensitivity_schema name DEFAULT 'lut'::name,
	sensitivity_table name DEFAULT 'sensitivity_broadscale_habitats'::name,
	column_habitat_type name DEFAULT 'hab_type'::name,
	column_eunis_l3 name DEFAULT 'eunis_l3'::name,
	column_sensitivity_eunis_l3 name DEFAULT 'eunis_l3_code'::name,
	column_sensitivity_su_num_max name DEFAULT 'sensitivity_ab_su_num_max'::name,
	column_confidence_su_num name DEFAULT 'confidence_ab_su_num'::name,
	column_sensitivity_ss_num_max name DEFAULT 'sensitivity_ab_ss_num_max'::name,
	column_confidence_ss_num name DEFAULT 'confidence_ab_ss_num'::name,
	exclude_empty_mismatched_eunis_l3 boolean DEFAULT true,
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
	pk_col name;
	geom_col name;
	cols_cursor refcursor;
	cols_row record;
	left_join character varying;

BEGIN
	BEGIN
		success := false;

		OPEN cols_cursor FOR 
		EXECUTE format('SELECT a.attname'
						   ',CASE WHEN i.indexrelid IS NOT NULL THEN true ELSE false END AS is_pk'
						   ',t.typname '
					   'FROM pg_attribute a '
						   'JOIN pg_class c ON a.attrelid = c.oid '
						   'JOIN pg_namespace n ON c.relnamespace = n.oid '
						   'JOIN pg_type t ON a.atttypid = t.oid '
						   'LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND i.indisprimary '
					   'WHERE n.nspname = $1 '
						   'AND c.relname = $2 '
						   'AND a.attnum > 0')
		USING input_schema, input_table;

		pk_col := '';
		geom_col := '';
		
		LOOP
			FETCH cols_cursor INTO cols_row;
			EXIT WHEN NOT FOUND;

			IF cols_row.is_pk THEN
				pk_col := cols_row.attname;
			ELSIF cols_row.typname = 'geometry' THEN
				geom_col := cols_row.attname;
			END IF;
		END LOOP;

		CLOSE cols_cursor;
		
		IF exclude_empty_mismatched_eunis_l3 THEN
			left_join := '';
		ELSE
			left_join := 'LEFT ';
		END IF;
		
		EXECUTE format('CREATE TABLE %1$I.%2$I AS '
					   'WITH cte_ranking AS '
					   '('
						   'SELECT RANK() OVER('
							   'PARTITION BY '
									'hab.%3$I '
							   'ORDER BY '
									'sen.%4$I DESC,'
									'sen.%5$I DESC,'
									'sen.%6$I DESC,'
									'sen.%7$I DESC'
									'hab.%8$I) AS ranking'
							   ',hab.%8$I'
							   ',hab.%3$I'
							   ',hab.%9$I'
							   ',hab.%10$I'
							   ',sen.%4$I'
							   ',sen.%5$I'
							   ',sen.%6$I'
							   ',sen.%7$I '
						   'FROM %11$I.%12$I hab '
							   '%13$s JOIN %14$I.%15$I sen ON hab.%10$I = sen.%16$I '
					   ') '
					   'SELECT %8$I'
						   ',%3$I'
						   ',%9$I'
						   ',%10$I'
						   ',%4$I'
						   ',%5$I'
						   ',%6$I'
						   ',%7$I '
					   'FROM cte_ranking '
					   'WHERE ranking = 1',
					   output_schema, output_table, geom_col,
					   column_sensitivity_su_num_max, column_confidence_su_num,
					   column_sensitivity_ss_num_max, column_confidence_ss_num,
					   pk_col, column_habitat_type, column_eunis_l3,
					   input_schema, input_table, left_join, sensitivity_schema, sensitivity_table,
					   column_sensitivity_eunis_l3);

		EXECUTE format('ALTER TABLE %1$I.%2$I '
					   'ADD CONSTRAINT %3$I PRIMARY KEY (%4$I)',
					   output_schema, output_table, 'pk' || output_table, pk_col);

		CALL bh3_index(output_schema, output_table, 
					   ARRAY[
						   ARRAY[geom_col::character varying,'s'::character varying]
						   ,ARRAY[pk_col::character varying,'u'::character varying]
					   ]);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_remove_duplicate_habitats: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_remove_duplicate_habitats2(name, name, name, name, name, name, name, name, name, name, name, name, name, boolean)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_remove_duplicate_habitats2(name, name, name, name, name, name, name, name, name, name, name, name, name, boolean)
    IS 'Purpose:
Removes duplicate geometries from the UK habitat map, keeping the instances with highest ranking sensitivity and confidence scores.
The criteria are the same as applied by bh3_remove_duplicate_habitats, but only duplicate geometries are removed, whereas 
bh3_remove_duplicate_habitats also considers overlaps.
The output is writen into a new table.

Approach:
Joins the input table to the sensitivity_table on column_eunis_l3 and column_sensitivity_eunis_l3.
The uses a window function on the join partitioning by the input table''s geometry column and ordering by the sensitivity_table''s
column_sensitivity_su_num_max, column_confidence_su_num, column_sensitivity_ss_num_max and column_confidence_ss_num and keeping only 
the first record of each window.
The output table has the primary key, geometry, column_habitat_type and column_eunis_l3 columns of the inpu table,
as well as the column_sensitivity_su_num_max, column_confidence_su_num, column_sensitivity_ss_num_max and column_confidence_ss_num of
the sensitivity_table.

Usage:
SELECT * FROM public.bh3_remove_duplicate_habitats2(
	''static''::name,
	''uk_habitat_map_wgs84_unique2''::name,
	''static''::name,
	''uk_habitat_map_wgs84''::name,
	''lut''::name,
	''sensitivity_broadscale_habitats''::name,
	''hab_type''::name,
	''eunis_l3''::name,
	''eunis_l3_code''::name,
	''sensitivity_ab_su_num_max''::name,
	''confidence_ab_su_num''::name,
	''sensitivity_ab_ss_num_max''::name,
	''confidence_ab_ss_num''::name,
	true);

Parameters:
output_schema						name					Schema of the output habitat table with unique geometries. Defaults to ''static''.
output_table						name					Name of the output habitat table with unique geometries. Defaults to ''uk_habitat_map_wgs84_unique''.
input_schema						name					Schema of the input habitat table. Defaults to ''static''.
input_table							name					Name of the input habitat table. Defaults to ''uk_habitat_map_wgs84''.
sensitivity_schema					name					Schema of sensitivity score table. Defaults to ''lut''.
sensitivity_table					name					Name of sensitivity score table. Defaults to ''sensitivity_broadscale_habitats''.
column_habitat_type					name					Name ofhabitat type column of input_table. Defaults to ''hab_type''.
column_eunis_l3						name					Name of EUNIS L3 column of input_table. Defaults to ''eunis_l3''.
column_sensitivity_eunis_l3			name					Name of EUNIS L3 column of sensitivity_table. Defaults to ''eunis_l3_code''.
column_sensitivity_su_num_max		name					Name of numeric surface sensitivity column. Defaults to ''sensitivity_ab_su_num_max''.
column_confidence_su_num			name					Name of numeric surface confidence column. Defaults to ''confidence_ab_su_num''.
column_sensitivity_ss_num_max		name					Name of numeric sub-surface sensitivity column. Defaults to ''sensitivity_ab_ss_num_max''.
column_confidence_ss_num			name					Name of numeric sub-surface confidence column. Defaults to ''confidence_ab_ss_num''.
exclude_empty_mismatched_eunis_l3	boolean					Controls whether habitats whose EUNIS L3 code is not matched in sensitivity_table are excluded (true) or included (false). Defaults to true.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_index';
