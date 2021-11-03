-- FUNCTION: public.bh3_remove_duplicate_habitats(name, name, name, name)

-- DROP FUNCTION public.bh3_remove_duplicate_habitats(name, name, name, name);

CREATE OR REPLACE FUNCTION public.bh3_remove_duplicate_habitats(
	input_schema name DEFAULT 'static'::name,
	input_table name DEFAULT 'uk_habitat_map_wgs84'::name,
	output_schema name DEFAULT 'static'::name,
	output_table name DEFAULT 'uk_habitat_map_wgs84_unique'::name,
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
	target_list text;
	cols_cursor refcursor;
	cols_row record;

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
		target_list := '';
		
		LOOP
			FETCH cols_cursor INTO cols_row;
			EXIT WHEN NOT FOUND;

			IF cols_row.is_pk THEN
				pk_col := cols_row.attname;
			ELSIF cols_row.typname = 'geometry' THEN
				geom_col := cols_row.attname;
			ELSE
				target_list := target_list || ',' || cols_row.attname;
			END IF;
		END LOOP;

		CLOSE cols_cursor;
		
		EXECUTE format('CREATE TABLE %6$I.%7$I AS '
					   'WITH cte AS '
					   '('
						   'SELECT RANK() OVER (PARTITION BY %1$I ORDER BY %2$I) AS row_rank '
							   ',%2$I'
							   ',%1$I'
							   '%3$s '
						   'FROM %4$I.%5$I'
					   ') '
					   'SELECT ROW_NUMBER() OVER(ORDER BY row_rank DESC) AS %2$I'
						   ',%2$I AS %8$I'
						   ',%1$I'
						  '%3$s '
					  'FROM cte '
					  'WHERE row_rank = 1',
					   geom_col, pk_col, target_list, input_schema, input_table, 
					   output_schema, output_table, pk_col || '_orig');

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

ALTER FUNCTION public.bh3_remove_duplicate_habitats(name, name, name, name)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_remove_duplicate_habitats(name, name, name, name)
    IS 'Purpose:
Removes duplicate geometries from the UK habitat map, creating a copy of the input table.

Approach:
Uses a window function partitioning by the input table''s geometry column and ording by its primary key column and keeping only the first record of each window.
The output table has all columns of the inpuut table plus a copy of the latter''s primary key values in an additonal column named like the original primary key column plus the suffix ''_orig''.
Usage:
SELECT * FROM public.bh3_remove_duplicate_habitats(
	''static''::name,
	''uk_habitat_map_wgs84''::name,
	''static''::name,
	''uk_habitat_map_wgs84_unique2''::name);

Parameters:
input_schema						name					Schema of the input habitat table. Defaults to ''static''.
input_table							name					Name of the input habitat table. Defaults to ''uk_habitat_map_wgs84''.
output_schema						name					Schema of the output habitat table with unique geometries. Defaults to ''static''.
output_table						name					Name of the output habitat table with unique geometries. Defaults to ''uk_habitat_map_wgs84_unique''.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_index';
