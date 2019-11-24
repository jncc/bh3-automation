-- FUNCTION: public.bh3_spatially_compare_tables(name, name, name, name, name, name)

-- DROP FUNCTION public.bh3_spatially_compare_tables(name, name, name, name, name, name);

CREATE OR REPLACE FUNCTION public.bh3_spatially_compare_tables(
	output_schema name,
	output_table name,
	input_schema1 name,
	input_schema2 name,
	input_table1 name DEFAULT 'disturbance_map'::name,
	input_table2 name DEFAULT 'disturbance_map'::name,
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
	where_clause text;
	cols_cursor refcursor;
	cols_row record;
	sqlstmt text;

BEGIN
	BEGIN
		success := false;
		
		IF input_schema1 IS NOT NULL AND input_schema2 IS NOT NULL AND input_table1 IS NOT NULL AND input_table2 IS NOT NULL AND output_schema IS NOT NULL AND output_table IS NOT NULL THEN
			OPEN cols_cursor FOR 
			EXECUTE format('WITH cte_l AS '
						   '('
							   'SELECT a.attnum'
								   ',a.attname'
								   ',CASE WHEN i.indexrelid IS NOT NULL THEN true ELSE false END AS is_pk'
								   ',a.atttypid'
								   ',t.typname '
							   'FROM pg_attribute a '
								   'JOIN pg_class c ON a.attrelid = c.oid '
								   'JOIN pg_namespace n ON c.relnamespace = n.oid '
								   'JOIN pg_type t ON a.atttypid = t.oid '
								   'LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND i.indisprimary '
							   'WHERE n.nspname = $1 '
								   'AND c.relname = $2 '
								   'AND a.attnum > 0 '
						  '),'
						   'cte_r AS '
						   '('
							   'SELECT a.attnum'
								   ',a.attname'
								   ',CASE WHEN i.indexrelid IS NOT NULL THEN true ELSE false END AS is_pk'
								   ',a.atttypid'
								   ',t.typname '
							   'FROM pg_attribute a '
								   'JOIN pg_class c ON a.attrelid = c.oid '
								   'JOIN pg_namespace n ON c.relnamespace = n.oid '
								   'JOIN pg_type t ON a.atttypid = t.oid '
								   'LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND i.indisprimary '
							   'WHERE n.nspname = $3 '
								   'AND c.relname = $4 '
								   'AND a.attnum > 0 '
						  ') '
						   'SELECT l.attname'
							   ',l.is_pk'
							   ',l.typname '
						   'FROM cte_l l JOIN cte_r r ON l.attname = r.attname AND l.atttypid = r.atttypid AND l.is_pk = r.is_pk '
						   'ORDER BY l.attnum')
			USING input_schema1, input_table1, input_schema2, input_table2;

			pk_col := '';
			geom_col := '';
			target_list := '';
			where_clause := '';

			LOOP
				FETCH cols_cursor INTO cols_row;
				EXIT WHEN NOT FOUND;

				IF cols_row.is_pk THEN
					pk_col := cols_row.attname;
				ELSIF cols_row.typname = 'geometry' THEN
					geom_col := cols_row.attname;
				ELSE
					target_list := target_list || format(',l.%1$I AS %2$I,r.%1$I AS %3$I', cols_row.attname, cols_row.attname || '_1', cols_row.attname || '_2');
					where_clause := where_clause || format(' OR l.%1$I != r.%1$I', cols_row.attname);
				END IF;
			END LOOP;

			CLOSE cols_cursor;

			IF length(pk_col) > 0 AND length(geom_col) > 0 AND length(target_list) > 0 AND length(where_clause) > 5 THEN
				EXECUTE format('CREATE TABLE %1$I.%2$I AS '
							   'SELECT ROW_NUMBER() OVER (ORDER BY l.%3$I,r.%3$I) AS %3$I'
								   ',l.%3$I AS %4$I'
								   ',r.%3$I AS %5$I'
								   ',l.%6$I'
								   '%7$s '
							   'FROM %8$I.%9$I l '
								   'JOIN %10$I.%11$I r ON l.%6$I && r.%6$I AND ST_Equals(l.%6$I,r.%6$I) ' 
							   'WHERE %12$s', 
							   output_schema, output_table, pk_col, pk_col || '1', pk_col || '2', 
							   geom_col, target_list, input_schema1, input_table1, input_schema2, input_table2, 
							   substring(where_clause, 5));

				EXECUTE format('ALTER TABLE %1$I.%2$I '
							   'ADD CONSTRAINT %3$I PRIMARY KEY (%4$I)',
							   output_schema, output_table, 'pk' || output_table, pk_col);

				CALL bh3_index(output_schema, output_table, 
							   ARRAY[
								   ARRAY[geom_col::character varying,'s'::character varying]
								   ,ARRAY[pk_col::character varying,'u'::character varying]
							   ]);

				success := true;
			END IF;
		END IF;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_spatially_compare_tables: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

COMMENT ON FUNCTION public.bh3_spatially_compare_tables(name, name, name, name, name, name)
    IS 'Purpose:
Spatially compares two input tables, writing records with equal geometries but at least one different attribute value in columns of corresponding name (except for the primary key column) into an output table.
The two tables must share the same schema, i.e. column names and types and primary key column, and both must be spatial tables with a single geometry column.
Usage:
SELECT * FROM public.bh3_spatially_compare_tables(
	''scot_terr_unoff_hab_unique''::name, 
	''compare_disturbance_map_scot_terr_unoff_hab_unique2''::name, 
	''scot_terr_unoff_hab_unique''::name,
	''scot_terr_unoff_hab_unique2''::name, 
	''disturbance_map''::name, 
	''disturbance_map''::name);

Approach:
Joins the two tables on their geometry columns using an internsect operator and the ST_Equals function and filters the joined set by mismatched values in columns of the corresponding names.
The mismatches are written into the output table.

Parameters:
output_schema						name					Schema of output table.
output_table						name					Name of output table.
input_schema1						name					Schema of first input table.
input_schema2						name					Schema of second input table.
input_table1						name					Name of first input table. Defaults to ''disturbance_map''.
input_table2						name					Name of second input table. Defaults to ''disturbance_map''.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_index';
