-- FUNCTION: public.bh3_remove_duplicate_geometries_table(name, name, character varying[], character varying[])

-- DROP FUNCTION public.bh3_remove_duplicate_geometries_table(name, name, character varying[], character varying[]);

CREATE OR REPLACE FUNCTION public.bh3_remove_duplicate_geometries_table(
	input_schema name,
	input_table name,
	ranking_columns character varying[],
	output_columns character varying[] DEFAULT NULL::character varying[])
    RETURNS SETOF record 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;
	pk_col name;
	geom_col name;
	col_name name;
	col_pair character varying[];
	cols_cursor refcursor;
	cols_row record;
	input_column_names character varying[] DEFAULT '{}';
	column_names character varying[] DEFAULT '{}';
	i int;
	arr_length int;
	sqlstmt text;
	ranking_list text;
	target_list text;
	target_list_qualified text;
	in_table text;

BEGIN
	BEGIN
		IF input_table IS NOT NULL THEN
			IF input_schema IS NOT NULL THEN
				in_table := quote_ident(input_schema) || '.' || quote_ident(input_table);
			ELSE
				in_table := quote_ident(input_table);
			END IF;
		ELSE
			RAISE EXCEPTION 'No input table specified'
				USING HINT = 'Please check your input_schema and input_table parameters.';
		END IF;

		IF input_schema IS NULL OR input_table IS NULL THEN
			RAISE EXCEPTION 'Invalid input table'
				USING HINT = 'Parameters input_schema and input_table are required.';
		END IF;

		IF ranking_columns IS NULL THEN
			RAISE EXCEPTION 'Parameter ranking_columns is null'
				USING HINT = 'ranking_columns must be a two-dimensional array with a second dimension length of two.';
		END IF;

		i := array_ndims(ranking_columns);
		IF i = 2 THEN
			i :=  array_length(ranking_columns, 2);
			IF i != 2 THEN
				RAISE EXCEPTION 'Length of the second dimension of array ranking_columns is %', i
					USING HINT = 'ranking_columns must be a two-dimensional array with a second dimension length of two.';
			END IF;
		ELSE
			RAISE EXCEPTION 'Array ranking_columns has % dimension(s)', i
				USING HINT = 'ranking_columns must be a two-dimensional array with a second dimension length of two.';
		END IF;

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

			input_column_names := array_append(input_column_names,cols_row.attname::character varying);

			IF cols_row.is_pk THEN
				pk_col := cols_row.attname;
			ELSIF cols_row.typname = 'geometry' THEN
				geom_col := cols_row.attname;
			ELSE
				column_names := array_append(column_names,cols_row.attname::character varying);
			END IF;
		END LOOP;

		CLOSE cols_cursor;

		ranking_list := '';

		FOREACH col_pair SLICE 1 IN ARRAY ranking_columns
		LOOP
			IF col_pair[1] ILIKE ANY(column_names) AND (col_pair[2] ILIKE 'ASC' OR col_pair[2] ILIKE 'DESC') THEN
				ranking_list := ranking_list || ',' || quote_ident(col_pair[1]) || ' ' || col_pair[2];
			ELSE
				RAISE EXCEPTION 'Table %.% does not have a column named %', input_schema, input_table, col_pair[1]
					USING HINT = 'Please check your input_schema, input_table and ranking_columns parameters.';
			END IF;
		END LOOP;

		IF length(ranking_list) > 1 THEN
			ranking_list := substring(ranking_list from 2);
		ELSE
			RAISE EXCEPTION 'Empty ranking column list'
				USING HINT = 'Please check your input_schema, input_table and ranking_columns parameters.';
		END IF;

		IF output_columns IS NULL THEN
			output_columns := column_names;
		END IF;

		col_name := '';
		target_list := '';
		target_list_qualified := '';

		i := array_ndims(output_columns);
		IF i = 1 THEN
			arr_length := array_length(output_columns, 1);
			i := 1;
			WHILE i <= arr_length 
			LOOP
				col_name := output_columns[i];

				IF col_name ILIKE ANY(column_names) THEN
					target_list := target_list || ',' || quote_ident(col_name);
					target_list_qualified := target_list_qualified || ',h.' || quote_ident(col_name);
				ELSE
					RAISE EXCEPTION 'Table %.% does not have a column named %', input_schema, input_table, col_name
						USING HINT = 'Please check your input_schema, input_table and output_columns parameters.';
				END IF;

				i := i + 1;
			END LOOP;
		END IF;

		IF length(target_list) > 1 THEN
			target_list := substring(target_list from 2);
			target_list_qualified := substring(target_list_qualified from 2);
		ELSE
			RAISE EXCEPTION 'Empty target list'
				USING HINT = 'Please check your input_schema, input_table and output_columns parameters.';
		END IF;

		sqlstmt := format(
			'WITH cte_dups AS '
			'('
				'SELECT l.%1$I'
					',ARRAY_AGG(r.%1$I) AS equal_pks '
				'FROM %2$s l '
					'JOIN %2$s r ON l.%1$I < r.%1$I AND ST_Equals(l.%3$I,r.%3$I) '
				'GROUP BY l.%1$I'
			'),'
			'cte_multi_dups AS '
			'('
				'SELECT l.%1$I '
				'FROM cte_dups l '
					'JOIN cte_dups r ON l.%1$I = ANY(r.equal_pks)'
			'),'
			'cte_join AS '
			'('
				'SELECT l.* '
				'FROM cte_dups l '
					'LEFT JOIN cte_multi_dups r ON l.%1$I = r.%1$I '
				'WHERE r.%1$I IS NULL'
			'),'
			'cte_unnest AS '
			'('
				'SELECT j.%1$I AS pk_l'
					',unnest(j.equal_pks) AS pk_r '
				'FROM cte_join j'
			'),'
			'cte_union AS '
			'('
				'SELECT u.pk_l'
					',h.%1$I'
					',h.%3$I'
					',%4$s '
				'FROM cte_unnest u '
					'JOIN %2$s h ON u.pk_l = h.%1$I '
				'UNION '
				'SELECT u.pk_l'
					',h.%1$I'
					',h.%3$I'
					',%5$s '
				'FROM cte_unnest u '
					'JOIN %2$s h ON u.pk_r = h.%1$I'
			'),'
			'cte_rank AS '
			'('
				'SELECT ROW_NUMBER() OVER('
										'PARTITION BY '
											'pk_l '
										'ORDER BY '
											'%6$s'
											',CASE WHEN %1$I = pk_l THEN 0 ELSE 1 END) AS ranking'
					',pk_l'
					',%1$I'
					',%3$I'
					',%4$s '
				'FROM cte_union'
			'),'
			'cte_unique AS '
			'('
				'SELECT %1$I'
					',%3$I'
					',%4$s '
				'FROM cte_rank '
				'WHERE ranking = 1'
			') '
			'SELECT %1$I'
				',%3$I'
				',%4$s '
			'FROM cte_unique u '
			'UNION '
			'SELECT h.%1$I'
				',h.%3$I'
				',%5$s '
			'FROM %2$s h '
			'LEFT JOIN cte_union u on h.%1$I = u.%1$I '
			'WHERE u.%1$I IS NULL '
			'ORDER BY 1',
			pk_col					-- 1
			,in_table				-- 2
			,geom_col				-- 3
			,target_list			-- 4
			,target_list_qualified	-- 5
			,ranking_list);			-- 6

    	RETURN QUERY EXECUTE sqlstmt;
		RETURN;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_remove_duplicate_geometries: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_remove_duplicate_geometries_table(name, name, character varying[], character varying[])
    OWNER TO postgres;
