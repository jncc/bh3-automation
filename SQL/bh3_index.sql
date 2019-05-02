-- PROCEDURE: public.bh3_index(name, name, character varying[])

-- DROP PROCEDURE public.bh3_index(name, name, character varying[]);

CREATE OR REPLACE PROCEDURE public.bh3_index(
	schema_name name,
	table_name name,
	column_names_types character varying[])
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	sqlstmt text;
	index_name text;
	index_data record;
	index_count integer;
	cnt character varying[];
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;

BEGIN
	IF length(coalesce(table_name, '')) > 0 AND column_names_types IS NOT NULL AND array_ndims(column_names_types) = 2 THEN
		IF length(coalesce(schema_name, '')) > 0 THEN
			BEGIN
				sqlstmt := format('SELECT count(*) AS N ' 
								  'FROM pg_index i '
									  'JOIN pg_class ci ON i.indexrelid = ci.oid '
									  'JOIN pg_class c ON i.indrelid = c.oid '
									  'JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey) '
									  'JOIN pg_namespace n ON c.relnamespace = n.oid '
								  'WHERE i.indnatts = 1 ' 
									  'AND n.nspname = $1 '
									  'AND c.relname = $2 '
									  'AND a.attname = $3');

				FOREACH cnt SLICE 1 IN ARRAY column_names_types LOOP
					IF array_length(cnt, 1) = 2 THEN
						CASE cnt[2]
							WHEN 's' THEN 
								index_name := format('sidx_%1$s_%2$s', table_name, cnt[1]);
								EXECUTE sqlstmt INTO index_count USING schema_name, table_name, cnt[1];
								IF index_count = 0 THEN
									EXECUTE format('CREATE INDEX %1$I ON %2$I.%3$I USING GIST(%4$I)', index_name, schema_name, table_name, cnt[1]);
								END IF;
							WHEN 'u' THEN 
								index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
								EXECUTE sqlstmt INTO index_count USING schema_name, table_name, cnt[1];
								IF index_count = 0 THEN
									EXECUTE format('CREATE UNIQUE INDEX %1$I ON %2$I.%3$I USING BTREE(%4$I)', index_name, schema_name, table_name, cnt[1]);
								END IF;
							ELSE
								index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
								EXECUTE sqlstmt INTO index_count USING schema_name, table_name, cnt[1];
								IF index_count = 0 THEN
									EXECUTE format('CREATE INDEX %1$I ON %2$I.%3$I USING BTREE(%4$I)', index_name, schema_name, table_name, cnt[1]);
								END IF;
						END CASE;
					END IF;
				END LOOP;

				EXECUTE format('ANALYZE %1$I.%2$I', schema_name, table_name);
			EXCEPTION
				WHEN others THEN
					GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
											  exc_detail = PG_EXCEPTION_DETAIL,
											  exc_hint = PG_EXCEPTION_HINT;
					RAISE INFO 'bh3_index: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
			END;
		ELSE
			BEGIN
				sqlstmt := format('SELECT count(*) AS N ' 
								  'FROM pg_index i '
									  'JOIN pg_class ci ON i.indexrelid = ci.oid '
									  'JOIN pg_class c ON i.indrelid = c.oid '
									  'JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey) '
								  'WHERE i.indnatts = 1 ' 
									  'AND c.relname = $1 '
									  'AND a.attname = $2');

				FOREACH cnt SLICE 1 IN ARRAY column_names_types LOOP
					IF array_length(cnt, 1) = 2 THEN
						CASE cnt[2]
								WHEN 's' THEN 
									index_name := format('sidx_%1$s_%2$s', table_name, cnt[1]);
									EXECUTE sqlstmt INTO index_count USING table_name, cnt[1];
									IF index_count = 0 THEN
										EXECUTE format('CREATE INDEX %1$I ON %2$I USING GIST(%3$I)', index_name, table_name, cnt[1]);
									END IF;
								WHEN 'u' THEN 
									index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
									EXECUTE sqlstmt INTO index_count USING table_name, cnt[1];
									IF index_count = 0 THEN
										EXECUTE format('CREATE UNIQUE INDEX %1$I ON %2$I USING BTREE(%3$I)', index_name, table_name, cnt[1]);
									END IF;
								ELSE
									index_name := format('idx_%1$s_%2$s', table_name, cnt[1]);
									EXECUTE sqlstmt INTO index_count USING table_name, cnt[1];
									IF index_count = 0 THEN
										EXECUTE format('CREATE INDEX %1$I ON %2$I USING BTREE(%3$I)', index_name, table_name, cnt[1]);
									END IF;
						END CASE;
					END IF;
				END LOOP;

				EXECUTE format('ANALYZE %1$I', table_name);
			EXCEPTION
				WHEN others THEN
					GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
											  exc_detail = PG_EXCEPTION_DETAIL,
											  exc_hint = PG_EXCEPTION_HINT;
					RAISE INFO 'bh3_index: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
			END;
		END IF;
	END IF;
END;
$BODY$;

COMMENT ON PROCEDURE public.bh3_index
    IS 'Purpose:
Indexes one or more columns of a table and rec-omputes its statistics. 

Approach:
Creates index(es) with name(s) composed of table and column names if they do not already exist.
Calls analyze to re-compute table statistics. 

Parameters:
schema_name			name					Schema name of table to be indexed.
table_name			name					Table name of table to be indexed.
column_names_types	character varying[][]	Two dimensional array of columns to be indexed. Each column is represented by an array of length two. 
			  								The first element is the column name; the second is the index type. 
			  								Valid types are, where ''s'' for spatial (GIST) and ''u'' for unique (BTREE).
			  								Any other value will be interpreted as requesting a non-unique BTREE index.

Calls:
No nested calls';
