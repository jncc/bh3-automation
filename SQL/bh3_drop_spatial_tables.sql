-- PROCEDURE: public.bh3_drop_spatial_tables(name[])

-- DROP PROCEDURE public.bh3_drop_spatial_tables(name[]);

CREATE OR REPLACE PROCEDURE public.bh3_drop_spatial_tables(
	spatial_tables name[])
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	table_count integer;
	stn name[];
	exc_text character varying;
	exc_detail character varying;
	exc_hint character varying;

BEGIN
	IF spatial_tables IS NOT NULL AND array_length(spatial_tables, 1) > 0 THEN
		BEGIN
			FOREACH stn SLICE 1 IN ARRAY spatial_tables LOOP
				IF array_length(stn, 1) = 2 AND length(coalesce(stn[2], '')) > 0 THEN
					IF length(coalesce(stn[1], '')) > 0 THEN
						EXECUTE format('SELECT count(*) AS N '
									   'FROM pg_class c '
										   'JOIN pg_namespace n ON c.relnamespace = n.oid '
									   'WHERE n.nspname = $1 AND c.relname = $2') 
						INTO table_count 
						USING stn[1], stn[2];

						IF table_count = 1 THEN
							EXECUTE 'SELECT DropGeometryTable($1::text,$2::text)' USING stn[1], stn[2];
						END IF;
					ELSE
						EXECUTE format('SELECT count(*) AS N '
									   'FROM pg_class c '
										   'JOIN pg_namespace n ON c.relnamespace = n.oid '
									   'WHERE c.relname = $1') 
						INTO table_count 
						USING stn[2];

						IF table_count = 1 THEN
							EXECUTE 'SELECT DropGeometryTable($1::text)' USING stn[2];
						END IF;
					END IF;
				END IF;
			END LOOP;
		EXCEPTION
			WHEN others THEN
				GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
										  exc_detail = PG_EXCEPTION_DETAIL,
										  exc_hint = PG_EXCEPTION_HINT;
				RAISE INFO 'bh3_drop_spatial_table: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
		END;
	END IF;
END;
$BODY$;
