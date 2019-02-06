-- PROCEDURE: public.bh3_drop_temp_table(name)

-- DROP PROCEDURE public.bh3_drop_temp_table(name);

CREATE OR REPLACE PROCEDURE public.bh3_drop_temp_table(
	table_name name)
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	temp_table name;

BEGIN
	IF length(coalesce(table_name, '')) > 0 THEN
		EXECUTE format('DROP TABLE IF EXISTS %1$I', table_name);

		EXECUTE format('SELECT quote_ident(n.nspname) || ''.'' || quote_ident(c.relname) '
					   'FROM pg_class c '
						   'JOIN pg_namespace n ON c.relnamespace = n.oid '
					   'WHERE n.nspname ~* $1 AND c.relname = $2'
		)
		INTO temp_table
		USING 'pg_temp', table_name;

		IF length(coalesce(temp_table,'')) > 0 THEN
			EXECUTE 'DROP TABLE ' || temp_table;
		END IF;
	END IF;
END;
$BODY$;
