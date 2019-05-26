-- FUNCTION: public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer)

-- DROP FUNCTION public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer);

CREATE OR REPLACE FUNCTION public.bh3_boundary_subdivide(
	boundary_filter integer[],
	output_schema name,
	output_table name DEFAULT 'boundary'::name,
	output_table_subdivide name DEFAULT 'boundary_subdivide'::name,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'unofficial_country_waters_simplified_wgs84'::name,
	boundary_filter_negate boolean DEFAULT false,
	output_srid integer DEFAULT 4326,
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
	start_time timestamp;
	rows_affected bigint;
	tn name;
	srid_bnd integer;
	geom_exp_bnd character varying;
	gid_condition text;
	negation text;

BEGIN
	BEGIN
		success := false;

		start_time := clock_timestamp();

		/* clean up any previous output left behind */
		CALL bh3_drop_spatial_tables(
			ARRAY[
				ARRAY[output_schema, output_table]::name[]
				,ARRAY[output_schema, output_table_subdivide]::name[]
			]::name[][]);

		srid_bnd := bh3_find_srid(boundary_schema, boundary_table, 'the_geom'::name);
		IF srid_bnd != 4326 THEN
			geom_exp_bnd := format('ST_Transform(the_geom,%s)', 4326);
		ELSE
			geom_exp_bnd := 'the_geom';
		END IF;

		IF boundary_filter_negate THEN
			negation = 'NOT';
		ELSE
			negation = '';
		END IF;

		IF array_length(boundary_filter, 1) = 1 THEN
			EXECUTE format('CREATE TABLE %1$I.%2$I AS '
						   'WITH cte_subdiv AS '
						   '('
							   'SELECT ST_Subdivide(%3$s) AS the_geom '
							   'FROM %4$I.%5$I '
							   'WHERE %6$s gid = $1'
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',the_geom '
						   'FROM cte_subdiv',
						   output_schema, output_table_subdivide, 
						   geom_exp_bnd, boundary_schema, boundary_table, 
						   negation)
			USING boundary_filter[1];
		ELSIF array_length(boundary_filter, 1) > 1 THEN
			EXECUTE format('CREATE TABLE %1$I.%2$I AS '
						   'WITH cte_subdiv AS '
						   '('
							   'SELECT ST_Subdivide(%3$s) AS the_geom '
							   'FROM %4$I.%5$I '
							   'WHERE %6$s gid = ANY($1)'
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',the_geom '
						   'FROM cte_subdiv',
						   output_schema, output_table_subdivide, 
						   geom_exp_bnd, boundary_schema, boundary_table, 
						   negation)
			USING boundary_filter;
		ELSE
			EXECUTE format('CREATE TABLE %1$I.%2$I AS '
						   'WITH cte_subdiv AS '
						   '('
							   'SELECT ST_Subdivide(%3$s) AS the_geom '
							   'FROM %4$I.%5$I'
						   ') '
						   'SELECT ROW_NUMBER() OVER() AS gid'
							   ',the_geom '
						   'FROM cte_subdiv',
						   output_schema, output_table_subdivide, 
						   geom_exp_bnd, boundary_schema, boundary_table);
		END IF;

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Inserted % rows into table %.%: %', 
			rows_affected, output_schema, output_table_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(output_schema, output_table_subdivide);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Repaired % geometries in table %.%: %', 
			rows_affected, output_schema, output_table_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format('ALTER TABLE %1$I.%2$I ADD CONSTRAINT %2$s_pkey PRIMARY KEY(gid)', 
					    output_schema, output_table_subdivide);
		CALL bh3_index(output_schema, output_table_subdivide, 
					   ARRAY[
						   ARRAY['the_geom','s']
						   ,ARRAY['gid','u']
					   ]);

		RAISE INFO 'bh3_boundary_subdivide: Indexed table %.%: %', 
			output_schema, output_table_subdivide, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		EXECUTE format(
			'CREATE TABLE %1$I.%2$I AS '
			'SELECT ST_Extent(the_geom) AS bbox'
				',bh3_safe_union(the_geom) AS the_geom '
			'FROM %1$I.%3$I',
			output_schema, output_table, output_table_subdivide);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Inserted % rows into table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		CALL bh3_repair_geometries(output_schema, output_table);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_boundary_subdivide: Repaired % geometries in table %.%: %', 
			rows_affected, output_schema, output_table, (clock_timestamp() - start_time);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_boundary_subdivide: Error text: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_boundary_subdivide(integer[], name, name, name, name, name, boolean, integer)
    IS 'Purpose:
Selects AOI polygons from boundary_schema.boundary_table identified by boundary_filter, splits them into smaller parts with no more than 256 vertices each 
and unions them into a single AOI polygon and its bounding box.

Approach:
Calls the ST_Subdivide function to split the AOI polygons identified by boundary_filter into smaller parts with no more than 256 vertices each.
These polygons are stored in table output_schema.output_table_subdivide and then unioned into a single AOI polygon and its bounding box stored in
table output_schema.output_table.

Parameters:
boundary_filter			integer[]	Array of primary key values (gid) of AOI polygons in boundary_table to be included (or excluded if boundary_filter_negate is true).
output_schema			name		Schema in which output tables are created.
output_table			name		Name of table containing single, unioned AOI polygon and bounding box. Defaults to ''boundary''.
output_table_subdivide	name		Name of table containing split AOI polygons. Defaults to ''boundary_subdivide''.
boundary_schema			name		Schema of table containing AOI boundary polygons. Defaults to ''static''.
boundary_table			name		Name of table containing AOI boundary polygons. Defaults to ''unofficial_country_waters_simplified_wgs84''.
boundary_filter_negate	boolean		Defaults to false.
output_srid				integer		SRID of output tables. Defaults to 4326.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_find_srid
bh3_safe_union';
