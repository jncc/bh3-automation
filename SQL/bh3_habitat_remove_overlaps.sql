-- FUNCTION: public.bh3_habitat_remove_overlaps(name, name)

-- DROP FUNCTION public.bh3_habitat_remove_overlaps(name, name);

CREATE OR REPLACE FUNCTION public.bh3_habitat_remove_overlaps(
	habitat_sensitivity_schema name,
	habitat_sensitivity_table name,
	OUT success boolean,
	OUT exc_text text,
	OUT exc_detail text,
	OUT exc_hint text)
    RETURNS record
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
	start_time timestamp;
	rows_affected bigint;
	temp_table_habitat_self_join name;
	temp_table_habitat_self_join_union name;
	temp_table_habitat_unique_overlaps name;
	temp_table_habitat_overlaps_removed name;
	temp_table_habitat_overlaps_replaced name;
	error_rec record;

BEGIN
	BEGIN
		success := false;

		temp_table_habitat_self_join := 'habitat_self_join'::name;
		temp_table_habitat_self_join_union := 'habitat_self_join_union'::name;
		temp_table_habitat_unique_overlaps := 'habitat_unique_overlaps'::name;
		temp_table_habitat_overlaps_removed := 'habitat_overlaps_removed'::name;
		temp_table_habitat_overlaps_replaced := 'habitat_overlaps_replaced'::name;
		
		start_time := clock_timestamp();

		/* clean up any previous temp tables left behind */
		CALL bh3_drop_temp_table(temp_table_habitat_self_join);
		CALL bh3_drop_temp_table(temp_table_habitat_self_join_union);
		CALL bh3_drop_temp_table(temp_table_habitat_unique_overlaps);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_removed);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_replaced);

		RAISE INFO 'bh3_habitat_remove_overlaps: Removed any outputs of previous runs: %', (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* spatially self-join habitat_sensitivity intersecting overlapping geometries  
		into temporary table combining attributes from both left and right sides */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_window AS '
					   '('
						   'SELECT RANK() OVER('
											   'PARTITION BY '
												   'the_geom '
											   'ORDER BY '
												   'sensitivity_ab_su_num_max DESC,'
												   'confidence_ab_su_num DESC,'
												   'sensitivity_ab_ss_num_max DESC,'
												   'confidence_ab_ss_num DESC) AS row_rank '
							   ',gid'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num '
						   'FROM %2$I.%3$I'
					   '),'
					   'cte_unique AS '
					   '('
						   'SELECT ROW_NUMBER() OVER(ORDER BY row_rank) AS gid'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num '
						   'FROM cte_window '
						   'WHERE row_rank = 1 AND NOT ST_IsEmpty(the_geom)'
					   ') '
					   'SELECT l.gid AS gid_l'
						   ',l.hab_type AS hab_type_l'
						   ',l.eunis_l3 AS eunis_l3_l'
						   ',l.sensitivity_ab_su_num_max AS sensitivity_ab_su_num_max_l'
						   ',l.confidence_ab_su_num AS confidence_ab_su_num_l'
						   ',l.sensitivity_ab_ss_num_max AS sensitivity_ab_ss_num_max_l'
						   ',l.confidence_ab_ss_num AS confidence_ab_ss_num_l'
						   ',r.gid AS gid_r'
						   ',r.hab_type AS hab_type_r'
						   ',r.eunis_l3 AS eunis_l3_r'
						   ',r.sensitivity_ab_su_num_max AS sensitivity_ab_su_num_max_r'
						   ',r.confidence_ab_su_num AS confidence_ab_su_num_r'
						   ',r.sensitivity_ab_ss_num_max AS sensitivity_ab_ss_num_max_r'
						   ',r.confidence_ab_ss_num	AS confidence_ab_ss_num_r'
						   ',(ST_Dump('
						   'CASE '
							   'WHEN ST_Touches(l.the_geom,r.the_geom) THEN NULL '
							   'ELSE ST_Intersection(l.the_geom, r.the_geom) '
						   'END)).geom AS the_geom '
					   'FROM cte_unique l '
						   'JOIN cte_unique r ON l.gid < r.gid AND ST_Intersects(l.the_geom,r.the_geom)',
					   temp_table_habitat_self_join, habitat_sensitivity_schema, habitat_sensitivity_table);
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % rows of self joined %.% into temporary table %: %', 
			rows_affected, habitat_sensitivity_schema, habitat_sensitivity_table, temp_table_habitat_self_join, 
			(clock_timestamp() - start_time);

		start_time := clock_timestamp();
		
		/* split joined set into constituent rows where each row is 
		one overlapping geometry with its original attributes */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'SELECT gid_l AS gid'
						   ',gid_l'
						   ',gid_r'
						   ',hab_type_l AS hab_type'
						   ',eunis_l3_l AS eunis_l3'
						   ',sensitivity_ab_su_num_max_l AS sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num_l AS confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max_l AS sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num_l AS confidence_ab_ss_num'
						   ',ST_Multi(ST_Union(the_geom)) AS the_geom '
					   'FROM %2$I '
					   'WHERE the_geom IS NOT NULL '
						   'AND NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1 ' 
					   'GROUP BY gid_l'
						   ',gid_r'
						   ',hab_type_l'
						   ',eunis_l3_l'
						   ',sensitivity_ab_su_num_max_l'
						   ',confidence_ab_su_num_l'
						   ',sensitivity_ab_ss_num_max_l'
						   ',confidence_ab_ss_num_l '
					   'UNION '
					   'SELECT gid_r AS gid'
						   ',gid_l'
						   ',gid_r'
						   ',hab_type_r AS hab_type'
						   ',eunis_l3_r AS eunis_l3'
						   ',sensitivity_ab_su_num_max_r AS sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num_r AS confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max_r AS sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num_r AS confidence_ab_ss_num'
						   ',ST_Multi(ST_Union(the_geom)) AS the_geom '
					   'FROM %2$I '
					   'WHERE the_geom IS NOT NULL '
						   'AND NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1 '
					   'GROUP BY gid_l'
						   ',gid_r'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num',
					   temp_table_habitat_self_join_union, temp_table_habitat_self_join)
		USING 'Polygon';
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % split rows from temporary table % into temporary table %: %', 
			rows_affected, temp_table_habitat_self_join, temp_table_habitat_self_join_union, 
			(clock_timestamp() - start_time);

		start_time := clock_timestamp();
		
		/* repair any invalid geometries */
		CALL bh3_repair_geometries(NULL, temp_table_habitat_self_join_union);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_self_join_union, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* rank duplicate overlaps by sensitivity and confidence keeping only top ranked row */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_ranking AS '
					   '('
						   'SELECT RANK() OVER('
											   'PARTITION BY '
												   'the_geom '
											   'ORDER BY '
												   'sensitivity_ab_su_num_max DESC,'
												   'confidence_ab_su_num DESC,'
												   'sensitivity_ab_ss_num_max DESC,'
												   'confidence_ab_ss_num DESC,'
												   'CASE WHEN gid = gid_l THEN 0 ELSE 1 END) AS ranking'
							   ',gid'
							   ',gid_l'
							   ',gid_r'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num'
							   ',the_geom '
						   'FROM %2$I'
					   ') '
					   'SELECT gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
						   ',ST_Multi(ST_Union(the_geom)) AS the_geom '
					   'FROM cte_ranking '
					   'WHERE ranking = 1 AND NOT ST_IsEmpty(the_geom) '
					   'GROUP BY gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num',
					   temp_table_habitat_unique_overlaps, temp_table_habitat_self_join_union);
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % unique overlap rows ranked by sensitivity and confidence into temporary table %: %', 
			rows_affected, temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* repair any invalid geometries */
		CALL bh3_repair_geometries(NULL, temp_table_habitat_unique_overlaps);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create spatial index on temp_table_habitat_unique_overlaps */
		CALL bh3_index(NULL, temp_table_habitat_unique_overlaps, 
					   ARRAY[
						   ARRAY['the_geom','s']
					   ]);

		RAISE INFO 'bh3_habitat_remove_overlaps: Created spatial index on temporary table %: %', 
			temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* erase unique_overlaps from input table habitat_sensitivity 
		into temporary table temp_table_habitat_overlaps_removed */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_sel AS '
					   '('
						   'SELECT hab.gid'
							   ',hab.hab_type'
							   ',hab.eunis_l3'
							   ',hab.sensitivity_ab_su_num_max'
							   ',hab.confidence_ab_su_num'
							   ',hab.sensitivity_ab_ss_num_max'
							   ',hab.confidence_ab_ss_num'
							   ',(ST_Dump('
							   'CASE '
								   'WHEN uov.the_geom IS NULL THEN hab.the_geom '
								   'WHEN ST_Touches(hab.the_geom,uov.the_geom) THEN hab.the_geom '
								   'ELSE ST_Difference(hab.the_geom,uov.the_geom) '
							   'END)).geom AS the_geom '
						   'FROM %2$I.%3$I hab '
							   'LEFT JOIN %4$I uov ON hab.gid = uov.gid'
					   ') '
					   'SELECT gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
						   ',ST_Multi(ST_Union('
						   'CASE '
							   'WHEN ST_IsValid(the_geom) THEN the_geom '
							   'ELSE ST_Buffer(the_geom,0) '
						   'END)) AS the_geom '
					   'FROM cte_sel '
					   'WHERE NOT ST_IsEmpty(the_geom) '
						   'AND ST_GeometryType(the_geom) ~* $1 '
					   'GROUP BY gid'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num',
					   temp_table_habitat_overlaps_removed, habitat_sensitivity_schema, 
					   habitat_sensitivity_table, temp_table_habitat_unique_overlaps)
		USING 'Polygon';
		
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % rows with overlaps removed into temporary table %: %', 
			rows_affected, temp_table_habitat_overlaps_removed, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* repair any invalid geometries */
		CALL bh3_repair_geometries(NULL, temp_table_habitat_overlaps_removed);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Repaired % geometries in temporary table %: %', 
			rows_affected, temp_table_habitat_overlaps_removed, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* create spatial index on temp_table_habitat_overlaps_removed */
		CALL bh3_index(NULL, temp_table_habitat_overlaps_removed, 
					   ARRAY[
						   ARRAY['the_geom','s']
					   ]);

		RAISE INFO 'bh3_habitat_remove_overlaps: Created spatial index on temporary table %: %', 
			temp_table_habitat_overlaps_removed, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* merge temporary tables temp_table_habitat_overlaps_removed (input table habitat_sensitivity rows with 
		overlaps removed) and temp_table_habitat_unique_overlaps (overlaps with highest sensitivity/confidence) */
		EXECUTE format('CREATE TEMP TABLE %1$I AS '
					   'WITH cte_union AS '
					   '('
						   'SELECT gid'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num '
						   'FROM %2$I '
						   'UNION '
						   'SELECT gid'
							   ',the_geom '
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num '
						   'FROM %3$I '
						   'ORDER BY gid'
					   ') '
					   'SELECT ROW_NUMBER() OVER() AS gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num '
					   'FROM cte_union',
					   temp_table_habitat_overlaps_replaced, temp_table_habitat_overlaps_removed, 
					   temp_table_habitat_unique_overlaps);
					   
		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Inserted % rows into temporary table % from temporary tables % and %: %', 
					   rows_affected, temp_table_habitat_overlaps_replaced, temp_table_habitat_overlaps_removed, 
					   temp_table_habitat_unique_overlaps, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* empty input table */
		EXECUTE format('TRUNCATE ONLY %1$I.%2$I', habitat_sensitivity_schema, habitat_sensitivity_table);

		RAISE INFO 'bh3_habitat_remove_overlaps: Emptied input table %.%: %', 
			habitat_sensitivity_schema, habitat_sensitivity_table, (clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* copy rows from temp_table_habitat_overlaps_removed into input table */
		EXECUTE format('INSERT INTO %1$I.%2$I '
					   '('
						   'gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num'
					   ') '
					   'WITH cte_window AS '
					   '('
						   'SELECT RANK() OVER('
											   'PARTITION BY '
												   'the_geom '
											   'ORDER BY '
													'CASE WHEN ST_IsEmpty(the_geom) THEN 1 ELSE 0 END,'
													'sensitivity_ab_su_num_max DESC,'
													'confidence_ab_su_num DESC,'
													'sensitivity_ab_ss_num_max DESC,'
													'confidence_ab_ss_num DESC,'
													'gid) AS row_rank '
							   ',gid'
							   ',the_geom'
							   ',hab_type'
							   ',eunis_l3'
							   ',sensitivity_ab_su_num_max'
							   ',confidence_ab_su_num'
							   ',sensitivity_ab_ss_num_max'
							   ',confidence_ab_ss_num '
						   'FROM %3$I'
					   ') '
					   'SELECT ROW_NUMBER() OVER(ORDER BY row_rank) AS gid'
						   ',the_geom'
						   ',hab_type'
						   ',eunis_l3'
						   ',sensitivity_ab_su_num_max'
						   ',confidence_ab_su_num'
						   ',sensitivity_ab_ss_num_max'
						   ',confidence_ab_ss_num '
					   'FROM cte_window '
					   'WHERE row_rank = 1', 
					   habitat_sensitivity_schema, habitat_sensitivity_table, 
					   temp_table_habitat_overlaps_replaced);

		GET DIAGNOSTICS rows_affected = ROW_COUNT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Copied % rows from temporary table % into input table %.%: %', 
			rows_affected, temp_table_habitat_overlaps_replaced, habitat_sensitivity_schema, habitat_sensitivity_table, 
			(clock_timestamp() - start_time);

		start_time := clock_timestamp();

		/* drop temp tables */
		CALL bh3_drop_temp_table(temp_table_habitat_self_join);
		CALL bh3_drop_temp_table(temp_table_habitat_self_join_union);
		CALL bh3_drop_temp_table(temp_table_habitat_unique_overlaps);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_removed);
		CALL bh3_drop_temp_table(temp_table_habitat_overlaps_replaced);

		RAISE INFO 'bh3_habitat_remove_overlaps: Dropped temporary tables: %', (clock_timestamp() - start_time);

		success := true;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_habitat_remove_overlaps: Error message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_habitat_remove_overlaps(name, name)
    OWNER TO bh3;

COMMENT ON FUNCTION public.bh3_habitat_remove_overlaps(name, name)
    IS 'Purpose:
Removes overlapping polygons from the input habitat sensitivity table.

Approach:
First, the input habitat_sensitivity table is spatially self-joined intersecting overlapping geometries into a temporary table 
combining attributes from both left and right sides of the join. The resulting joined row set is then split into its constituent rows 
each of which is one overlapping geometry with its original attributes. Any duplicate overlaps in this split row set are ranked by 
sensitivity and confidence keeping only top ranked rows. The unique overlapping polygons in this last row set are then erased from the 
input habitat sensitivity table. Finally, the resulting set of input habitat sensitivity table rows with overlaps removed and the set of overlapping 
polygons with the highest sensitivity/confidence are merged using a union query and the resulting rows are inserted into the previously emptied 
input habitat_sensitivity table.

Parameters:
habitat_sensitivity_schema		name		Schema of the input habitat sensitivity table.
habitat_sensitivity_table		name		Name of the input habitat sensitivity table.

Returns:
A single error record. If execution succeeds its success field will be true and the remaining fields will be empty.

Calls:
bh3_drop_temp_table
bh3_repair_geometries';
