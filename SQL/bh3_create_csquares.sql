-- FUNCTION: public.bh3_create_csquares(integer, boolean, name, name, boolean, numeric, integer)

-- DROP FUNCTION public.bh3_create_csquares(integer, boolean, name, name, boolean, numeric, integer);

CREATE OR REPLACE FUNCTION public.bh3_create_csquares(
	boundary_filter integer,
	boundary_clip boolean DEFAULT false,
	boundary_schema name DEFAULT 'static'::name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
	boundary_filter_negate boolean DEFAULT false,
	cell_size_degrees numeric DEFAULT 0.05,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(
		gid bigint,
		"row" integer,
		col integer,
		the_geom geometry) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	exc_text text;
	exc_detail text;
	exc_hint text;
	negation text;
	srid_bnd int;
	geom_exp_bnd text;
	geom_exp_grid text;

BEGIN
	BEGIN
		IF boundary_filter_negate THEN
			negation = 'NOT';
		ELSE
			negation = '';
		END IF;

		srid_bnd := bh3_find_srid(boundary_schema, boundary_table, 'the_geom');
		IF srid_bnd != 4326 AND srid_bnd > 0 THEN
			geom_exp_bnd := format('ST_Transform(%1$I,%2$s)', 'the_geom', 4326);
		ELSE 
			geom_exp_bnd := 'the_geom';
		END IF;

		srid_bnd := bh3_find_srid(boundary_schema, boundary_table, 'the_geom');
		IF output_srid != 4326 THEN
			IF boundary_clip THEN
				geom_exp_grid := format('ST_Transform(ST_ClipByBox2D(bnd.the_geom,(grid_row).geom),%s)', output_srid);
			ELSE
				geom_exp_grid := format('ST_Transform((grid_row).geom,%s)', output_srid);
			END IF;
		ELSE 
			IF boundary_clip THEN
				geom_exp_grid := 'ST_ClipByBox2D(bnd.the_geom,(grid_row).geom)';
			ELSE
				geom_exp_grid := '(grid_row).geom';
			END IF;
		END IF;

		RETURN QUERY EXECUTE format(
			'WITH cte_bbox AS '
			'('
				'SELECT ST_Extent(%1$s) AS bbox'
					',ST_Union(%1$s) AS the_geom '
				'FROM %2$I.%3$I '
				'WHERE %4$s gid = $1'
			'),'
			'cte_bbox_coords AS '
			'('
				'SELECT ST_XMin(bbox) AS xmin'
					',ST_YMin(bbox) AS ymin'
					',ST_XMax(bbox) AS xmax'
					',ST_YMax(bbox) AS ymax '
				'FROM cte_bbox'
			'),'
			'cte_bbox_meas AS '
			'('
				'SELECT (xmin - xmin::numeric %% %5$s - CASE WHEN xmin < 0 THEN 0.05 else 0 END)::double precision AS xmin'
					',(ymin - ymin::numeric %% %5$s - CASE WHEN ymin < 0 THEN 0.05 else 0 END)::double precision AS ymin'
					',(xmax - xmax::numeric %% %5$s) + CASE WHEN xmax < 0 THEN 0 else 0.05 END AS xmax'
					',(ymax - ymax::numeric %% %5$s) + CASE WHEN ymax < 0 THEN 0 else 0.05 END AS ymax '
				'FROM cte_bbox_coords'
			'),'
			'cte_grid AS '
			'('
				'SELECT bh3_create_fishnet('
					'((ymax - ymin) / %5$s)::integer'
					',((xmax - xmin) / %5$s)::integer'
					',%5$s'
					',%5$s'
					',xmin'
					',ymin'
					',4326) AS grid_row '
				'FROM cte_bbox_meas'
			') '
			'SELECT ROW_NUMBER() OVER() AS gid' 
				',(grid_row).row'
				',(grid_row).col' 
				',%6$s AS the_geom '
			'FROM cte_grid g '
				'JOIN cte_bbox bnd ON ST_Intersects((grid_row).geom,bnd.the_geom)',
			geom_exp_bnd, boundary_schema, boundary_table, negation, cell_size_degrees, geom_exp_grid)
		USING boundary_filter;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_create_csquares: Exception. Message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_create_csquares(integer, boolean, name, name, boolean, numeric, integer)
    OWNER TO postgres;
