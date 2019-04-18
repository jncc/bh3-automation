-- FUNCTION: public.bh3_create_csquares(name, name, boolean, numeric, integer)

-- DROP FUNCTION public.bh3_create_csquares(name, name, boolean, numeric, integer);

CREATE OR REPLACE FUNCTION public.bh3_create_csquares(
	boundary_schema name,
	boundary_table name DEFAULT 'official_country_waters_wgs84'::name,
	boundary_clip boolean DEFAULT false,
	cell_size_degrees numeric DEFAULT 0.05,
	output_srid integer DEFAULT 4326)
    RETURNS TABLE(gid bigint, "row" integer, col integer, the_geom geometry) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
DECLARE
	exc_text text;
	exc_detail text;
	exc_hint text;
	geom_exp_grid text;

BEGIN
	BEGIN
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
			'WITH cte_bbox_coords AS '
			'('
				'SELECT ST_XMin(bbox) AS xmin'
					',ST_YMin(bbox) AS ymin'
					',ST_XMax(bbox) AS xmax'
					',ST_YMax(bbox) AS ymax '
				'FROM %1$I.%2$I'
			'),'
			'cte_bbox_meas AS '
			'('
				'SELECT (xmin - xmin::numeric %% %3$s - CASE WHEN xmin < 0 THEN 0.05 else 0 END)::double precision AS xmin'
					',(ymin - ymin::numeric %% %3$s - CASE WHEN ymin < 0 THEN 0.05 else 0 END)::double precision AS ymin'
					',(xmax - xmax::numeric %% %3$s) + CASE WHEN xmax < 0 THEN 0 else 0.05 END AS xmax'
					',(ymax - ymax::numeric %% %3$s) + CASE WHEN ymax < 0 THEN 0 else 0.05 END AS ymax '
				'FROM cte_bbox_coords'
			'),'
			'cte_grid AS '
			'('
				'SELECT bh3_create_fishnet('
					'((ymax - ymin) / %3$s)::integer'
					',((xmax - xmin) / %3$s)::integer'
					',%3$s'
					',%3$s'
					',xmin'
					',ymin'
					',4326) AS grid_row '
				'FROM cte_bbox_meas'
			') '
			'SELECT ROW_NUMBER() OVER() AS gid' 
				',(grid_row).row'
				',(grid_row).col' 
				',%4$s AS the_geom '
			'FROM cte_grid g '
				'JOIN %1$I.%2$I bnd ON ST_Intersects((grid_row).geom,bnd.the_geom)',
			boundary_schema, boundary_table, cell_size_degrees, geom_exp_grid);
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS exc_text = MESSAGE_TEXT,
								  exc_detail = PG_EXCEPTION_DETAIL,
								  exc_hint = PG_EXCEPTION_HINT;
		RAISE INFO 'bh3_create_csquares: Exception. Message: %. Detail: %. Hint: %', exc_text, exc_detail, exc_hint;
	END;
END;
$BODY$;

ALTER FUNCTION public.bh3_create_csquares(name, name, boolean, numeric, integer)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_create_csquares(name, name, boolean, numeric, integer)
    IS 'Purpose:
Creates an in-memory table of c-squares of the specified cell size within the specified polygon boundary.
The output geometries may be clipped by the boundary polygon.

Approach:
Calls the bh3_create_fishnet function to create a grid within the bounding box of the unioned boundary geometries
and returns the grid cells intersecting the boundary geometries, optionally clipping them by the boundary.

Parameters:
boundary_schema			name		Schema of table containing single AOI boundary polygon and bounding box.
boundary_table			name		Name of table containing single AOI boundary polygon and bounding box. Defaults to ''boundary''.
boundary_clip			boolean		If true grid will be clipped by boundary polygon. Defaults to false.
cell_size_degrees		numeric		Cell size in degrees. Defaults to 0.05.
output_srid				integer		SRID of output table. Defaults to 4326.

Returns:
An in-memory table of c-squares of the specified cell size within the specified polygon boundary.

Calls:
bh3_find_srid
bh3_create_fishnet';
