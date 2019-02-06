-- FUNCTION: public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer)

-- DROP FUNCTION public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer);

CREATE OR REPLACE FUNCTION public.bh3_create_fishnet(
	num_rows integer,
	num_cols integer,
	size_x double precision,
	size_y double precision,
	origin_x double precision DEFAULT 0,
	origin_y double precision DEFAULT 0,
	srid integer DEFAULT 4326)
    RETURNS TABLE("row" integer, col integer, geom geometry) 
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE STRICT 
    ROWS 1000
AS $BODY$
BEGIN
	RETURN QUERY
	SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * size_x + origin_x, i * size_y + origin_y) AS geom
	FROM generate_series(0, num_rows - 1) AS i,
		 generate_series(0, num_cols - 1) AS j,
	(
		SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(0,0),ST_Point(size_x,size_y)),srid) AS cell
	) AS foo;
END;
$BODY$;

ALTER FUNCTION public.bh3_create_fishnet(integer, integer, double precision, double precision, double precision, double precision, integer)
    OWNER TO postgres;
