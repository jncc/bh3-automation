-- FUNCTION: public.bh3_safe_union_transfn(geometry, geometry)

-- DROP FUNCTION public.bh3_safe_union_transfn(geometry, geometry);

CREATE OR REPLACE FUNCTION public.bh3_safe_union_transfn(
	agg_state geometry,
	el geometry)
    RETURNS geometry
    LANGUAGE 'plpgsql'
    COST 100
    IMMUTABLE PARALLEL UNSAFE
AS $BODY$
DECLARE
	g geometry;
BEGIN
	IF agg_state IS NULL THEN
    	RETURN el;
	ELSIF el IS NOT NULL THEN
		BEGIN
			g := ST_Union(agg_state, el);
			IF ST_Area(g) > 0 THEN
				RETURN g;
			ELSE
				RETURN ST_GeomFromText('POLYGON EMPTY');
			END IF;
		EXCEPTION
			WHEN OTHERS THEN
				BEGIN
					RETURN ST_Union(ST_Buffer(agg_state, 0.00000001), ST_Buffer(el, 0.00000001));
				EXCEPTION
					WHEN OTHERS THEN
						RETURN ST_GeomFromText('POLYGON EMPTY');
				END;
		END;
	ELSE
		RETURN ST_GeomFromText('POLYGON EMPTY');
	END IF;
END;
$BODY$;


ALTER FUNCTION public.bh3_safe_union_transfn(geometry, geometry)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_safe_union_transfn(geometry, geometry)
    IS 'Purpose:
State transition function for bh3_safe_union aggregate, which is a wrapper around ST_Union with exception handling.

Approach:
Calls ST_Union on the unaltered input parameter. 
If it throws an exception it is called again on the input geometry buffered by 0.00000001.
If that also fails an empty polygon is returned.

Parameters:
agg_state	geometry	State object. ST_Union of input geometries.
el	geometry	Input geometry.

Returns:
ST_Union of the (possibly buffered) geometries, or if that fails an empty polygon.

Calls:
No nested calls.';



--DROP AGGREGATE IF EXISTS bh3_safe_union(geometry);

CREATE AGGREGATE public.bh3_safe_union(geometry)
(
    sfunc = public.bh3_safe_union_transfn,
    stype = geometry
);