-- FUNCTION: public.bh3_safe_union(geometry, double precision)

-- DROP FUNCTION public.bh3_safe_union(geometry, double precision);

CREATE OR REPLACE FUNCTION public.bh3_safe_union(
	geom geometry,
	tolerance double precision DEFAULT (
	0.00000001)::double precision)
    RETURNS geometry
    LANGUAGE 'plpgsql'

    COST 100
    STABLE STRICT 
AS $BODY$
BEGIN
    RETURN ST_Union(geom);
    EXCEPTION
        WHEN OTHERS THEN
            BEGIN
                RETURN ST_Union(ST_Buffer(geom, tolerance));
                EXCEPTION
                    WHEN OTHERS THEN
                        RETURN ST_GeomFromText('POLYGON EMPTY');
    END;
END
$BODY$;

ALTER FUNCTION public.bh3_safe_union(geometry, double precision)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_safe_union(geometry, double precision)
    IS 'Purpose:
Wrapper around ST_Union with exception handling.

Approach:
Calls ST_Union on the unaltered input parameter. 
If it throws an exception it is called again on the input geometry buffered by tolerance.
If that also fails an empty polygon is returned.

Parameters:
geom	geometry	Geometry from which to erase geom_erase (first parameter passed into ST_Difference).
tolerance	double precision	Distance by which geometry will be buffered if ST_Union fails).

Returns:
ST_Union of the (possibly buffered) geometrys, or if that fails an empty polygon.

Calls:
No nested calls.';
