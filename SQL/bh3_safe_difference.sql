-- FUNCTION: public.bh3_safe_difference(geometry, geometry)

-- DROP FUNCTION public.bh3_safe_difference(geometry, geometry);

CREATE OR REPLACE FUNCTION public.bh3_safe_difference(
	geom_from geometry,
	geom_erase geometry)
    RETURNS geometry
    LANGUAGE 'plpgsql'

    COST 100
    STABLE STRICT 
AS $BODY$
BEGIN
    RETURN ST_Difference(geom_from, geom_erase);
    EXCEPTION
        WHEN OTHERS THEN
            BEGIN
                RETURN ST_Difference(ST_Buffer(geom_from, 0.0000001), ST_Buffer(geom_erase, 0.0000001));
                EXCEPTION
                    WHEN OTHERS THEN
                        RETURN ST_GeomFromText('POLYGON EMPTY');
    END;
END
$BODY$;

ALTER FUNCTION public.bh3_safe_difference(geometry, geometry)
    OWNER TO postgres;

COMMENT ON FUNCTION public.bh3_safe_difference(geometry, geometry)
    IS 'Purpose:
Wrapper around ST_Difference with exception handling.

Approach:
Calls ST_Difference on the unaltered input parameters. 
If it throws an exception it is called again on the input geometries buffered by 0.0000001.
If that also fails an empty polygon is returned.

Parameters:
geom_from	geometry	Geometry from which to erase geom_erase (first parameter passed into ST_Difference).
geom_erase	geometry	Geometry to erase from geom_from (second parameter passed into ST_Difference).

Returns:
ST_Difference of the two (possibly buffered) geometries, or if that fails an empty polygon.

Calls:
No nested calls.';
