-- FUNCTION: public.bh3_safe_difference(geometry, geometry, double precision)

-- DROP FUNCTION public.bh3_safe_difference(geometry, geometry, double precision);

CREATE OR REPLACE FUNCTION public.bh3_safe_difference(
	geom_from geometry,
	geom_erase geometry,
	tolerance double precision DEFAULT (
	0.00000001)::double precision)
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
                RETURN ST_Difference(ST_Buffer(geom_from, tolerance), ST_Buffer(geom_erase, tolerance));
                EXCEPTION
                    WHEN OTHERS THEN
                        RETURN ST_GeomFromText('POLYGON EMPTY');
    END;
END
$BODY$;

ALTER FUNCTION public.bh3_safe_difference(geometry, geometry, double precision)
    OWNER TO postgres;
