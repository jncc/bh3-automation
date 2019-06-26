-- FUNCTION: public.bh3_safe_clipbybox2d(geometry, geometry)

-- DROP FUNCTION public.bh3_safe_clipbybox2d(geometry, geometry);

CREATE OR REPLACE FUNCTION public.bh3_safe_clipbybox2d(
	geom_clip geometry,
	geom_rect geometry)
    RETURNS geometry
    LANGUAGE 'plpgsql'

    COST 100
    STABLE STRICT 
AS $BODY$
BEGIN
	BEGIN
    	RETURN ST_ClipByBox2D(geom_clip, geom_rect);
    EXCEPTION
        WHEN OTHERS THEN
            BEGIN
                RETURN ST_ClipByBox2D(geom_clip, ST_Envelope(geom_rect));
			EXCEPTION
				WHEN OTHERS THEN
					BEGIN
						RETURN ST_ClipByBox2D(ST_Buffer(geom_clip, 0.00000001), geom_rect);
					EXCEPTION
						WHEN OTHERS THEN
							BEGIN
								RETURN ST_ClipByBox2D(ST_Buffer(geom_clip, 0.00000001), ST_Envelope(geom_rect));
							EXCEPTION
								WHEN OTHERS THEN
									RETURN ST_GeomFromText('POLYGON EMPTY');
							END;
					END;
			END;
    END;
END
$BODY$;

ALTER FUNCTION public.bh3_safe_clipbybox2d(geometry, geometry)
    OWNER TO bh3;
