CREATE TABLE static.unofficial_country_waters_simplified_wgs84_holes
(
	gid serial NOT NULL,
	the_geom geometry(MultiPolygon,4326),
	adminarea character varying(100) COLLATE pg_catalog."default",
	CONSTRAINT unofficial_country_waters_simplified_wgs84_holes_pkey PRIMARY KEY (gid)
);

CREATE UNIQUE INDEX idx_unofficial_country_waters_simplified_wgs84_holes_gid
	ON static.unofficial_country_waters_simplified_wgs84_holes USING btree
	(gid)
	TABLESPACE pg_default;

CREATE INDEX sidx_unofficial_country_waters_simplified_wgs84_holes_the_geom
	ON static.unofficial_country_waters_simplified_wgs84_holes USING gist
	(the_geom)
	TABLESPACE pg_default;


WITH cte_polygons AS
(
	SELECT gid, (ST_Dump(the_geom)).geom AS the_geom, adminarea, shape_leng, shape_area
	FROM static.official_country_waters_wgs84
),
cte_rings AS
(
	SELECT gid, ST_DumpRings(the_geom) AS geom_dump, adminarea, shape_leng, shape_area
	FROM cte_polygons
),
cte_unofficial_country_waters_simplified_wgs84_holes AS
(
	SELECT ST_Union((geom_dump).geom) AS the_geom
	FROM cte_rings
	WHERE (geom_dump).path[1] > 0
)
INSERT INTO static.unofficial_country_waters_simplified_wgs84_holes
(
	gid
	,the_geom
	,adminarea
)
SELECT u.gid
	,ST_Multi(ST_MakeValid(ST_Difference(u.the_geom, h.the_geom))) AS the_geom
	,u.adminarea
FROM static.unofficial_country_waters_simplified_wgs84 u, cte_unofficial_country_waters_simplified_wgs84_holes h
ORDER BY u.gid;