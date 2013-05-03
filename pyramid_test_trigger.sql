\i pyramid.sql
DROP TABLE IF EXISTS istituti_small_pyramid CASCADE;

WITH tinfo AS (
  SELECT extract(epoch from min(created_at)) as min,
         extract(epoch from max(created_at)) as max
  FROM istituti_small
), tslots AS (
  SELECT array_agg((t.min + ( (t.max-t.min) / 16 ) * i)::numeric) as slots
  FROM tinfo t, generate_series(1, 15) i
)
SELECT CDB_BuildPyramid('istituti_small', 'the_geom_webmercator', 'created_at',
                        t.slots)
FROM tslots t;

-- Print summary of pixels
-- TODO: compute actual sum with CDB_TorquePixel_dump
select res, count(ext) from istituti_small_pyramid
group by res order by res desc;

-- Count pixels for tile 5,4,3
SELECT count(ext) FROM istituti_small_pyramid p
 WHERE p.res = ( select max(res) from istituti_small_pyramid
                 where res < CDB_XYZ_Resolution(3) )
   AND p.ext && CDB_XYZ_Extent(5, 4, 3);

-- Fetch pixels for tile 5,4,3
SELECT
  st_xmin(ext) as x, st_ymin(ext) as y, CDB_TorquePixel_agg(v)
  FROM istituti_small_pyramid p
  WHERE p.res = ( select max(res) from istituti_small_pyramid
                 where res < CDB_XYZ_Resolution(3) )
   AND p.ext && CDB_XYZ_Extent(5, 4, 3)
  GROUP BY ext;

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'PRE', st_xmin(inp.e), st_ymin(inp.e), i.res, count(i.v)
FROM istituti_small_pyramid i, inp
WHERE i.ext && inp.e GROUP BY inp.e, res ORDER BY inp.e;

-- INSERT a row
INSERT INTO istituti_small (cartodb_id, the_geom_webmercator, created_at)
  VALUES ( -1, 'SRID=3857;POINT(0 0)', '2013-05-02 17:17:17' );
INSERT INTO istituti_small (cartodb_id, the_geom_webmercator, created_at)
  VALUES ( -2, 'SRID=3857;POINT(0 1)', '2013-05-02 18:18:18' );

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'INS', st_xmin(inp.e), st_ymin(inp.e), i.res, i.v
FROM istituti_small_pyramid i, inp
WHERE i.ext && inp.e ORDER BY inp.e;

-- UPDATE the row
UPDATE istituti_small SET the_geom_webmercator = 'SRID=3857;POINT(300 0)'
WHERE cartodb_id = -1;

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'UPD', st_xmin(inp.e), st_ymin(inp.e), i.res, i.v
FROM istituti_small_pyramid i, inp
WHERE i.ext && inp.e ORDER BY inp.e;

-- DELETE the rows
DELETE FROM istituti_small WHERE cartodb_id < 0;

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'DEL', st_xmin(inp.e), st_ymin(inp.e), i.res, i.v
FROM istituti_small_pyramid i, inp
WHERE i.ext && inp.e ORDER BY inp.e;

--  now with no time

DROP TABLE IF EXISTS istituti_small_pyramid CASCADE;
SELECT CDB_BuildPyramid('istituti_small', 'the_geom_webmercator', NULL, NULL);

select res, count(ext) from istituti_small_pyramid
group by res order by res desc;
