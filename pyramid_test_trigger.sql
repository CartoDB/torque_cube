\i pyramid.sql
DROP TABLE IF EXISTS cdb_pyramid.istituti_small CASCADE;

WITH tinfo AS (
  SELECT extract(epoch from min(created_at)) as min,
         extract(epoch from max(created_at)) as max
  FROM istituti_small
)
SELECT CDB_BuildPyramid('istituti_small', 'the_geom_webmercator', '{val}',
 'floor(((extract(epoch from ($1)."created_at") - ' || min || ')/(' || max || '-' || min || '))*16)::numeric')
FROM tinfo;

-- Print summary of pixels
select count(val) as count0, sum(val) as v0 from  istituti_small;
select res, count(ext),
  sum((select sum(v) from cdb_torquepixel_dump(v,0))) as sum,
  sum((select sum(v) from cdb_torquepixel_dump(v,1))) as v
from cdb_pyramid.istituti_small
group by res order by res desc;

-- Count pixels for tile 5,4,3
SELECT count(ext) FROM cdb_pyramid.istituti_small p
 WHERE p.res = ( select max(res) from cdb_pyramid.istituti_small
                 where res < CDB_XYZ_Resolution(3) )
   AND p.ext && CDB_XYZ_Extent(5, 4, 3);

-- Fetch pixels for tile 5,4,3
SELECT
  st_xmin(ext) as x, st_ymin(ext) as y, CDB_TorquePixel_agg(v)
  FROM cdb_pyramid.istituti_small p
  WHERE p.res = ( select max(res) from cdb_pyramid.istituti_small
                 where res < CDB_XYZ_Resolution(3) )
   AND p.ext && CDB_XYZ_Extent(5, 4, 3)
  GROUP BY ext;

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'PRE', st_xmin(inp.e), st_ymin(inp.e), i.res, count(i.v)
FROM cdb_pyramid.istituti_small i, inp
WHERE i.ext && inp.e GROUP BY inp.e, res ORDER BY inp.e;

-- INSERT a row
INSERT INTO istituti_small (cartodb_id, the_geom_webmercator, created_at, val)
  VALUES ( -1, 'SRID=3857;POINT(0 0)', '2013-05-02 17:17:17', 4 );
INSERT INTO istituti_small (cartodb_id, the_geom_webmercator, created_at, val)
  VALUES ( -2, 'SRID=3857;POINT(0 1)', '2013-05-02 18:18:18', 5 );

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'INS', st_xmin(inp.e), st_ymin(inp.e), i.res, i.v
FROM cdb_pyramid.istituti_small i, inp
WHERE i.ext && inp.e ORDER BY inp.e;

-- UPDATE the row
UPDATE istituti_small SET the_geom_webmercator = 'SRID=3857;POINT(300 0)'
WHERE cartodb_id = -1;

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'UPD', st_xmin(inp.e), st_ymin(inp.e), i.res, i.v
FROM cdb_pyramid.istituti_small i, inp
WHERE i.ext && inp.e ORDER BY inp.e;

-- DELETE the rows
DELETE FROM istituti_small WHERE cartodb_id < 0;

WITH inp AS (
  SELECT ST_Buffer('SRID=3857;POINT(0 0)', 10) as e UNION ALL
  SELECT ST_Buffer('SRID=3857;POINT(300 0)', 10)
) SELECT 'DEL', st_xmin(inp.e), st_ymin(inp.e), i.res, i.v
FROM cdb_pyramid.istituti_small i, inp
WHERE i.ext && inp.e ORDER BY inp.e;

--  now with no time

DROP TABLE IF EXISTS cdb_pyramid.istituti_small CASCADE;
SELECT CDB_BuildPyramid('istituti_small', 'the_geom_webmercator', NULL, NULL);

select res, count(ext) from cdb_pyramid.istituti_small
group by res order by res desc;
