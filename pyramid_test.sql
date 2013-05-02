\i pyramid.sql
DROP TABLE IF EXISTS istituti_pyramid CASCADE;
SELECT CDB_BuildPyramid('istituti', 'the_geom_webmercator', 'created_at');

-- Print summary of pixels
select res, sum(c), count(*) from istituti_pyramid
group by res order by res desc;

-- Print summary of time slots per resolution
select res, count(distinct t) from istituti_pyramid
group by res order by res;

-- Count pixels for tile 5,4,3
SELECT count(distinct ext) FROM istituti_pyramid p
 WHERE p.res = ( select max(res) from istituti_pyramid
                 where res < CDB_XYZ_Resolution(3) )
   AND p.ext && CDB_XYZ_Extent(5, 4, 3);

-- Fetch pixels for tile 5,4,3
SELECT
  st_xmin(ext) as x, st_ymin(ext) as y,
  array_agg(extract(epoch from t)::text || ':' || c::text) as v
  FROM istituti_pyramid p
  WHERE p.res = ( select max(res) from istituti_pyramid
                 where res < CDB_XYZ_Resolution(3) )
   AND p.ext && CDB_XYZ_Extent(5, 4, 3)
  GROUP BY ext;
