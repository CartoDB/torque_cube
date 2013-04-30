\i pyramid.sql
DROP TABLE IF EXISTS istituti_pyramid CASCADE;
SELECT CDB_XYZ_BuildPyramid('istituti', 'the_geom_webmercator');

-- Print summary of what's been constructed
select round(ST_xmin(st_extent(ext))::numeric, 0) as x0, round(st_xmax(st_extent(ext))::numeric, 0) as x1, round(st_ymin(st_extent(ext))::numeric, 0) as y0, round(st_ymax(st_extent(ext))::numeric, 0) as y1, res, sum(c), count(*)
from istituti_pyramid
group by res
order by res desc;

-- Fetch tiles for tile 1,0,1
SELECT count(*) FROM istituti_pyramid p
 WHERE p.res = ( select max(res) from istituti_pyramid
                 where res < CDB_XYZ_Resolution(1) )
   AND p.ext && CDB_XYZ_Extent(1, 0, 1);

-- Fetch tiles for tile 2,1,2
SELECT count(*) FROM istituti_pyramid p
 WHERE p.res = ( select max(res) from istituti_pyramid
                 where res < CDB_XYZ_Resolution(2) )
   AND p.ext && CDB_XYZ_Extent(2, 1, 2);

-- Fetch tiles for tile 5,4,3
SELECT count(*) FROM istituti_pyramid p
 WHERE p.res = ( select max(res) from istituti_pyramid
                 where res < CDB_XYZ_Resolution(3) )
   AND p.ext && CDB_XYZ_Extent(5, 4, 3);
