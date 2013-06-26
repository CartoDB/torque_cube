set client_min_messages to WARNING;
create table source (id int, g geometry, t int, v int);
INSERT INTO source VALUES
 (0, 'POINT(0 2047)', 0, 1),
 (1, null, 1, 1),
 (2, 'POINT(0 0)', null, 1),
 (3, 'POINT(2047 0)', 3, null)
 ;
ANALYZE source;
SELECT CDB_BuildPyramid('source', 'g', '{v}', 'CASE WHEN ($1).t < 4 THEN 0 ELSE 1 END', NULL, '');
SELECT 'C',st_xmax(ext),st_ymax(ext),v FROM cdb_pyramid.source
  ORDER BY res, ext;
INSERT INTO source VALUES
 (4, 'POINT(0 2047)', 4, 2),
 (5, 'POINT(0 0)', 5, 2),
 (6, 'POINT(2047 0)', 6, 2)
 ;
SELECT 'I',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
UPDATE source SET g = NULL WHERE id = 0;
SELECT 'U0',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
UPDATE source SET v = 3 WHERE id = 3;
SELECT 'U1',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
UPDATE source SET t = 2 WHERE id = 2;
SELECT 'U2',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
DELETE FROM source WHERE id = 1;
SELECT 'D0',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;

SELECT 'MD', tab, ptab, res FROM cdb_pyramid.cdb_pyramid;

DROP TABLE source;
DROP TABLE cdb_pyramid.source;
