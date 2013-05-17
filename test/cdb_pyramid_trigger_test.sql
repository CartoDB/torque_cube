set client_min_messages to WARNING;
create table source (id int, g geometry, t timestamp, v int);
INSERT INTO source VALUES
 (0, 'POINT(0 2047)', now(), 1),
 (1, null, now(), 1),
 (2, 'POINT(0 0)', null, 1),
 (3, 'POINT(2047 0)', now(), null)
 ;
ANALYZE source;
SELECT CDB_BuildPyramid('source', 'g', '{v}', 'CASE WHEN ($1).t < ' || quote_literal(now()) || ' THEN 0 ELSE 1 END');
SELECT 'C',st_xmax(ext),st_ymax(ext),v FROM cdb_pyramid.source
  ORDER BY res, ext;
INSERT INTO source VALUES
 (4, 'POINT(0 2047)', now(), 2),
 (5, 'POINT(0 0)', now(), 2),
 (6, 'POINT(2047 0)', now(), 2)
 ;
SELECT 'I',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
UPDATE source SET g = NULL WHERE id = 0;
SELECT 'U0',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
UPDATE source SET v = 3 WHERE id = 3;
SELECT 'U1',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
DELETE FROM source WHERE id = 1;
SELECT 'D0',st_xmax(ext) x,st_ymax(ext) y,v FROM cdb_pyramid.source
  ORDER BY res, x, y;
DROP TABLE source;
DROP TABLE cdb_pyramid.source;
