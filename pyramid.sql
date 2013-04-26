--CREATE OR REPLACE FUNCTION CDB_XYZ_PyramidTiles(tbl regclass, col text, x, y, z, r)

CREATE OR REPLACE FUNCTION CDB_XYZ_BuildPyramid(tbl regclass, col text)
RETURNS void AS
$$
DECLARE
  sql text;
  ptab text; -- pyramids table
  ppc integer; -- pixels per tilegrid cell (aka resolution)
  maxgpt integer; -- max geometries per tile
  maxz integer; -- max z levels
  z integer; -- current z level
  tblinfo RECORD;
  tile_ext_stack geometry[];
  tile_res_stack float8[];
  tile_ext geometry;
  tile_res float8;
BEGIN

  -- Setup parameters (TODO: take as param?)
  maxgpt := 65535;
  maxz := 64;
  ppc := 16;

  -- Extract table info
  WITH info AS (
    SELECT c.relname, n.nspname FROM pg_namespace n, pg_class c
    WHERE c.relnamespace = n.oid AND c.oid = tbl
  )
  SELECT nspname as nsp, relname as tab,
         st_estimated_extent(nspname, relname, col) as ext
  FROM info
  INTO tblinfo;

  RAISE DEBUG '"%"."%" ext is %', tblinfo.nsp, tblinfo.tab, tblinfo.ext;

  -- 1. Create the pyramid table 
  --ptab := quote_ident(tblinfo.nsp) || '."' || tblinfo.tab || '_xyz_pyramid' || '"';
  --sql := 'CREATE TABLE ' || ptab || '(z int, x int, y int, data numeric[], primary key (z,x,y))';
  --EXECUTE sql;

  -- 2. Start for top-level summary and add tiles to pyramid table
  --    Stop condition is when the biggest tile in the Z level has
  --    less than the max number of geometries OR when Z level is
  --    higher than the max z.
  tile_ext_stack = ARRAY[ CDB_XYZ_Extent(0,0,0) ];
  tile_res_stack = ARRAY[ CDB_XYZ_Resolution(0) * 16 ];
  WHILE array_length(tile_ext_stack, 1) LOOP

    tile_ext := tile_ext_stack[ array_upper( tile_ext_stack, 1 ) ];
    tile_ext_stack := tile_ext_stack[ 0 : array_upper(tile_ext_stack,1)-1 ];

    tile_res := tile_res_stack[ array_upper( tile_res_stack, 1 ) ];
    tile_res_stack := tile_res_stack[ 0 : array_upper(tile_res_stack,1)-1 ];

    RAISE DEBUG 'Tile extent is: %', tile_ext::box2d;
    RAISE DEBUG 'Tile resolution is: %', tile_res;

    sql := 'WITH grid AS ( SELECT CDB_RectangleGrid( '
        || quote_literal(tile_ext::text) || '::geometry,' || tile_res
        || ',' || tile_res
        || ') as cell  ), hgrid AS ( SELECT cell, (st_xmin(cell)-st_xmin('
        || quote_literal(tile_ext::text) || '::geometry)+(' || tile_res
        || '/2))/' || tile_res || ' x, (st_ymin(cell)-st_ymin('
        || quote_literal(tile_ext::text) || '::geometry)+('
        || tile_res || '/2))/' || tile_res
        || ' y FROM grid ) SELECT g.x, g.y, count(' || quote_ident(col)
        || ') v FROM hgrid g, ' || quote_ident(tblinfo.nsp) || '.'
        || quote_ident(tblinfo.tab) || ' i WHERE i.'
        || quote_ident(col) || ' && ' || quote_literal(tile_ext::text)
        || '::geometry AND ST_Intersects(i.' || quote_ident(col)
        || ', g.cell) GROUP BY g.x, g.y';

    RAISE DEBUG 'Query: %', sql;

  END LOOP;

  -- 3. Setup triggers to maintain the pyramid table

END;
$$
LANGUAGE 'plpgsql';
