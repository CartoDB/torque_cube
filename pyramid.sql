-- {
DROP FUNCTION _CDB_XYZ_BuildPyramid_Tile(tbl regclass, col text, tile_ext geometry, tile_res float8);
CREATE OR REPLACE FUNCTION _CDB_XYZ_BuildPyramid_Tile(tbl regclass, col text, tile_ext geometry, tile_res float8)
RETURNS TABLE(ext geometry, x int, y int, v int)
AS $$
DECLARE
  rec RECORD;
  sql text;
  tile_totcount integer;
BEGIN

  sql := 'WITH grid AS ( SELECT CDB_RectangleGrid( '
      || quote_literal(tile_ext::text) || '::geometry,' || tile_res
      || ',' || tile_res
      || ') as cell  ), hgrid AS ( SELECT cell, round((st_xmin(cell)-st_xmin('
      || quote_literal(tile_ext::text) || '::geometry)+(' || tile_res
      || '/2))/' || tile_res || ') x, round((st_ymin(cell)-st_ymin('
      || quote_literal(tile_ext::text) || '::geometry)+('
      || tile_res || '/2))/' || tile_res
      || ') y FROM grid ) SELECT g.cell as ext, g.x, g.y, count(' || quote_ident(col)
      || ') FROM hgrid g, ' || tbl::text || ' i WHERE i.'
      || quote_ident(col) || ' && ' || quote_literal(tile_ext::text)
      || '::geometry AND ST_Intersects(i.' || quote_ident(col)
      || ', g.cell) GROUP BY g.cell, g.x, g.y';

  RAISE DEBUG 'Query: %', sql;

  FOR rec IN  EXECUTE sql LOOP
    --RAISE DEBUG 'Count in macropixel %,%:%', rec.x, rec.y, rec.count;
    ext := rec.ext;
    x := rec.x;
    y := rec.y;
    v := rec.count;
    RETURN NEXT;
  END LOOP;

END;
$$
LANGUAGE 'plpgsql'; -- }

-- {
CREATE OR REPLACE FUNCTION CDB_XYZ_BuildPyramid(tbl regclass, col text)
RETURNS void AS
$$
DECLARE
  sql text;
  ptab text; -- pyramids table
  ppc integer; -- pixels per tilegrid cell (aka resolution)
  maxgpt integer; -- max geometries per tile
  maxz integer; -- max z levels
  tblinfo RECORD;
  tile_ext_stack geometry[];
  tile_res_stack float8[];
  tile_ext geometry;
  tile_res float8;
  tile_rast RASTER;
  rec RECORD;
  tmp_float float8;
  pixel_vals integer[];
  tile_quadcount integer[];
  qi integer;
  hew float8; -- half extent width
  heh float8; -- half extent height
  srid int;
BEGIN

  -- Setup parameters (TODO: take as param?)
  maxgpt := 65535;
  maxz := 64;
  ppc := 16;
  srid := 3857; -- TODO: derive from table/col ?

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
  ptab := quote_ident(tblinfo.nsp) || '."' || tblinfo.tab || '_pyramid' || '"';
  sql := 'CREATE TABLE ' || ptab || '(tile_ext geometry, res float8, ext geometry, x int, y int, v int)';
  BEGIN
    EXECUTE sql;
  EXCEPTION 
    WHEN OTHERS THEN
      RAISE EXCEPTION 'Got % (%)', SQLERRM, SQLSTATE;
  END;

  -- 2. Start for top-level summary and add tiles to pyramid table
  --    Stop condition is when the biggest tile in the Z level has
  --    less than the max number of geometries OR when Z level is
  --    higher than the max z.
  --tile_ext_stack = ARRAY[ CDB_XYZ_Extent(2,1,2) ];
  -- TODO: find a better starting point, using estimated extent
  tile_ext_stack = ARRAY[ CDB_XYZ_Extent(0,0,0) ];
  tile_res_stack = ARRAY[ CDB_XYZ_Resolution(0) * 16 ];
  WHILE array_length(tile_ext_stack, 1) > 0 LOOP

    tile_ext := tile_ext_stack[ array_upper( tile_ext_stack, 1 ) ];
    tile_ext_stack := tile_ext_stack[ 0 : array_upper(tile_ext_stack,1)-1 ];

    tile_res := tile_res_stack[ array_upper( tile_res_stack, 1 ) ];
    tile_res_stack := tile_res_stack[ 0 : array_upper(tile_res_stack,1)-1 ];

    RAISE DEBUG 'Tile extent is: %', tile_ext::box2d;
    RAISE DEBUG 'Tile resolution is: %', tile_res;

    tile_quadcount := ARRAY[0,0,0,0]; -- ul,ur,ll,lr
    sql := 'INSERT INTO ' || ptab
        || '(tile_ext, res, ext, x, y, v) SELECT ' || quote_literal(tile_ext::text)
        || ',' || tile_res || ', ext, x, y, v FROM _CDB_XYZ_BuildPyramid_Tile('
        || quote_literal(tbl) || ',' || quote_literal(col::text)
        || ',' || quote_literal(tile_ext::text)
        || ', ' || tile_res || ') RETURNING x,y,v';

    RAISE DEBUG '%', sql;

    FOR rec IN EXECUTE sql
    LOOP
      qi := (rec.x/8)::integer + (rec.y/8)::integer * 2;
      RAISE DEBUG 'Count in macropixel %,% (quad %) : %', rec.x, rec.y, qi, rec.v;
      tile_quadcount[qi + 1] := tile_quadcount[qi+1] + rec.v;
      -- TODO: count per-quad ?
    END LOOP;

    -- Quads:
    --   0 1
    --   2 3
    -- Stack subtiles where count > maxgpt
    IF tile_quadcount[1] > maxgpt OR
       tile_quadcount[2] > maxgpt OR
       tile_quadcount[3] > maxgpt OR
       tile_quadcount[4] > maxgpt 
    THEN
      hew := ( st_xmax(tile_ext) - st_xmin(tile_ext) ) / 2.0;
      heh := ( st_ymax(tile_ext) - st_ymin(tile_ext) ) / 2.0;
      FOR qi IN SELECT generate_series(0,3) LOOP
        RAISE DEBUG 'Count in quad %:%', qi, tile_quadcount[qi+1];
        IF tile_quadcount[qi+1] > 0 THEN 
          -- stack the four subtiles
          RAISE DEBUG 'Stacking quad %', qi;
          tile_ext_stack := tile_ext_stack || ST_MakeEnvelope(
              st_xmin(tile_ext) + hew * (qi % 2),
              st_ymin(tile_ext) + heh * (qi / 2),
              ( st_xmin(tile_ext) + hew * (qi % 2) ) + hew,
              ( st_ymin(tile_ext) + heh * (qi / 2) ) + heh,
              srid
              );
          tile_res_stack := tile_res_stack || ( tile_res / 2.0 );
        END IF;
      END LOOP;
    END IF;

  END LOOP;

  -- 3. Setup triggers to maintain the pyramid table
  --    and indices on the pyramid table


END;
$$
LANGUAGE 'plpgsql';
-- }
