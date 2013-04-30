-- {
DROP FUNCTION _CDB_XYZ_BuildPyramid_Tile(tbl regclass, col text, tile_ext geometry, tile_res float8);
CREATE OR REPLACE FUNCTION _CDB_XYZ_BuildPyramid_Tile(tbl regclass, col text, tile_ext geometry, tile_res float8)
RETURNS TABLE(ext geometry, v int)
AS $$
DECLARE
  rec RECORD;
  sql text;
  tile_totcount integer;
BEGIN

  sql := 'WITH hgrid AS ( SELECT CDB_RectangleGrid( '
      || quote_literal(tile_ext::text) || '::geometry,' || tile_res
      || ',' || tile_res
      || ', ST_SetSRID(ST_MakePoint(st_xmin('
      || quote_literal(tile_ext::text) || '::geometry) -'
      || (tile_res/2.0) || ', st_ymin('
      || quote_literal(tile_ext::text) || '::geometry) -'
      || (tile_res/2.0) || '), ST_SRID(' || quote_literal(tile_ext::text)
      || '::geometry))) as cell ) SELECT g.cell as ext, count('
      || quote_ident(col) || ') FROM hgrid g, ' || tbl::text || ' i WHERE i.'
      || quote_ident(col) || ' && ' || quote_literal(tile_ext::text)
      || '::geometry AND ST_Intersects(i.' || quote_ident(col)
      || ', g.cell) GROUP BY g.cell';

  --RAISE DEBUG 'Query: %', sql;

  FOR rec IN  EXECUTE sql LOOP
    --RAISE DEBUG 'Count in macropixel %,%:%', rec.x, rec.y, rec.count;
    ext := rec.ext;
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
  maxpix integer; -- max pixels 
  tblinfo RECORD;
  tile_ext geometry;
  tile_res float8;
  prev_tile_res float8;
  rec RECORD;
  pixel_vals integer;
BEGIN

  -- Setup parameters 
  maxpix := 16384; -- 4096; -- 65535;

  -- Extract table info
  WITH info AS (
    SELECT gc.srid, c.relname, n.nspname FROM pg_namespace n, pg_class c, geometry_columns gc
    WHERE c.relnamespace = n.oid AND c.oid = tbl
    AND gc.f_table_schema = n.nspname AND gc.f_table_name = c.relname
    AND gc.f_geometry_column = col
  )
  SELECT nspname as nsp, relname as tab, srid,
         st_estimated_extent(nspname, relname, col) as ext
  FROM info
  INTO tblinfo;

  RAISE DEBUG '"%"."%" ext is %', tblinfo.nsp, tblinfo.tab, tblinfo.ext;

  -- 1. Create the pyramid table 
  ptab := quote_ident(tblinfo.nsp) || '."' || tblinfo.tab || '_pyramid' || '"';
  sql := 'CREATE TABLE ' || ptab || '(res float8, ext geometry, c int)';
  BEGIN
    EXECUTE sql;
  EXCEPTION 
    WHEN OTHERS THEN
      RAISE EXCEPTION 'Got % (%)', SQLERRM, SQLSTATE;
  END;

  --sql := 'SET enable_seqscan = OFF'; EXECUTE sql;

  -- 2. Start from bottom-level summary and add summarize up to top
  --    Stop condition is when we have less than maxpix "pixels"
  tile_ext := ST_SetSRID(tblinfo.ext::geometry, tblinfo.srid);
  tile_res := least(st_xmax(tile_ext)-st_xmin(tile_ext), st_ymax(tile_ext)-st_ymin(tile_ext)) / 1024;

  sql := 'INSERT INTO ' || ptab
      || '(res, ext, c) SELECT ' 
      || tile_res || ', ext, v FROM _CDB_XYZ_BuildPyramid_Tile('
      || quote_literal(tbl) || ',' || quote_literal(col::text)
      || ',' || quote_literal(tile_ext::text)
      || ', ' || tile_res || ')';

  -- RAISE DEBUG '%', sql;

  EXECUTE sql;

  GET DIAGNOSTICS pixel_vals := ROW_COUNT;

  RAISE DEBUG 'START: % pixels with resolution %', pixel_vals, tile_res;

  sql := 'CREATE INDEX ON ' || ptab || ' using gist (ext)';
  RAISE DEBUG '%', sql;
  EXECUTE sql;

  sql := 'CREATE INDEX ON ' || ptab || ' (res)';
  RAISE DEBUG '%', sql;
  EXECUTE sql;

  -- TODO: build upper levels based on lower ones
  WHILE pixel_vals > maxpix LOOP

    prev_tile_res = tile_res;
    tile_res := tile_res * 2;

    sql := 'INSERT INTO ' || ptab
      || '(res, ext, c) ' 
      || 'WITH hgrid AS ( SELECT CDB_RectangleGrid( '
      || quote_literal(tile_ext::text) || '::geometry,' || tile_res
      || ',' || tile_res
      || ', ST_SetSRID(ST_MakePoint(st_xmin('
      || quote_literal(tile_ext::text) || '::geometry) -'
      || (tile_res/2.0) || ', st_ymin('
      || quote_literal(tile_ext::text) || '::geometry) -'
      || (tile_res/2.0) || '), ST_SRID(' || quote_literal(tile_ext::text)
      || '::geometry))) as cell ) SELECT ' || tile_res
      || ', g.cell as ext, sum(c) FROM hgrid g,'
      || ptab || ' i WHERE i.res = ' || prev_tile_res
      || ' AND ST_Intersects(i.ext, g.cell) GROUP BY g.cell';

    RAISE DEBUG '%', sql;

    EXECUTE sql;

    GET DIAGNOSTICS pixel_vals := ROW_COUNT;

    RAISE DEBUG '% pixels with resolution %', pixel_vals, tile_res;

  END LOOP;

  -- Compute stats
  sql := 'ANALYZE ' || ptab;
  RAISE DEBUG '%', sql;
  EXECUTE sql;



  -- 3. Setup triggers to maintain the pyramid table
  --    and indices on the pyramid table


END;
$$
LANGUAGE 'plpgsql';
-- }
