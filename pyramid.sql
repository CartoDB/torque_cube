-- {
CREATE OR REPLACE FUNCTION _CDB_BuildPyramid_Tile(tbl regclass, col text, res float8, originX float8, originY float8, tcol text, tres integer, torigin integer)
RETURNS TABLE(ext geometry, t timestamp, c int)
AS $$
DECLARE
  rec RECORD;
  sql text;
  cell text;
  tcell text;
  tslot text;
  tile_totcount integer;
BEGIN

  cell := 'ST_SnapToGrid(' || quote_ident(col) || ', '
      || originX || ',' || originY || ','
      || res || ',' || res || ')'
      ;

  tcell := '''epoch''::timestamp + ( '
      || torigin
      || ' + round( ( extract(epoch from ' || quote_ident(tcol) || ')-'
      || torigin || ') / ' || tres || ') * ' || tres
      || ') * ''1s''::interval';

  -- TODO: use a diagonal line for the cell extent ?
  sql := 'SELECT ST_Envelope(ST_Buffer('
      || cell || ',' || (res/2.0) || ', 1)) as ext, ';
  IF tcol IS NOT NULL THEN
    sql := sql || tcell || ' as t, ';
  END IF;
  sql := sql
      || 'count('
      || quote_ident(col) || ') FROM ' || tbl::text
      || ' GROUP BY ' || cell;
  IF tcol IS NOT NULL THEN
    sql := sql || ',  t';
  END IF;

  RAISE DEBUG 'Query: %', sql;

  FOR rec IN  EXECUTE sql LOOP
    --RAISE DEBUG 'Count in macropixel %,%:%', rec.x, rec.y, rec.count;
    ext := rec.ext;
    IF tcol IS NOT NULL THEN
      t := rec.t;
    END IF;
    c := rec.count;
    RETURN NEXT;
  END LOOP;

END;
$$
LANGUAGE 'plpgsql'; -- }

-- {
CREATE OR REPLACE FUNCTION CDB_BuildPyramid(tbl regclass, col text, tcol text DEFAULT NULL)
RETURNS void AS
$$
DECLARE
  sql text;
  ptab text; -- pyramids table
  maxpix integer; -- max pixels 
  tblinfo RECORD;
  tbltinfo RECORD; -- table time info
  tile_ext geometry;
  tile_res float8;
  prev_tile_res float8;
  rec RECORD;
  pixel_vals integer;
  ntslots integer;
BEGIN

  -- Setup parameters 
  maxpix := 16383; 

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

  ntslots := 16;
  IF tcol IS NOT NULL THEN
    sql := 'SELECT min(' || quote_ident(tcol) || '), max('
      || quote_ident(tcol) || '), NULL::integer as res FROM ' || tbl;
    RAISE DEBUG '%', sql;
    EXECUTE sql INTO tbltinfo;
    tbltinfo.res := ceil(extract(epoch from tbltinfo.max - tbltinfo.min) / ntslots);

    RAISE DEBUG 'Time resolution: % seconds', tbltinfo.res;

  END IF;

  -- 1. Create the pyramid table 
  ptab := quote_ident(tblinfo.nsp) || '."' || tblinfo.tab || '_pyramid' || '"';
  sql := 'CREATE TABLE ' || ptab || '(res float8, ext geometry, tres integer, ';
  IF tcol IS NOT NULL THEN
      sql := sql || ' t timestamp,';
  END IF;
  sql := sql || 'c int)';
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
  tile_res := least(st_xmax(tile_ext)-st_xmin(tile_ext), st_ymax(tile_ext)-st_ymin(tile_ext)) / 2048; 

  -- TODO: re-compute tile_ext to always be the full webmercator extent
  --       or better yet take it as a parameter

  -- TODO: compute upper levels from lower ones, using ST_Covers

  LOOP

    sql := 'INSERT INTO ' || ptab
        || '(res, ext, t, c) SELECT '
        || tile_res || ', ext, t, c FROM _CDB_BuildPyramid_Tile('
        || quote_literal(tbl) || ',' || quote_literal(col::text)
        || ',' || tile_res || ', ' || st_xmin(tile_ext) - tile_res/2.0 || ', '
        || st_ymin(tile_ext) - tile_res/2.0;
    IF tcol IS NOT NULL THEN
        sql := sql || ', ' || quote_literal(tcol) || ','
          || tbltinfo.res || ',' || floor(extract(epoch from tbltinfo.min));
    END IF;
    sql := sql || ')';

    RAISE DEBUG '%', sql;

    EXECUTE sql;

    sql := 'SELECT count(distinct ext) FROM ' || ptab || ' WHERE res = ' || tile_res;
    EXECUTE sql INTO pixel_vals;
    --GET DIAGNOSTICS pixel_vals := ROW_COUNT;

    RAISE DEBUG '% pixels with resolution %', pixel_vals, tile_res;

    IF pixel_vals <= maxpix THEN EXIT; END IF;

    tile_res := tile_res * 2;

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
