-- Add the values of a torque pixel (what) to another (target)
--
-- Pixel are in this form:
--  <num_timeslots> [<t1>,<tN>] <f1t1>,<f1tN> [<f2t1>,<f2tN>]
--
--  {
CREATE OR REPLACE FUNCTION CDB_TorquePixel_add(target numeric[], what numeric[])
RETURNS numeric[] AS
$$
DECLARE
  i integer;
  si integer;
  ti integer;
  tvaloff integer;
  svaloff integer;
  tslot integer;
  ntslots integer;
  ntarget numeric[];
BEGIN

  -- Make aggregate friendly
  IF target IS NULL THEN RETURN what; END IF;
  IF what IS NULL THEN RETURN target; END IF;

  ntslots := target[1];
  svaloff := 2 + what[1];

  IF what[1] > 1 THEN
    RAISE EXCEPTION 'Multi time slots source pixel value unsupported';
  END IF;

  IF ntslots > 0 THEN
    -- target has time slots, find our ones
    IF what[1] != 1 THEN
      RAISE EXCEPTION 'Cannot increment a time-based pixel value by a timeless value';
    END IF;
    tslot := what[2];
    FOR i IN 2..target[1]+1 LOOP
      IF target[i] = tslot THEN
        ntarget := target; 
        tvaloff := ntslots + i;
        EXIT;
      END IF;
    END LOOP;
    IF tvaloff IS NULL THEN
      -- need to make space for the new value
      ntarget := ARRAY[ target[1] + 1 ] || target[2:1+ntslots] || ARRAY[tslot::numeric];
      FOR i IN 0..(array_upper(what, 1) - svaloff) LOOP
        ti := 2 + ntslots + ( ntslots * i );
        ntarget := ntarget || target[ti:(ti+(ntslots-1))] || ARRAY[ 0::numeric ];
      END LOOP;
      tvaloff := 1 + 2 * ntarget[1];
      ntslots := ntslots + 1;
    END IF;
  ELSE
    -- target has no time slots
    IF what[1] != 0 THEN
      RAISE WARNING 'Discarding time slot from value (target is not time-based)';
    END IF;
    ntarget := target; 
    tvaloff := 2;
  END IF;

  -- RAISE DEBUG 'Source: % (val off %)', what, svaloff;
  -- RAISE DEBUG 'Target: %', target;
  -- RAISE DEBUG 'Ntarget: % (val off %, ntslots %)', ntarget, tvaloff, ntslots;
  -- RAISE DEBUG 'Value offsets: s=% t=%', svaloff, tvaloff;

  -- TODO: add each value of source to value of target
  FOR i IN 0..(array_upper(what, 1) - svaloff) LOOP
    si := svaloff+i;
    ti := tvaloff+(i*COALESCE(NULLIF(ntslots,0),1)); 
    --RAISE DEBUG 'Adding s:% (%) to t:% (%)', si, what[si], ti, ntarget[ti];
    ntarget[ti] := ntarget[ti] + what[si];
  END LOOP;

  --RAISE DEBUG '- %', target;
  --RAISE DEBUG '+ %', ntarget;

  RETURN ntarget;

END;
$$
LANGUAGE 'plpgsql';
-- }

DROP AGGREGATE IF EXISTS CDB_TorquePixel_agg (numeric[]);
CREATE AGGREGATE CDB_TorquePixel_agg (numeric[])
(
  sfunc = CDB_TorquePixel_add,
  stype = numeric[]
);

-- Delete the value of a torque pixel (what) from another (target)
--
-- Pixel are in this form:
--  <num_timeslots> [<t1>,<tN>] <f1t1>,<f1tN> [<f2t1>,<f2tN>]
--
--  {
CREATE OR REPLACE FUNCTION CDB_TorquePixel_del(target numeric[], what numeric[])
RETURNS numeric[] AS
$$
DECLARE
  i integer;
  si integer;
  ti integer;
  tvaloff integer;
  svaloff integer;
  tslot integer;
  ntslots integer;
  ntarget numeric[];
BEGIN
  ntslots := target[1];
  svaloff := 2 + what[1];

  IF what[1] > 1 THEN
    RAISE EXCEPTION 'Multi time slots source pixel value unsupported';
  END IF;

  IF ntslots > 0 THEN
    -- target has time slots, find our ones
    IF what[1] != 1 THEN
      RAISE EXCEPTION 'Cannot delete a timeless value from a time-based pixel';
    END IF;
    tslot := what[2];
    FOR i IN 2..target[1]+1 LOOP
      IF target[i] = tslot THEN
        ntarget := target; 
        tvaloff := ntslots + i;
        EXIT;
      END IF;
    END LOOP;
    IF tvaloff IS NULL THEN
      -- nothing to do here (unexpected?)
      RAISE WARNING 'Source pixel timeslot not found in target';
      return target;
    END IF;
  ELSE
    -- target has no time slots
    IF what[1] != 0 THEN
      RAISE WARNING 'Discarding time slot from value (target is not time-based)';
    END IF;
    ntarget := target; 
    tvaloff := 2;
  END IF;

  --RAISE DEBUG 'Source: % (val off %)', what, svaloff;
  --RAISE DEBUG 'Target: %', target;
  --RAISE DEBUG 'Ntarget: % (val off %, ntslots %)', ntarget, tvaloff, ntslots;
  --RAISE DEBUG 'Value offsets: s=% t=%', svaloff, tvaloff;

  -- TODO: add each value of source to value of target
  FOR i IN 0..(array_upper(what, 1) - svaloff) LOOP
    si := svaloff+i;
    ti := tvaloff+(i*COALESCE(NULLIF(ntslots,0),1)); 
    --RAISE DEBUG 'Adding s:% (%) to t:% (%)', si, what[si], ti, ntarget[ti];
    ntarget[ti] := ntarget[ti] - what[si];
  END LOOP;

  --RAISE DEBUG '- %', target;
  --RAISE DEBUG '+ %', ntarget;

  RETURN ntarget;

END;
$$
LANGUAGE 'plpgsql' STRICT;
-- }

-- Return set of values for given colun in pixel, indexed by timeslot
-- @param col 0-based attribute index
CREATE OR REPLACE FUNCTION CDB_TorquePixel_dump(pixel numeric[], col integer)
RETURNS TABLE (t numeric, v numeric)
AS
$$
DECLARE
  i integer;
  vi integer;
  nslots integer;
BEGIN
  nslots := COALESCE(NULLIF(pixel[1],0),1);
  RAISE DEBUG 'p:% -- nslots:%', pixel, nslots;
  FOR i IN 1..nslots LOOP
    RAISE DEBUG 'Index %', i;
    t := CASE WHEN pixel[1] = 0 THEN NULL ELSE pixel[1+i] END;
    vi := 2 + pixel[1] + col * nslots + (i-1);
    RAISE DEBUG 't: %', t;
    RAISE DEBUG 'vi: %', vi;
    v := pixel[vi];
    RETURN NEXT;
  END LOOP;
END;
$$
LANGUAGE 'plpgsql' STRICT;

-- {
CREATE OR REPLACE FUNCTION CDB_BuildPyramid(tbl regclass, col text, tcol text, temporal_bins numeric[])
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
  resolutions float8[];
BEGIN

  -- Setup parameters  (higher number to stop first)
  maxpix := 65535; -- 256x256 are enough

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
  sql := 'CREATE TABLE ' || ptab || '(res float8, ext geometry, tres integer, ';
  sql := sql || 'v numeric[])';
  EXECUTE sql;

  --sql := 'SET enable_seqscan = OFF'; EXECUTE sql;

  -- 2. Start from bottom-level summary and add summarize up to top
  --    Stop condition is when we have less than maxpix "pixels"
  tile_ext := ST_SetSRID(tblinfo.ext::geometry, tblinfo.srid);
  tile_res := least(st_xmax(tile_ext)-st_xmin(tile_ext), st_ymax(tile_ext)-st_ymin(tile_ext))
        / (256*8); -- enough to render 8 tiles per side with no loss of precision
  -- TODO: round resolution to be on the webmercator resolution set ?

  -- TODO: re-compute tile_ext to always be the full webmercator extent
  --       or better yet take it as a parameter

  -- TODO: compute upper levels from lower ones, using ST_Covers

  LOOP

    resolutions := resolutions || tile_res;

    sql := 'WITH pixels AS ( SELECT '
        || tile_res
        || ', ST_Envelope(ST_Buffer('
        || 'ST_SnapToGrid(' || quote_ident(col) || ', '
        || st_xmin(tile_ext) - tile_res/2.0 || ','
        || st_ymin(tile_ext) - tile_res/2.0 || ','
        || tile_res || ',' || tile_res || ')'
        || ',' || (tile_res/2.0) || ', 1)) as ext, '
        || 'CDB_TorquePixel_agg(ARRAY['
        ;
    IF tcol IS NOT NULL THEN
      sql := sql
        || '1::numeric,CASE';
      FOR i IN 1..array_upper(temporal_bins, 1) LOOP
        sql := sql
          || ' WHEN extract(epoch from ' || quote_ident(tcol) || ') < '
          || temporal_bins[i] || ' THEN ' || (i-1);
      END LOOP;
      sql := sql || 'ELSE ' || array_upper(temporal_bins, 1)
        || ' END';
    ELSE
      sql := sql || '0::numeric';
    END IF;
    sql := sql
        || ', 1'
        -- TODO: add more field summaries
        || ']) as v FROM ' || tbl::text
        || ' GROUP BY ext'; 
    sql := sql || '), ins AS ( INSERT INTO ' || ptab
      || '(res, ext, v) SELECT '
      || tile_res || ', ext, v FROM pixels ) SELECT count(distinct ext) FROM pixels ';

    RAISE DEBUG '%', sql;

    EXECUTE sql INTO pixel_vals;

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
  sql := 'DROP TRIGGER IF EXISTS cdb_maintain_pyramid ON ' || tbl;
  RAISE DEBUG '%', sql;
  EXECUTE sql;

  sql := 'CREATE TRIGGER cdb_maintain_pyramid AFTER INSERT OR UPDATE OR DELETE ON '
    || tbl || ' FOR EACH ROW EXECUTE PROCEDURE _CDB_PyramidTrigger('
    || col || ',' || COALESCE(tcol, quote_literal('null')) || ',' || quote_literal(ptab) || ','
    || quote_literal(tile_ext::text) || ','
    || quote_literal(resolutions::text) || ','
    || quote_literal(COALESCE(temporal_bins, '{}')::text) || ')';
  RAISE DEBUG 'TRIGGER CREATION: %', sql;
  EXECUTE sql;


END;
$$
LANGUAGE 'plpgsql';
-- }


CREATE OR REPLACE FUNCTION _CDB_PyramidTrigger()
RETURNS TRIGGER AS
$$
DECLARE
  gcol text;
  tcol text;
  ptab text;
  full_extent geometry;
  resolutions float8[];
  res float8;
  temporal_bins numeric[];
  sql text;
  g geometry;
  i integer;
  originX float8;
  originY float8;
  oldinfo RECORD;
  newinfo RECORD;
BEGIN

  IF TG_NARGS < 6 THEN
    RAISE EXCEPTION 'Illegal call to _CDB_PyramidTrigger (need 6 args, got %)', TG_NARGS;
  END IF;

  gcol := TG_ARGV[0];
  tcol := TG_ARGV[1];
  ptab := TG_ARGV[2];
  full_extent := TG_ARGV[3];
  resolutions := TG_ARGV[4];
  temporal_bins := TG_ARGV[5];

  sql := 'SELECT ($1).' || quote_ident(gcol) || ' as g';
  IF tcol IS NOT NULL THEN
    sql := sql || ', ARRAY[1::numeric, CASE ';
    FOR i IN 1..array_upper(temporal_bins, 1) LOOP
      sql := sql || ' WHEN extract(epoch from ($1).'
        || quote_ident(tcol) || ') < '
        || temporal_bins[i] || ' THEN ' || (i-1);
    END LOOP;
    sql := sql || 'ELSE ' || array_upper(temporal_bins, 1) || ' END, 1] as v';
  END IF;

  -- Extract info from NEW record
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    EXECUTE sql USING NEW INTO newinfo;
  END IF;

  -- Extract info from OLD record
  IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
    EXECUTE sql USING OLD INTO oldinfo;
  END IF;

  FOR i IN 1..array_upper(resolutions,1) LOOP
    res := resolutions[i];
    RAISE DEBUG ' updating resolution %', res;
    originX := st_xmin(full_extent) - res/2.0;
    originY := st_ymin(full_extent) - res/2.0;

    IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
      -- decrement
      g := ST_SnapToGrid(oldinfo.g, originX, originY, res, res);
      RAISE DEBUG ' resolution % : % @ %', res, ST_AsText(g), oldinfo.v;
      -- Updel
      sql := 'UPDATE ' || ptab || ' set v = CDB_TorquePixel_del(v, '
        || quote_literal(oldinfo.v) || ') WHERE ext && '
        || quote_literal(oldinfo.g::text);
      RAISE DEBUG ' %', sql;
      EXECUTE sql;
    END IF;

    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
      -- increment
      g := ST_SnapToGrid(newinfo.g, originX, originY, res, res);
      RAISE DEBUG ' resolution % : % @ %', res, ST_AsText(g), newinfo.v;
      -- Upsert
      sql := 'WITH upsert as (UPDATE ' || ptab || ' set v=CDB_TorquePixel_add(v, '
        || quote_literal(newinfo.v) 
        || ') WHERE ext && ' || quote_literal(newinfo.g::text)
        || ' RETURNING ext ) INSERT INTO '
        || ptab || '(res,ext,v) SELECT ' || res || ', ST_Envelope(ST_Buffer('
        || quote_literal(newinfo.g::text) || ',' || (res/2.0) || ', 1)), '
        || quote_literal(newinfo.v)
        || ' WHERE NOT EXISTS (SELECT * FROM upsert)'; 
      RAISE DEBUG ' %', sql;
      EXECUTE sql;
    END IF;
  END LOOP;

  RETURN NULL;
END;
$$
LANGUAGE 'plpgsql';
