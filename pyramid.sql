-- CREATE SCHEMA IF NOT EXISTS cdb_pyramid;
DO $$ BEGIN
  IF NOT EXISTS ( SELECT nspname FROM pg_namespace WHERE nspname = 'cdb_pyramid' ) THEN
    CREATE SCHEMA cdb_pyramid; 
  END IF;
END $$ LANGUAGE 'plpgsql';

-- {
CREATE OR REPLACE FUNCTION CDB_ListPyramids()
RETURNS TABLE (tab regclass, gcol text, tcol text, ext geometry, res numeric[], tbins numeric[], fields text[])
AS $$
DECLARE
  r1 RECORD;
  args text;
  aa text[];
BEGIN
  FOR r1 IN SELECT oid, tgrelid FROM pg_trigger
             WHERE tgname = 'cdb_maintain_pyramid'
  LOOP
    args := (regexp_split_to_array(pg_get_triggerdef(r1.oid), '[\(\)]'))[2];
    --RAISE DEBUG 'Args: %', args;

    EXECUTE 'SELECT ARRAY[' ||  args || ']' INTO aa;

    tab := r1.tgrelid;
    gcol := aa[1];
    tcol := aa[2];
    -- NOTE: aa[3] is the pyramid table regclass
    ext := aa[4];
    res := aa[5];
    tbins := aa[6];
    fields := aa[7];

    RETURN NEXT;

  END LOOP;
END;
$$ LANGUAGE 'plpgsql';
-- }

CREATE OR REPLACE VIEW cdb_pyramid.cdb_pyramid AS
SELECT * FROM CDB_ListPyramids();

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
  j integer;
  si integer;
  ti integer;
  tslot integer;
  ntslots integer;
  ntarget numeric[];
  snvals integer; -- number of values in source
  snslots integer; -- number of time slots in source
  tnslots integer; -- number of time slots in target
  ttslot integer[]; 
  missing numeric[];
  svaloff integer; -- source values offset
  tvaloff integer; -- target values offset
BEGIN

  -- Make aggregate friendly
  IF target IS NULL THEN RETURN what; END IF;
  IF what IS NULL THEN RETURN target; END IF;

  IF ( target[1] = 0 ) != ( what[1] = 0 ) THEN
    RAISE EXCEPTION 'Cannot add timeless and timed pixel values';
  END IF;

  snvals := floor( (array_upper(what, 1) - 1 - what[1]) / COALESCE(NULLIF(what[1],0),1) );

  --RAISE WARNING 'Source has % timeslots and % values', sntslots, snvals;

  ntslots := target[1];

  -- for each source value, find time slot offset in target, possibly null
  missing := ARRAY[]::numeric[];
  FOR si IN 0..what[1]-1 LOOP
    tslot := what[2+si];
    -- RAISE DEBUG 'Looking for timeslot %', tslot;
    ti := NULL;
    FOR i IN 0..target[1]-1 LOOP
      IF target[2+i] = tslot THEN
        ti := i;
        EXIT;
      END IF;
    END LOOP;
    -- RAISE DEBUG 'Timeslot % found in target slot %', tslot, ti;
    IF ti IS NOT NULL THEN
      ttslot[1+si] := ti;
    ELSE
      ttslot[1+si] := ntslots + COALESCE(array_upper(missing, 1), 0);
      missing := missing || tslot::numeric;
    END IF;
  END LOOP;

  IF array_upper(missing, 1) > 0 THEN
    -- need to make space for the new values
    ntarget := (target[1] + array_upper(missing, 1)) ::numeric || target[2:1+ntslots] || missing;
    FOR i IN 0..snvals-1 LOOP
      ti := 2 + ntslots + ( ntslots * i );
      ntarget := ntarget || target[ti:(ti+(ntslots-1))] || array_fill(0::numeric, ARRAY[array_upper(missing, 1)]);
    END LOOP;
  ELSE
    ntarget := target;
  END IF;

  --RAISE WARNING 'source:%, target:%, ttslot:% missing:%', what, target, ttslot, missing;
  --RAISE WARNING 'ntarget:%', ntarget;

  svaloff := 2 + what[1];
  tvaloff := 2 + ntarget[1];
  snslots := COALESCE(NULLIF(what[1],0),1);
  tnslots := COALESCE(NULLIF(ntarget[1],0),1);
  FOR i IN 0..snvals-1 LOOP -- for each value 
    FOR j IN 0..snslots-1 LOOP -- for each timeslot in source
      si := svaloff + j + ( i * snslots );
      ti := tvaloff + COALESCE(ttslot[j+1],0) + ( i * tnslots );
      -- RAISE DEBUG 'Adding s:% (%) to t:% (%) -- ttslot[%]=%', si, what[si], ti, ntarget[ti], j+1,ttslot[j+1];
      ntarget[ti] := ntarget[ti] + what[si];
    END LOOP;
  END LOOP;

  -- RAISE DEBUG '- %', target;
  -- RAISE DEBUG '+ %', ntarget;

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

  -- add each value of source to value of target
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
  --RAISE DEBUG 'p:% -- nslots:%', pixel, nslots;
  FOR i IN 1..nslots LOOP
    -- RAISE DEBUG 'Index %', i;
    t := CASE WHEN pixel[1] = 0 THEN NULL ELSE pixel[1+i] END;
    vi := 2 + pixel[1] + col * nslots + (i-1);
    --RAISE DEBUG 't: %', t;
    --RAISE DEBUG 'vi: %', vi;
    v := pixel[vi];
    RETURN NEXT;
  END LOOP;
END;
$$
LANGUAGE 'plpgsql' STRICT;

-- {
-- @param tbl table identifier (passing its name would work)
-- @param col geometry column
-- @param fields array of field names to aggregate (sum) in output pixels,
--        can be NULL for none.
-- @param tcol time column, expected to be timestamp, can be NULL
-- @temporal_bins array of epoch values to separate time slots
--                (lower to higher), can be NULL
-- 
CREATE OR REPLACE FUNCTION CDB_BuildPyramid(tbl regclass, col text, fields text[], tcol text, temporal_bins numeric[])
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
  maxpix := 1024; -- 32x32

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
  ptab := 'cdb_pyramid."' || tblinfo.tab || '"';
  sql := 'CREATE TABLE ' || ptab || '(res float8, ext geometry, v numeric[])';
  EXECUTE sql;
  -- if the source table can be SELECT'ed by publicuser/tileuser,
  -- grant that privilege to the pyramid as well
  sql := 'SELECT u, has_table_privilege(u, $1, ''SELECT'') p FROM ( VALUES ($2,$3) ) f(u)';
  RAISE DEBUG '%', sql;
  FOR rec IN EXECUTE sql USING tbl::text, 'publicuser', 'tileuser' LOOP
    RAISE DEBUG 'Privilege: %', rec.p;
    IF rec.p THEN
      sql := 'GRANT SELECT ON ' || ptab::text || ' TO ' || rec.u;
      RAISE DEBUG '%', sql;
      EXECUTE sql;
    END IF;
  END LOOP;

  -- 2. Start from bottom-level summary and add summarize up to top
  --    Stop condition is when we have less than maxpix "pixels"
  tile_ext := ST_SetSRID(tblinfo.ext::geometry, tblinfo.srid);
  tile_res := least(st_xmax(tile_ext)-st_xmin(tile_ext), st_ymax(tile_ext)-st_ymin(tile_ext))
        / (256*8); -- enough to render 8 tiles per side with no loss of precision
  -- TODO: round resolution to be on the webmercator resolution set ?

  -- TODO: re-compute tile_ext to always be the full webmercator extent
  --       or better yet take it as a parameter

  resolutions := resolutions || tile_res;

  sql := ' INSERT INTO ' || ptab
      || '(res, ext, v) SELECT '
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
    sql := sql || ' ELSE ' || array_upper(temporal_bins, 1)
      || ' END';
  ELSE
    sql := sql || '0::numeric';
  END IF;
  sql := sql || ', 1'; -- count

  IF fields IS NOT NULL THEN
    -- add more field summaries
    FOR rec IN SELECT unnest(fields) f LOOP
      sql := sql || ',' || quote_ident(rec.f);
    END LOOP;
  END IF;

  sql := sql 
      || ']) as v FROM ' || tbl::text
      || ' GROUP BY ext'; 

  RAISE DEBUG '%', sql;

  EXECUTE sql;

  GET DIAGNOSTICS pixel_vals := ROW_COUNT;

  RAISE DEBUG '% pixels with resolution %', pixel_vals, tile_res;

  -- create indices

  sql := 'CREATE INDEX ON ' || ptab || '(res)';
  EXECUTE sql;

  sql := 'CREATE INDEX ON ' || ptab ||' using gist (ext)';
  EXECUTE sql;

  -- compute upper levels from lower ones

  WHILE pixel_vals > maxpix LOOP

    sql := ' INSERT INTO ' || ptab
        || '(res, ext, v) SELECT '
        || tile_res * 2
        || ', ST_Envelope(ST_Buffer('
        || 'ST_SnapToGrid(ST_Centroid(ext), '
        || st_xmin(tile_ext) - tile_res || ','
        || st_ymin(tile_ext) - tile_res || ','
        || tile_res * 2 || ',' || tile_res * 2
        || '), ' || tile_res
        || ')) as new_ext, CDB_TorquePixel_agg(v) FROM '
        || ptab || ' WHERE res = ' || tile_res 
        || ' GROUP BY new_ext '
    ; 

    RAISE DEBUG '%', sql;

    EXECUTE sql;

    GET DIAGNOSTICS pixel_vals := ROW_COUNT;

    tile_res := tile_res * 2;

    RAISE DEBUG '% pixels with resolution %', pixel_vals, tile_res;

    resolutions := resolutions || tile_res;

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
    || col || ',' || quote_literal(COALESCE(tcol, '')) || ',' || quote_literal(ptab) || ','
    || quote_literal(tile_ext::text) || ','
    || quote_literal(resolutions::text) || ','
    || quote_literal(COALESCE(temporal_bins, '{}')::text) || ','
    || quote_literal(COALESCE(fields, '{}')::text)
    || ')';
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
  fields text[];
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
  rec RECORD;
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
  fields := TG_ARGV[6];

  RAISE DEBUG 'Fields: %', fields;

  -- trigger procedures cannot take NULL, so we assume empty string is a null
  IF tcol = '' THEN tcol := null; END IF;

  sql := 'SELECT ($1).' || quote_ident(gcol) || ' as g';
  sql := sql || ', ARRAY[';
  IF tcol IS NOT NULL THEN
    sql := sql || '1::numeric, CASE ';
    FOR i IN 1..array_upper(temporal_bins, 1) LOOP
      sql := sql || ' WHEN extract(epoch from ($1).'
        || quote_ident(tcol) || ') < '
        || temporal_bins[i] || ' THEN ' || (i-1);
    END LOOP;
    sql := sql || ' ELSE ' || array_upper(temporal_bins, 1) || ' END';
  ELSE
    sql := sql || '0::numeric';
  END IF;
  sql := sql || ',1';
  FOR rec IN SELECT unnest(fields) f LOOP
    sql := sql || ', ($1).' || quote_ident(rec.f);
  END LOOP;
  sql := sql || '] as v';

  -- Extract info from NEW record
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    EXECUTE sql USING NEW INTO newinfo;
  END IF;

  -- Extract info from OLD record
  IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
    EXECUTE sql USING OLD INTO oldinfo;
    res := resolutions[1]; -- we assume first element is highest (min) 
    originX := st_xmin(full_extent) - res/2.0;
    originY := st_ymin(full_extent) - res/2.0;

    --RAISE DEBUG 'oldinfo: %', oldinfo;
  END IF;

  -- Do nothing on UPDATE if old and new fields of interest
  -- did not change
  IF TG_OP = 'UPDATE' THEN
    IF tcol IS NULL OR oldinfo.v[2] = newinfo.v[2] THEN
      IF oldinfo.g = newinfo.g OR (
              ST_SnapToGrid(oldinfo.g, originX, originY, res, res)
            = ST_SnapToGrid(newinfo.g, originX, originY, res, res) )
      THEN
        RETURN NULL;
      END IF;
    END IF;
  END IF;

  IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
    IF oldinfo.g IS NOT NULL THEN
      -- decrement
      sql := 'UPDATE ' || ptab || ' set v = CDB_TorquePixel_del(v, '
        || quote_literal(oldinfo.v) || ') WHERE ext && '
        || quote_literal(oldinfo.g::text);
      RAISE DEBUG ' %', sql;
      EXECUTE sql;
    END IF;
  END IF;

  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    IF newinfo.g IS NOT NULL THEN
      FOR i IN 1..array_upper(resolutions,1) LOOP
        res := resolutions[i];
        RAISE DEBUG ' updating resolution %', res;
        originX := st_xmin(full_extent) - res/2.0;
        originY := st_ymin(full_extent) - res/2.0;

        -- increment
        g := ST_SnapToGrid(newinfo.g, originX, originY, res, res);
        RAISE DEBUG ' resolution % : % @ %', res, ST_AsText(g), newinfo.v;
        -- Upsert
        sql := 'WITH upsert as (UPDATE ' || ptab || ' set v=CDB_TorquePixel_add(v, '
          || quote_literal(newinfo.v) 
          || ') WHERE res = ' || res || ' AND ext && '
          || quote_literal(newinfo.g::text)
          || ' RETURNING ext ) INSERT INTO '
          || ptab || '(res,ext,v) SELECT ' || res || ', ST_Envelope(ST_Buffer('
          || quote_literal(newinfo.g::text) || ',' || (res/2.0) || ', 1)), '
          || quote_literal(newinfo.v)
          || ' WHERE NOT EXISTS (SELECT * FROM upsert)'; 
        RAISE DEBUG ' %', sql;
        EXECUTE sql;
      END LOOP;
    END IF;
  END IF;

  RETURN NULL;
END;
$$
LANGUAGE 'plpgsql';
