"""One day of adsb.lol's global ADS-B history (readsb globe_history, ODbL 1.0) as GeoParquet plus a browser replay file.
Source: the GitHub release v<YYYY.MM.DD>-planes-readsb-prod-0 of adsblol/globe_history_<year>: split tar parts (.tar.aa, .ab, ...)
that concatenate into one tar holding traces/<last 2 hex>/trace_full_<icao>.json (gzip despite the name), one per aircraft.
Streamed: curl writes the parts in order into Python's tarfile ('r|'), traces go into a batch folder, each batch of BATCH files is
turned into one Parquet part by DuckDB and deleted, so the disk never holds the day's tar. Every other member of the tar (acas/,
heatmap/, README.txt, licences) is kept as delivered under v/<D>/raw/.
Outputs (sources.OUT):
  v/<D>/traces-NNN.parquet  GeoParquet, one row per trace point, every field (aircraft fields per row, `details` = readsb's
                            aircraft object as JSON where the point carries one), geometry = POINT(lon lat) EPSG:4326
  v/<D>/replay/HH.bin       one file per UTC hour: per aircraft, time-ordered, at most one position per 30 s bucket (layout:
                            REPLAY below); replay/index.json lists the hours with their aircraft and point counts
  v/<D>/acas.parquet        raw/acas/acas.json.gz (one ACAS resolution advisory per line: readsb's aircraft object plus
                            `acas_ra`) as GeoParquet, every field (`record` = the whole line as JSON)
  v/<D>/raw/...             the release's non-trace files as delivered (heatmap/NN.bin.ttf is not parsed: see the card)
  v/<D>/live/hk-<D>.parquet the day's Hong Kong snapshots (live.py), copied from the bucket
  v/<D>/index.json          the day's entry (becomes the Kaggle day dataset's index)
  index.json                every day baked so far (read back from the bucket), latest.json the newest day
Where they go: everything in v/<D>/ to the Kaggle day dataset (kaggle_archive.py); traces and replay to the HF bucket for
HF_DAYS days (publish.py)
Usage: python3 bake.py [<release tag>]   (default: the latest prod-0 release; e.g. a ...-mlatonly-0 tag for a small test)"""
import datetime, gzip, json, os, re, shutil, subprocess, sys, tarfile, urllib.error, urllib.request
import duckdb, numpy as np
import sources

BATCH = 1 << 30                                   # bytes of trace JSON per Parquet part
STEP = 30                                         # replay: first position in each 30 s bucket per aircraft
HF_DAYS = 7                                       # days of traces and replay on the HF bucket; Kaggle keeps every day
REPO = 'milleleaf/world-flights'
KG = 'https://www.kaggle.com/datasets/helenkwok/world-flights-'
BK = f'https://huggingface.co/buckets/{REPO}/resolve/'
UA = {'User-Agent': 'Milleleaf world-flights (https://github.com/milleleaf/world-flights)'}
REPLAY = """little-endian: b'AOR1', uint32 header length H, H bytes of JSON header (padded to 4), then N values per column in
header.columns order: t int32 (s after day_start), lon int32 and lat int32 (1e-5 degrees), altitude int32 (ft; -999999 = ground,
-2147483648 = none), ground_speed int16 (kt; -1 none), track int16 (0.1 degree; -1 none). header.aircraft = [[icao, registration,
type, first point, point count], ...] in icao order; an aircraft's points are contiguous and time-ordered. One file per UTC hour
(header.hour); an aircraft flying across hours appears in each of them."""
EXPECTED_DAILY = {'parquet_gb': 6, 'heatmap_raw_gb': 1, 'replay_gb': 0.7, 'note': 'estimate from 2026-10-09 (a ~4.1 GB prod release)'}
ACAS = """
SELECT to_timestamp((json->'acas_ra'->>'unix_timestamp')::DOUBLE) AS "time", json->>'hex' AS "hex", json->>'flight' AS "flight",
  json->>'r' AS "registration", json->>'t' AS "type", (json->>'lat')::DOUBLE AS "lat", (json->>'lon')::DOUBLE AS "lon",
  json->>'alt_baro' AS "alt_baro", json->'acas_ra'->>'advisory' AS "advisory",
  json->'acas_ra'->>'advisory_complement' AS "advisory_complement", json->'acas_ra'->>'threat_id_hex' AS "threat_id_hex",
  json->'acas_ra'->>'full_bytes' AS "full_bytes", json AS "record",
  CASE WHEN json->>'lat' IS NOT NULL THEN ST_Point((json->>'lon')::DOUBLE, (json->>'lat')::DOUBLE) END AS "geometry"
FROM read_json_objects('{src}', format='newline_delimited', compression='gzip')"""


def api(url):
    h = dict(UA, Accept='application/vnd.github+json')
    if os.environ.get('GH_TOKEN'): h['Authorization'] = f"Bearer {os.environ['GH_TOKEN']}"
    return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=60))


def release(tag=None):
    """(tag, day, [asset urls in concatenation order])"""
    years = [int(tag[1:5])] if tag else [datetime.date.today().year, datetime.date.today().year - 1]
    for y in years:
        rels = api(f'https://api.github.com/repos/adsblol/globe_history_{y}/releases?per_page=30')
        rels = [r for r in rels if (r['tag_name'] == tag if tag else re.fullmatch(r'v\d{4}\.\d\d\.\d\d-planes-readsb-prod-0', r['tag_name']))]
        if rels:
            r = max(rels, key=lambda r: r['tag_name'])
            return r['tag_name'], r['tag_name'][1:11].replace('.', '-'), [a['browser_download_url'] for a in sorted(r['assets'], key=lambda a: a['name'])]
    sys.exit(f'no release {tag or "prod-0"} found')


def previous_index():
    try: return json.load(urllib.request.urlopen(urllib.request.Request(BK + 'index.json', headers=UA), timeout=60))
    except urllib.error.HTTPError as e:
        if e.code in (401, 404): return {'days': []}
        raise


COLS = {'icao': 'VARCHAR', 'r': 'VARCHAR', 't': 'VARCHAR', 'desc': 'VARCHAR', 'ownOp': 'VARCHAR', 'year': 'VARCHAR', 'dbFlags': 'INTEGER',
        'noRegData': 'BOOLEAN', 'version': 'VARCHAR', 'timestamp': 'DOUBLE', 'trace': 'JSON[][]'}     # every top-level field seen
POINTS = """
SELECT icao, r registration, t AS "type", "desc" description, ownOp owner_operator, "year", dbFlags db_flags, noRegData no_reg_data,
  version readsb_version, "timestamp" day_start, p[1]::DOUBLE AS t, to_timestamp("timestamp" + p[1]::DOUBLE) AS "time",
  p[2]::DOUBLE lat, p[3]::DOUBLE lon,
  CASE WHEN p[4]::VARCHAR = '"ground"' THEN NULL ELSE p[4]::INTEGER END altitude, p[4]::VARCHAR = '"ground"' on_ground,
  p[5]::DOUBLE ground_speed, p[6]::DOUBLE track, p[7]::INTEGER flags, p[8]::INTEGER vert_rate,
  CASE WHEN json_type(p[9]) = 'NULL' THEN NULL ELSE p[9] END details, p[10]->>'$' AS "source",
  p[11]::INTEGER alt_geom, p[12]::INTEGER geom_rate, p[13]::INTEGER ias, p[14]::DOUBLE roll, ST_Point(p[3]::DOUBLE, p[2]::DOUBLE) geometry
FROM (SELECT *, unnest(trace) p FROM {src})
WHERE CASE WHEN len(p) <> 14 THEN error('a trace point with ' || len(p) || ' fields (expected 14): map them in POINTS') ELSE true END"""


def flush(con, batch, n, day_dir, parts):
    """one batch (NDJSON, one aircraft per line) -> traces-NNN.parquet and a replay part; streamed, no table held in memory"""
    keys = {k for k, in con.execute(f"""SELECT DISTINCT unnest(json_keys(json)) FROM read_json_objects('{batch}',
                                       format='newline_delimited', maximum_object_size=67108864)""").fetchall()}
    assert keys <= COLS.keys(), f'new top-level trace fields {keys - COLS.keys()}: add them to COLS and POINTS (never drop a field)'
    cols = ', '.join(f'"{k}": \'{v}\'' for k, v in COLS.items())
    src = f"read_json('{batch}', format='newline_delimited', maximum_object_size=67108864, columns={{{cols}}})"
    out = os.path.join(day_dir, f'traces-{n:03d}.parquet')
    con.execute(f"COPY ({POINTS.format(src=src)}) TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 500000)")
    con.execute(f"""COPY (SELECT icao, any_value(registration) OVER w registration, any_value("type") OVER w "type", t::INTEGER t,
                    round(lon * 1e5)::INTEGER lon, round(lat * 1e5)::INTEGER lat,
                    CASE WHEN on_ground THEN -999999 ELSE coalesce(altitude, -2147483648) END altitude,
                    coalesce(round(ground_speed), -1)::SMALLINT ground_speed, coalesce(round(track * 10), -1)::SMALLINT track
                    FROM '{out}' WINDOW w AS (PARTITION BY icao)
                    QUALIFY row_number() OVER (PARTITION BY icao, floor(t / {STEP}) ORDER BY t) = 1)
                    TO '{parts}/replay-{n:03d}.parquet' (FORMAT parquet)""")
    os.remove(batch)


def write_replay(con, parts, folder, day, day_start):
    """one replay file per UTC hour of t (30 s buckets never straddle an hour) plus folder/index.json"""
    src = f"read_parquet('{parts}/*.parquet')"
    cols = [('t', '<i4'), ('lon', '<i4'), ('lat', '<i4'), ('altitude', '<i4'), ('ground_speed', '<i2'), ('track', '<i2')]
    hours = []
    for h, in con.execute(f'SELECT DISTINCT t // 3600 FROM {src} ORDER BY 1').fetchall():
        w = f'{src} WHERE t // 3600 = {h}'
        ac = con.execute(f'SELECT icao, any_value(registration), any_value("type"), count(*) FROM {w} GROUP BY icao ORDER BY icao').fetchall()
        first = np.concatenate([[0], np.cumsum([a[3] for a in ac])[:-1]]).tolist()
        r = con.execute(f'SELECT t, lon, lat, altitude, ground_speed, track FROM {w} ORDER BY icao, t').fetchnumpy()
        head = json.dumps({'day': day, 'day_start': day_start, 'hour': h, 'step': STEP, 'points': len(r['t']),
                           'columns': [c for c, _ in cols], 'layout': REPLAY,
                           'aircraft': [[i, reg, typ, f, c] for (i, reg, typ, c), f in zip(ac, first)]}, separators=(',', ':')).encode()
        head += b' ' * (-len(head) % 4)
        name = f'{h:02d}.bin'
        with open(os.path.join(folder, name), 'wb') as f:
            f.write(b'AOR1' + np.uint32(len(head)).tobytes() + head)
            for c, t in cols: f.write(np.asarray(r[c]).astype(t).tobytes())
        hours.append({'hour': h, 'file': name, 'aircraft': len(ac), 'points': len(r['t']), 'bytes': os.path.getsize(os.path.join(folder, name))})
    json.dump({'day': day, 'day_start': day_start, 'step': STEP, 'layout': REPLAY, 'hours': hours}, open(os.path.join(folder, 'index.json'), 'w'), indent=1)
    return sum(x['points'] for x in hours)


def main(tag=None):
    tag, day, urls = release(tag)
    idx = previous_index()
    if any(d['day'] == day for d in idx['days']) and not os.environ.get('BAKE_AGAIN'):
        print(f'{day} is already baked; nothing new'); idx['new'], idx['new_day'] = [], None
        os.makedirs(sources.OUT, exist_ok=True); json.dump(idx, open(os.path.join(sources.OUT, 'index.json'), 'w'), indent=1)
        return
    rel = f'v/{day}'; day_dir = os.path.join(sources.OUT, rel); work = os.path.join(sources.CACHE, 'work')
    folder, parts = os.path.join(work, 'batch.ndjson'), os.path.join(work, 'replay')
    shutil.rmtree(day_dir, ignore_errors=True); shutil.rmtree(work, ignore_errors=True)
    for d in (day_dir, parts): os.makedirs(d)
    day_dir = os.path.abspath(day_dir)
    con = duckdb.connect(os.path.join(work, 'spill.duckdb'))
    con.execute(f"LOAD spatial; SET memory_limit='6GB'; SET threads=4; SET temp_directory='{work}/tmp'; SET preserve_insertion_order=false")
    curl = subprocess.Popen(['curl', '-sSfL', '--retry', '3', *urls], stdout=subprocess.PIPE)
    n = files = 0; batch = open(folder, 'wb')
    with tarfile.open(fileobj=curl.stdout, mode='r|') as tar:            # one trace per line: DuckDB reads lines in bounded memory
        for m in tar:
            if not m.isfile(): continue
            if not re.fullmatch(r'(\./)?traces/[0-9a-f]{2}/trace_full_[^/]+\.json', m.name):   # acas, heatmap, README, licences
                rel_name = os.path.normpath(m.name); assert not rel_name.startswith(('..', '/')), m.name
                dst = os.path.join(day_dir, 'raw', rel_name)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                with open(dst, 'wb') as f: shutil.copyfileobj(tar.extractfile(m), f)
                continue
            batch.write(gzip.decompress(tar.extractfile(m).read()).replace(b'\n', b'') + b'\n')   # raw newlines only between tokens
            files += 1
            if batch.tell() >= BATCH:
                batch.close(); flush(con, folder, n, day_dir, parts); n += 1; batch = open(folder, 'wb')
                print(f'{files} aircraft, {n} parts', flush=True)
    assert curl.wait() == 0, f'download failed (curl exit {curl.returncode})'
    if batch.tell(): batch.close(); flush(con, folder, n, day_dir, parts)
    aircraft, points = con.execute(f"SELECT count(DISTINCT icao), count(*) FROM read_parquet('{day_dir}/traces-*.parquet')").fetchone()
    assert aircraft == files, f'{files} trace files but {aircraft} aircraft'
    day_start = int(con.execute(f"SELECT min(day_start) FROM read_parquet('{day_dir}/traces-*.parquet')").fetchone()[0])
    os.makedirs(os.path.join(day_dir, 'replay'))
    replay_points = write_replay(con, parts, os.path.join(day_dir, 'replay'), day, day_start)
    acas = os.path.join(day_dir, 'raw', 'acas', 'acas.json.gz'); acas_events = 0
    if os.path.exists(acas) and os.path.getsize(acas) > 20:                  # an empty gzip is 20 bytes (the mlat-only release)
        con.execute(f"COPY ({ACAS.format(src=acas)} ORDER BY 1) TO '{day_dir}/acas.parquet' (FORMAT parquet, COMPRESSION zstd)")
        acas_events = con.execute(f"SELECT count(*) FROM '{day_dir}/acas.parquet'").fetchone()[0]
    hk = os.path.join(day_dir, 'live', f'hk-{day}.parquet'); os.makedirs(os.path.dirname(hk))   # live.py's day, complete by now
    try: urllib.request.urlretrieve(BK + f'live/hk-{day}.parquet', hk)
    except urllib.error.HTTPError as e:
        if e.code not in (401, 404): raise
        os.rmdir(os.path.dirname(hk)); print(f'no Hong Kong snapshots for {day}')
    con.close(); shutil.rmtree(work)
    prev = [d for d in idx['days'] if d['day'] < day]
    if prev and tag.endswith('-prod-0') and points < prev[-1]['points'] * 0.5:
        sys.exit(f'{day}: {points} points, under half of {prev[-1]["day"]} ({prev[-1]["points"]}): a broken release?')
    names = sorted(os.path.relpath(os.path.join(r, f), day_dir) for r, _, fs in os.walk(day_dir) for f in fs) + ['index.json']
    on_bucket = [f for f in names if f.startswith(('traces-', 'replay/'))]          # what the map needs; the rest is on Kaggle only
    entry = {'day': day, 'release': tag, 'source': urls, 'aircraft': aircraft, 'points': points, 'replay_points': replay_points,
             'acas_events': acas_events, 'files': names, 'bytes': sum(os.path.getsize(os.path.join(day_dir, f)) for f in names[:-1]),
             'kaggle': KG + day, 'bucket_files': on_bucket, 'bucket': [BK + f'{rel}/{f}' for f in on_bucket]}
    json.dump(entry, open(os.path.join(day_dir, 'index.json'), 'w'), indent=1)       # the day dataset's own index
    days = sorted([d for d in idx['days'] if d['day'] != day] + [entry], key=lambda d: d['day'])
    idx = {'built': datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds'),
           'counts': {'days': len(days), 'aircraft': sum(d['aircraft'] for d in days), 'points': sum(d['points'] for d in days)},
           'licence': 'ODbL 1.0, adsb.lol contributors', 'hf_days': HF_DAYS, 'expected_daily_volume': EXPECTED_DAILY,
           'replay_layout': REPLAY, 'days': days, 'new_day': day, 'new': [f'{rel}/{f}' for f in on_bucket]}
    json.dump(idx, open(os.path.join(sources.OUT, 'index.json'), 'w'), indent=1)
    json.dump({k: entry[k] for k in ('day', 'aircraft', 'points', 'replay_points', 'acas_events', 'kaggle', 'bucket')} | {'index': BK + 'index.json'},
              open(os.path.join(sources.OUT, 'latest.json'), 'w'), indent=1)
    print(json.dumps({'day': day, 'aircraft': aircraft, 'points': points, 'replay_points': replay_points, 'acas_events': acas_events, 'bytes': entry['bytes']}))


if __name__ == '__main__':
    duckdb.connect().execute('INSTALL spatial')
    main(*sys.argv[1:2])
