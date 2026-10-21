"""Hong Kong near-live aircraft from the adsb.lol API (ODbL 1.0): one GET of /v2/point/22.3/114.2/250 (250 nm circle) per run.
Bucket (overwritten in place):  live/hk.json               the latest response as received, every field
                                live/hk-YYYY-MM-DD.parquet  that UTC day's snapshots so far, appended each run (GeoParquet: one row
                                                            per aircraft per snapshot, `aircraft` = the full API object as JSON)
The daily bake copies each complete day into that day's Kaggle dataset; publish.py removes it from the bucket after HF_DAYS,
once Kaggle lists it. On a 429 or any API error the run is skipped: no retry loop.
Needs HF_TOKEN. Usage: python3 live.py"""
import datetime, json, os, sys, tempfile, urllib.request
import duckdb
from huggingface_hub import batch_bucket_files, create_bucket, download_bucket_files

URL = 'https://api.adsb.lol/v2/point/22.3/114.2/250'
UA = {'User-Agent': 'Milleleaf world-flights (https://github.com/milleleaf/world-flights)'}
REPO = 'milleleaf/world-flights'
ROWS = """
SELECT to_timestamp((j->>'now')::BIGINT / 1000) AS "snapshot", j->>'msg' AS "msg", (j->>'total')::INTEGER AS "total",
  (j->>'ctime')::BIGINT AS "ctime", (j->>'ptime')::BIGINT AS "ptime",
  a->>'hex' AS "hex", a->>'flight' AS "flight", a->>'r' AS "registration", a->>'t' AS "type", a->>'category' AS "category", a->>'type' AS "source",
  (a->>'lat')::DOUBLE AS "lat", (a->>'lon')::DOUBLE AS "lon",
  CASE WHEN a->>'alt_baro' = 'ground' THEN NULL ELSE (a->>'alt_baro')::INTEGER END AS "alt_baro", a->>'alt_baro' = 'ground' AS "on_ground",
  (a->>'alt_geom')::INTEGER AS "alt_geom", (a->>'gs')::DOUBLE AS "gs", (a->>'track')::DOUBLE AS "track", (a->>'seen_pos')::DOUBLE AS "seen_pos",
  (a->>'seen')::DOUBLE AS "seen", a AS "aircraft", ST_Point((a->>'lon')::DOUBLE, (a->>'lat')::DOUBLE) AS "geometry"
FROM (SELECT json j, unnest(json_extract(json, '$.ac[*]')) a FROM read_json_objects('{src}'))"""


def fetch():
    try:
        with urllib.request.urlopen(urllib.request.Request(URL, headers=UA), timeout=60) as r:
            body = r.read()
        d = json.loads(body)
        assert isinstance(d.get('ac'), list) and d.get('now'), f"unexpected response: {str(d)[:200]}"
        return body, d
    except Exception as e:                                             # 429, 5xx, timeout, bad JSON: back off until the next run
        print(f'::warning::adsb.lol API: {e}; skipping this run'); sys.exit(0)


def append(con, snap, old, out):
    """old (or None) + this snapshot -> out"""
    new = ROWS.format(src=snap)
    q = f"SELECT * FROM read_parquet('{old}') UNION ALL BY NAME {new}" if old else new
    con.execute(f"COPY ({q} ORDER BY snapshot, hex) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")


def main():
    body, d = fetch()
    day = datetime.datetime.fromtimestamp(d['now'] / 1000, datetime.UTC).date()
    name = f'live/hk-{day}.parquet'
    w = tempfile.mkdtemp()
    snap, old, out = os.path.join(w, 'hk.json'), os.path.join(w, 'old.parquet'), os.path.join(w, 'new.parquet')
    open(snap, 'wb').write(body)
    create_bucket(REPO, private=False, exist_ok=True)                    # the first snapshot may run before the first bake
    download_bucket_files(REPO, [(name, old)], raise_on_missing_files=False)   # absent on the day's first run
    have = os.path.exists(old)
    con = duckdb.connect(); con.execute('INSTALL spatial; LOAD spatial')
    append(con, snap, old if have else None, out)
    rows = con.execute(f"SELECT count(*), count(DISTINCT snapshot) FROM '{out}'").fetchone()
    batch_bucket_files(REPO, add=[(snap, 'live/hk.json'), (out, name)])
    print(json.dumps({'snapshot': d['now'], 'aircraft': len(d['ac']), 'file': name, 'rows': rows[0], 'snapshots': rows[1]}))


if __name__ == '__main__':
    main()
