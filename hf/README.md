---
pretty_name: World flights (adsb.lol ADS-B history and Hong Kong live)
license: odbl
license_name: odbl-1.0
license_link: https://opendatacommons.org/licenses/odbl/1-0/
tags:
  - geospatial
  - aviation
  - ads-b
  - geoparquet
---

# World flights

Every aircraft position adsb.lol's feeders received, one day at a time, plus near-live snapshots around Hong Kong. Rebuilt by
[AlreadyOpen/world-flights](https://github.com/milleleaf/world-flights): the daily bake runs at 04:30 UTC, after adsb.lol
publishes the previous UTC day (about 03:26 UTC); the Hong Kong snapshot runs every 15 minutes.

## Where the data is

- **Full history: Kaggle, one public dataset per day**, created once and never changed or deleted:
  `https://www.kaggle.com/datasets/helenkwok/world-flights-YYYY-MM-DD`, e.g.
  [helenkwok/world-flights-2026-10-09](https://www.kaggle.com/datasets/helenkwok/world-flights-2026-10-09). Each holds the
  complete day: every file listed below, at the dataset's root.
- **Index of all days: [helenkwok/world-flights](https://www.kaggle.com/datasets/helenkwok/world-flights)** on Kaggle (updated
  daily): `index.json` with every day's dataset slug, aircraft, points and size, `latest.json`, and the newest complete Hong
  Kong day (`live/hk-YYYY-MM-DD.parquet`).
- **Hugging Face, for maps: the last 7 days only.** The bucket `huggingface.co/buckets/milleleaf/world-flights` (CORS and HTTP
  Range) holds `v/<YYYY-MM-DD>/traces-NNN.parquet` and `v/<YYYY-MM-DD>/replay/` for the newest 7 days, `index.json`,
  `latest.json` and `live/hk*`. A day leaves the bucket only after its Kaggle dataset is listed by Kaggle. This dataset repo
  holds only this card, `index.json` and `manifest.json`: no copy of the data.

## Files

- `v/<YYYY-MM-DD>/traces-NNN.parquet`: GeoParquet (`geometry` = POINT(lon lat), WGS 84), **one row per trace point, every
  point and every field** of adsb.lol's daily `globe_history` release (`planes-readsb-prod-0`). Split into parts of about a
  gigabyte of source JSON each; an aircraft is never split across parts. Columns:
  - per aircraft (repeated on each of its rows): `icao` (24-bit address, hex; `~` prefix = non-ICAO), `registration`, `type`
    (ICAO type code), `description`, `owner_operator`, `year`, `db_flags` (readsb bitfield: 1 military, 2 interesting, 4 PIA,
    8 LADD), `no_reg_data`, `readsb_version`, `day_start` (Unix seconds, 00:00 UTC of the day)
  - per point (readsb's trace array, in its order): `t` (seconds after `day_start`; `time` = the same as a timestamp), `lat`,
    `lon`, `altitude` (ft, barometric unless flag 8; NULL when on the ground or unknown), `on_ground`, `ground_speed` (kt),
    `track` (degrees; can be true heading when on the ground), `flags` (1 position stale, 2 start of a new leg, 4 vertical rate is
    geometric, 8 altitude is geometric), `vert_rate` (ft/min), `details` (readsb's full aircraft object as JSON, present when
    it changed: flight, squawk, category, nav_*, nic/nac/sil, wind, temperatures, ...), `source` (`adsb_icao`, `mlat`,
    `tisb_*`, `adsr_*`, ...), `alt_geom`, `geom_rate`, `ias`, `roll`
- `v/<YYYY-MM-DD>/replay/HH.bin` (one file per UTC hour, so a browser loads one hour at a time) and `replay/index.json`
  (hours with their aircraft and point counts and file sizes): the same day for a map that replays it. **Downsampled: per
  aircraft, only the first position in each 30-second bucket is kept**, and only `t`, `lon`, `lat`, `altitude`,
  `ground_speed` and `track` (rounded: 1e-5 degree, whole feet, whole knots, 0.1 degree). Everything else is in the Parquet,
  which is complete. An aircraft flying across hours appears in each hour's file; hour `24` holds the few points readsb logged
  just after midnight. Layout (no library needed: `DataView` and typed arrays): `AOR1`, a uint32 header length, a JSON header
  (`hour`, `aircraft`: `[icao, registration, type, first point, count]` in icao order, `columns`, `layout`), then one
  little-endian column after another: `t` int32, `lon` int32, `lat` int32, `altitude` int32 (-999999 ground, -2147483648
  unknown), `ground_speed` int16 and `track` int16 (-1 unknown). An aircraft's points are contiguous and time-ordered.
- `v/<YYYY-MM-DD>/raw/`: **every other file of the release, as delivered**: `acas/acas.json.gz`, `acas/acas.csv.gz`,
  `heatmap/NN.bin.ttf` (48 half-hour files), `README.txt`, `LICENSE-ODbL.txt`, `LICENSE-cc0.txt`.
- `v/<YYYY-MM-DD>/acas.parquet`: GeoParquet of `raw/acas/acas.json.gz`, one row per ACAS/TCAS resolution advisory message
  (8,391 on 2026-10-09), every field: `record` is the whole line (readsb's aircraft object plus `acas_ra` with time, advisory,
  ARA/RAT/MTE/RAC, threat id and the raw message bytes), with `time`, `hex`, `flight`, `registration`, `type`, `lat`, `lon`,
  `alt_baro`, `advisory`, `advisory_complement`, `threat_id_hex` and `full_bytes` as columns. `acas.csv.gz` holds the same
  events in a shorter, headerless text form; it is kept as delivered and not parsed separately.
- **The heatmap files are not parsed.** They are readsb's binary heatmap (gzip, then little-endian int32 records of 16 bytes:
  10-second slice markers, positions and callsign/squawk records). The format is defined only by readsb's and tar1090's source
  code, not documented, so this dataset keeps the originals rather than guess at fields. The trace Parquet holds the same
  positions at full resolution.
- `index.json` (the day's entry; the top-level `index.json` lists every day): counts, file names and sizes, the Kaggle URL,
  and bucket URLs while the day is on Hugging Face. `latest.json`: the newest day.
- `live/hk-YYYY-MM-DD.parquet` (UTC day): GeoParquet, one row per aircraft per snapshot of the adsb.lol API within 250 nm of
  (22.3 N, 114.2 E), every 15 minutes; `aircraft` holds the full API object as JSON, with common fields as columns. Built up on
  the bucket during the day, then archived in that day's Kaggle dataset. `live/hk.json` (bucket only): the latest response.
- `manifest.json` (Hugging Face): sha256 and size of the files of the latest daily bake, the counts and the sources.

Kaggle stores `.gz` files decompressed, so the Kaggle copies of `raw/acas/*.gz` arrive without `.gz`, at full size.

**Expected volume** (estimate from 2026-10-09, a 4.1 GB release; also in `index.json` `expected_daily_volume`): about
**6 GB of trace Parquet per day**, plus about 1 GB of raw heatmap files and 0.7 GB of replay files: roughly 8 GB per day on
Kaggle (about 2.9 TB a year; Kaggle allows 200 GB per dataset), and about 7 x 6.7 GB = 47 GB on the Hugging Face bucket.

## Source and licence

Data from [adsb.lol](https://adsb.lol) contributors ([historical data](https://www.adsb.lol/docs/open-data/historical/),
[GitHub releases](https://github.com/adsblol/globe_history_2026/releases), [API](https://api.adsb.lol/docs)), made available
under the [Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/). Credit: "adsb.lol contributors,
ODbL 1.0". Feeders waive their rights to the raw data under CC0; adsb.lol releases the database under ODbL.

Every file, here and in the Kaggle datasets, is a **derived database under the ODbL**: if you publicly use or share a database adapted from it, you must
release that adapted database under the ODbL (share-alike), credit adsb.lol contributors, and keep it open. That is why this
dataset holds adsb.lol data only and is kept apart from AlreadyOpen datasets under other licences; combining them is your
choice and your share-alike obligation. Aircraft registration and type fields come with readsb's database as bundled in the
release. Provided as is, with no warranty, and not endorsed by adsb.lol.
