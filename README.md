# world-flights

An [Milleleaf](https://github.com/milleleaf) pipeline: adsb.lol's daily global ADS-B history as GeoParquet (every point,
every field) plus a browser replay file, and Hong Kong near-live snapshots every 15 minutes. The archive is on Kaggle (free public storage): one dataset per day, `helenkwok/world-flights-<D>`, created
once and never changed, plus `helenkwok/world-flights` indexing them. Hugging Face (`alreadyopen/world-flights`) keeps only what
the map needs: the bucket holds the last `HF_DAYS` = 7 days of traces and replay, the index and `live/hk*`; the dataset repo
holds the card, index and manifest. Data: adsb.lol contributors, ODbL 1.0.

| File | Does |
|---|---|
| `bake.py` | Latest `planes-readsb-prod-0` release of `adsblol/globe_history_<year>`: streams the split tar (curl into `tarfile`), one NDJSON batch of about 1 GB at a time into DuckDB (6 GB memory cap), writes `v/<D>/traces-NNN.parquet`, hourly `v/<D>/replay/HH.bin`, `v/<D>/acas.parquet` and the release's other files as delivered in `v/<D>/raw/`, then `index.json` (all days, read back from the bucket) and `latest.json`. `python3 bake.py <tag>` bakes another release, e.g. a small `...-mlatonly-0` for a local test |
| `kaggle_archive.py` | The day's own Kaggle dataset (skipped if it exists), verified with `kaggle datasets files`; then the small index dataset |
| `publish.py` | The new day to the HF bucket, index and manifest to the HF dataset; days older than `HF_DAYS` leave the bucket only once Kaggle lists today's and their datasets (`python3 publish.py test`: self-test) |
| `live.py`, `live.yml` | Every 15 minutes: one API call (250 nm around Hong Kong), `live/hk.json` and `live/hk-<UTC day>.parquet` on the bucket; a 429 or error skips the run. The bake archives each complete day to Kaggle |
| `bake.yml` | Daily at 04:30 UTC (adsb.lol publishes about 03:26 UTC): bake, Kaggle archive, HF publish; a manual run takes a release tag to re-bake a day |

Measured on 2026-10-09 data: the mlat-only release (42,187 aircraft, 10.1M points) bakes in 20 s locally; the first 300 MB of
the prod release held 7,847 aircraft and 10.6M points (442 MB Parquet). A full prod day is about 4.1 GB of tar, ~26 GB of JSON,
~150M points, ~6 GB of Parquet, ~1 GB of raw heatmap and ~0.7 GB of replay: about 8 GB a day (estimates scaled from that sample).

Kaggle limits (docs, 2026-10): 200 GB per dataset and 50 top-level files; no published limit on the number of public
datasets. If one dataset a day ever becomes a problem, change `publish.day_slug` to weeks.

Built from [pipeline-template](https://github.com/milleleaf/pipeline-template); `manifest.py`, `check.py`, `check.yml`
and `kaggle_mirror.py` are its shared files (`kaggle_mirror.mirror` uploads the Kaggle datasets).

## Lessons (each cost a failed run once)

- Don't push to a pipeline repo while its run is going: the run's keep-alive push is rejected.
- Git keeps no empty folders: `mkdir -p` before writing into one.
- Linux has a smaller CA store than macOS: a server that leaves out an intermediate certificate fails only on the runner
  (hk-transit `mtr.py` bundles the missing one).
- Install DuckDB extensions once before any parallel work (`INSTALL` races on a fresh runner).
- An unquoted `: ` inside a workflow `run:` line makes the file invalid YAML, and GitHub silently drops its triggers
  ("does not have 'workflow_dispatch' trigger"). `check.py` now parses every workflow.
- Kaggle stores `.gz` files decompressed (`bus/10_10.json.gz` arrives as `bus/10_10.json`, at its full size), so the
  Kaggle copy's names and sha256 differ from `manifest.json`; say so in the card if the pipeline publishes `.gz`.
- `python publish.py | tee …` reports `tee`'s exit status, so a publish the gate refused showed as a green run. The publish
  step starts with `set -o pipefail` (found on hk-property, 2026-10-10).
- Never put client or private data in a pipeline repo: they are public, and so is their history.

Code: Apache-2.0. Data: under each source's own terms, listed in `sources.json`.
