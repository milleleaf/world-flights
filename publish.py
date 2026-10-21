"""Publish the new day to Hugging Face (needs HF_TOKEN with write access to the alreadyopen org, and KAGGLE_API_TOKEN to check
the archive). Runs after kaggle_archive.py. Hugging Face holds only what the map needs; the archive is on Kaggle.
  bucket   REPO  traces-NNN.parquet and replay/ of the newest HF_DAYS days, index.json, latest.json (live.py adds live/hk*),
                 overwritten in place (read by browsers: CORS + HTTP Range)
  dataset  REPO  the card, index.json and manifest.json only: no copy of the data
A day leaves the bucket (its v/<D>/ files and live/hk-<D>.parquet) only when `kaggle datasets files` lists both today's day
dataset and that day's: if today's Kaggle upload failed, nothing is removed. A day never archived stays on the bucket.
Nothing is uploaded unless the publish gate passes (manifest.py). Gate counts are totals over every day, so a quiet weekend
never trips it (bake.py stops a day under half the previous day's points); the gate compares only files outside v/.
Usage: python3 publish.py     (after bake.py and kaggle_archive.py)"""
import json, os, subprocess, sys
from huggingface_hub import CommitOperationAdd, HfApi, batch_bucket_files, create_bucket
import bake, manifest, sources

REPO = bake.REPO
_idx = os.path.join(sources.OUT, 'index.json')
NEW = json.load(open(_idx)).get('new', []) if os.path.exists(_idx) else []
DATASET_FILES = ['index.json']
BUCKET_FILES = ['index.json', 'latest.json'] + NEW
KAGGLE = ('helenkwok/world-flights', 'Milleleaf: World flights (adsb.lol)')   # index of the day datasets (kaggle_archive.py)
day_slug = lambda day: f'{KAGGLE[0]}-{day}'


def on_kaggle(slug):
    """the dataset exists and lists its index.json (Kaggle has finished processing it)"""
    r = subprocess.run(['kaggle', 'datasets', 'files', slug, '-v', '--page-size', '200'], capture_output=True, text=True)
    return r.returncode == 0 and 'index.json' in r.stdout


def window(idx, verified=on_kaggle):
    """bucket paths to delete for days older than the newest HF_DAYS, each archived on Kaggle; marks them off the bucket"""
    old = [d for d in idx['days'][:-bake.HF_DAYS] if d.get('bucket')]
    if not old or not verified(day_slug(idx['new_day'])):
        return []
    drop = []
    for d in old:
        if verified(day_slug(d['day'])):
            drop += [f"v/{d['day']}/{f}" for f in d['bucket_files']] + [f"live/hk-{d['day']}.parquet"]; d['bucket'] = None
    return drop


def main():
    o = lambda f: os.path.join(sources.OUT, f); idx = json.load(open(o('index.json')))
    if not NEW:
        print(json.dumps({'nothing new': True})); return
    drop = window(idx)
    json.dump(idx, open(o('index.json'), 'w'), indent=1)
    m = manifest.manifest(sources.OUT, sorted(set(DATASET_FILES + BUCKET_FILES)), idx['counts'], idx['built'])
    old = manifest.published(f'https://huggingface.co/buckets/{REPO}/resolve/manifest.json')
    if old: old['files'] = {f: v for f, v in old['files'].items() if not f.startswith('v/')}
    p = manifest.problems(m, old)
    if p and os.environ.get('PUBLISH_FORCE') != '1':
        sys.exit('publish gate: not publishing (PUBLISH_FORCE=1 overrides):\n  ' + '\n  '.join(p))
    for x in p: print(f'publish gate: forced past: {x}', file=sys.stderr)
    json.dump(m, open(o('manifest.json'), 'w'), indent=1)
    create_bucket(REPO, private=False, exist_ok=True)
    batch_bucket_files(REPO, add=[(o(f), f) for f in BUCKET_FILES] + [(o('manifest.json'), 'manifest.json')])   # add first
    api = HfApi(); api.create_repo(REPO, repo_type='dataset', exist_ok=True)
    card = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hf', 'README.md')
    api.create_commit(REPO, repo_type='dataset', commit_message=f"{idx['new_day']}: bake of {idx['built']}",
                      operations=[CommitOperationAdd(f, o(f)) for f in DATASET_FILES + ['manifest.json']] + [CommitOperationAdd('README.md', card)])
    if drop: batch_bucket_files(REPO, delete=drop)
    print(json.dumps({'bucket': f'https://huggingface.co/buckets/{REPO}', 'day': idx['new_day'], 'built': idx['built'],
                      'counts': m['counts'], 'removed_from_bucket': len(drop)}))


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'test':                    # window() self-test, no network
        d = lambda n: {'day': f'2026-10-{n:02d}', 'bucket': ['x'], 'bucket_files': ['traces-000.parquet']}
        idx = {'days': [d(n) for n in range(1, 10)], 'new_day': '2026-10-09'}
        assert window(idx, lambda s: False) == [] and all(x['bucket'] for x in idx['days'])          # Kaggle failed: keep all
        assert window(idx, lambda s: not s.endswith('-01')) == ['v/2026-10-02/traces-000.parquet', 'live/hk-2026-10-02.parquet']
        assert idx['days'][0]['bucket'] and not idx['days'][1]['bucket']                             # unarchived day stays
        print('publish: window self-test ok')
    else:
        main()
