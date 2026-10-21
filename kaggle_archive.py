"""The archive: every baked day as its own public Kaggle dataset, created once and never versioned or deleted (Kaggle public
storage is free; Hugging Face keeps only a 7-day window, see publish.py). Runs after bake.py, before publish.py.
  helenkwok/world-flights-<YYYY-MM-DD>  the day, complete: traces-NNN.parquet, acas.parquet, replay/, raw/ (acas, heatmap,
                                        README, licences), live/hk-<D>.parquet (Hong Kong snapshots) and the day's index.json.
                                        Skipped when it already exists: a day is never re-uploaded.
  helenkwok/world-flights               small, versioned each run: index.json (every day dataset: slug, counts, sizes),
                                        latest.json and the newest complete Hong Kong day, live/hk-<D>.parquet
Limits (Kaggle datasets docs, 2026-10): 200 GB per dataset (a day is ~8 GB), 50 top-level files (folders go up zipped and
count as one), no published limit on the number of public datasets. If Kaggle ever refuses one dataset a day, switch day_slug
to weeks (create on the week's last day).
Needs KAGGLE_API_TOKEN. Usage: python3 kaggle_archive.py"""
import json, os, shutil, subprocess, sys, time
import kaggle_mirror, publish, sources
from publish import day_slug, on_kaggle

def main():
    idx = json.load(open(os.path.join(sources.OUT, 'index.json')))
    day = idx.get('new_day')
    if not day:
        print('nothing new'); return
    root = os.path.join(sources.OUT, 'v', day)
    slug = day_slug(day)
    if subprocess.run(['kaggle', 'datasets', 'files', slug], capture_output=True).returncode == 0:
        print(f'{slug} exists: never re-uploaded')                    # a re-bake of an archived day
    else:
        top = sorted(os.listdir(root))
        assert len(top) <= 50, f'{len(top)} top-level paths: Kaggle allows 50'
        kaggle_mirror.mirror(slug, f'Milleleaf: World flights {day}', root, top)   # creates: it does not exist
    for _ in range(30):                                                # Kaggle processes an upload before listing its files
        if on_kaggle(slug): break
        time.sleep(60)
    else:
        sys.exit(f'{slug}: not listed by `kaggle datasets files` after 30 minutes')
    small = [f for f in ['index.json', 'latest.json'] if os.path.exists(os.path.join(sources.OUT, f))]
    hk = f"live/hk-{day}.parquet"
    if os.path.exists(os.path.join(root, hk)):                          # the newest complete Hong Kong day
        os.makedirs(os.path.join(sources.OUT, 'live'), exist_ok=True)
        shutil.copy(os.path.join(root, hk), os.path.join(sources.OUT, hk)); small.append(hk)
    kaggle_mirror.mirror(*publish.KAGGLE, sources.OUT, small)
    print(json.dumps({'day': slug, 'verified': True, 'index': publish.KAGGLE[0]}))


if __name__ == '__main__':
    main()
