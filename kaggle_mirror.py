"""Mirror what publish.py published to a public Kaggle dataset (the same file in every Milleleaf pipeline).
Kaggle's "Remote URL" cannot pull from Hugging Face (every HF download is a 302 to a signed URL, which Kaggle refuses; a direct
200 URL is accepted), so the files are uploaded. Runs after publish.py as its own step with continue-on-error: Kaggle failing
never blocks Hugging Face. Needs KAGGLE_API_TOKEN in the environment (a repo secret; never on argv). The dataset is owned by
Helen's account (helenkwok): a Kaggle group cannot own one. Description = hf/README.md without its YAML header; licence "other"
(per-file terms are in the description).
By default it mirrors EVERYTHING publish.py publishes (DATASET_FILES + BUCKET_FILES + manifest.json), keeping relative paths;
a folder is uploaded as a zip that Kaggle unpacks again (Kaggle allows only 50 top-level files). Mirroring less is a narrowing:
ask Helen first.
Usage: python3 kaggle_mirror.py                                  (reads REPO, DATASET_FILES, BUCKET_FILES, KAGGLE from publish.py)
       python3 kaggle_mirror.py <owner/slug> "<title>" <root> <path>...  (paths relative to root)"""
import csv, io, json, os, re, subprocess, sys, tempfile, time
from datetime import datetime, timedelta, timezone

def local_files(d):
    """every file under the upload folder (symlinks followed), as Kaggle lists it: relative path -> bytes"""
    out = {}
    for top, _, names in os.walk(d, followlinks=True):
        for n in names:
            p = os.path.join(top, n)
            if os.path.relpath(p, d) != 'dataset-metadata.json':
                out[os.path.relpath(p, d)] = os.path.getsize(p)
    return out

def missing(slug, files, since):
    """'' when Kaggle has the dataset ready, updated since `since` (UTC) and holding every file at its size; else why not"""
    run = lambda *a: subprocess.run(['kaggle', 'datasets', *a], capture_output=True, text=True)
    if run('status', slug).stdout.strip() != 'ready':
        return 'not ready'
    owner, name = slug.split('/')
    rows = csv.DictReader(io.StringIO(run('list', '-s', name, '--user', owner, '--csv').stdout))
    updated = next((r['lastUpdated'] for r in rows if r['ref'] == slug), '')
    if updated[:19] < since:
        return f'last updated {updated or "never"}, before this upload'
    have, token = {}, None
    while True:                                                            # Kaggle lists 200 files per page
        lines = run('files', slug, '--page-size', '200', '--csv', *(['--page-token', token] if token else [])).stdout.splitlines()
        token = lines.pop(0).split('=', 1)[1].strip() if lines and lines[0].startswith('Next Page Token') else None
        have |= {r['name']: int(r['size']) for r in csv.DictReader(io.StringIO('\n'.join(lines)))}
        if not token:
            break
    bad = sorted(f for f, n in files.items() if have.get(f) != n)
    return f'{len(bad)} of {len(files)} files missing or another size, e.g. {bad[:3]}' if bad else ''

def landed(slug, files, since, wait=600):
    """after the CLI exits non-zero: Kaggle sometimes answers with a broken reply although the upload went through
    (world-buildings-nl, 2026-10-11). Success only if every file is there at its size within `wait` seconds."""
    deadline = time.time() + wait
    while why := missing(slug, files, since):
        if time.time() > deadline:
            print(f'kaggle: upload not complete after {wait} s: {why}', file=sys.stderr)
            return False
        time.sleep(30)
    print('kaggle: the CLI reported an error, but every file landed', file=sys.stderr)
    return True

def card_body():
    t = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hf', 'README.md'), encoding='utf-8').read()
    return re.sub(r'\A---\n.*?\n---\n', '', t, flags=re.S).strip()

def mirror(slug, title, root, paths):
    exists = subprocess.run(['kaggle', 'datasets', 'files', slug], capture_output=True).returncode == 0
    with tempfile.TemporaryDirectory() as d:
        for p in paths:                                                    # keep relative paths: bus/x.json.gz -> bus/...
            dst = os.path.join(d, p.split('/')[0])
            if not os.path.exists(dst):
                os.symlink(os.path.abspath(os.path.join(root, p.split('/')[0])), dst)
        meta = {'title': title, 'id': slug, 'licenses': [{'name': 'other'}], 'description': card_body()}
        json.dump(meta, open(os.path.join(d, 'dataset-metadata.json'), 'w'), indent=1)
        mode = ['-r', 'zip']                                               # folders go up zipped; Kaggle unpacks them
        if exists:
            cmd = ['kaggle', 'datasets', 'version', '-p', d, '-m', os.environ.get('KAGGLE_MESSAGE', 'update'), '-t', '-q', *mode]
        else:
            cmd = ['kaggle', 'datasets', 'create', '-p', d, '--public', '-t', '-q', *mode]
        since = (datetime.now(timezone.utc) - timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')   # clock skew
        if subprocess.run(cmd).returncode and not landed(slug, local_files(d), since):
            sys.exit(1)
    print(json.dumps({'kaggle': f'https://www.kaggle.com/datasets/{slug}', 'paths': sorted({p.split('/')[0] for p in paths}),
                      'versioned' if exists else 'created': True}))

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if len(sys.argv) < 5:
            sys.exit(__doc__)
        mirror(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:])
    else:
        import publish, sources
        mirror(*publish.KAGGLE, sources.OUT, sorted(set(publish.DATASET_FILES + publish.BUCKET_FILES + ['manifest.json'])))
