"""Publish gate and output manifest (the same file in every Milleleaf pipeline; a template repo once there are three).
manifest(): sha256 and size of every published file, the bake's counts, and the declared sources (sources.json).
gate(): run before any upload. Compares the new manifest with the one already published and refuses (exit 1) when a count
disappears or drops by more than DROP, or a file is empty; nothing published yet (first run) passes. PUBLISH_FORCE=1 publishes
anyway, for a drop that is real (the workflow's manual run has a 'force' box).
Usage: python3 manifest.py    (self-test, no data, no network: CI runs it)"""
import hashlib, json, os, sys, urllib.error, urllib.request

DROP = 0.10

def sha256(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()

def manifest(out, files, counts, built, here=os.path.dirname(os.path.abspath(__file__))):
    """out: the folder the published paths are relative to; files: those paths; counts: {name: int}"""
    return {'built': built, 'counts': counts,
            'files': {f: {'sha256': sha256(os.path.join(out, f)), 'bytes': os.path.getsize(os.path.join(out, f))} for f in sorted(files)},
            'sources': json.load(open(os.path.join(here, 'sources.json')))}

def published(url):
    """the manifest now online, or None before the first publish"""
    try: return json.load(urllib.request.urlopen(url, timeout=60))
    except urllib.error.HTTPError as e:
        if e.code in (401, 404): return None                          # HF answers 401 for a repo that does not exist yet
        raise

def problems(new, old):
    p = [f'{f} is empty' for f, v in new['files'].items() if not v['bytes']]
    for k, v in (old or {}).get('counts', {}).items():
        if k not in new['counts']: p.append(f'count {k} is gone (was {v})')
        elif new['counts'][k] < v * (1 - DROP): p.append(f'count {k} dropped {v} -> {new["counts"][k]} (more than {DROP:.0%})')
    p += [f'{f} is gone' for f in (old or {}).get('files', {}) if f not in new['files']]
    return p

def gate(new, url):
    p = problems(new, published(url))
    if p and os.environ.get('PUBLISH_FORCE') != '1':
        sys.exit('publish gate: not publishing (PUBLISH_FORCE=1 overrides):\n  ' + '\n  '.join(p))
    for x in p: print(f'publish gate: forced past: {x}', file=sys.stderr)

if __name__ == '__main__':
    f = lambda n, b=1: {'files': {'a': {'bytes': b}}, 'counts': {'n': n}}
    assert problems(f(100), None) == [] and problems(f(95), f(100)) == []
    assert problems(f(89), f(100)) and problems(f(100, 0), f(100)) and problems(f(200), f(100)) == []
    assert problems({'files': {'a': {'bytes': 1}}, 'counts': {}}, f(1))                                   # a count vanished
    assert problems({'files': {'b': {'bytes': 1}}, 'counts': {'n': 1}}, f(1))                             # a file vanished
    print('manifest: self-test ok')
