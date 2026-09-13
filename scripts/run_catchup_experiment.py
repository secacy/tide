#!/usr/bin/env python3
"""EXP-015: continuous capture with default bounded recovery and controlled Worker rate."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time


def overlay_source(source):
    replacements = [
        ('deadline = time.Now().Add(cfg.Budget)', 'deadline = time.Now().Add(cfg.Budget)\ncatchupRecord(runCtx, "recovery_start", "", checkpoint, captured.Load(), deadline, "")'),
        ('report.Attempts++', 'report.Attempts++\ncatchupRecord(runCtx, "attempt_start", a.id, from, captured.Load(), deadline, "")'),
        ('attemptErr, fatal := c.attemptIO(attemptCtx, start, a.input, emit)', 'attemptErr, fatal := c.attemptIO(attemptCtx, start, a.input, emit)\ncatchupRecord(runCtx, "attempt_exit", a.id, 0, captured.Load(), time.Time{}, fmt.Sprint(attemptErr))'),
        ('active.ready = true', 'active.ready = true\ncatchupRecord(runCtx, "ready", active.id, checkpoint, captured.Load(), deadline, "")'),
        ('cache.drop(checkpoint)\n\t\t\tif cfg.Observe', 'cache.drop(checkpoint)\ncatchupRecord(runCtx, "applied", active.id, checkpoint, captured.Load(), deadline, "")\n\t\t\tif cfg.Observe'),
        ('deadline = time.Time{}', 'if !deadline.IsZero() { catchupRecord(runCtx, "caught_up", active.id, checkpoint, checkpoint, deadline, "") }\ndeadline = time.Time{}'),
    ]
    for old, new in replacements:
        if source.count(old) != 1:
            raise ValueError(f'observation anchor changed: {old}')
        source = source.replace(old, new)
    return source + '\nfunc init() { catchupOverlayEnabled = true }\n'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--count', type=int, default=2)
    p.add_argument('--race', action='store_true')
    args = p.parse_args()
    if args.count < 1:
        p.error('count must be positive')
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    meta_path = output.with_suffix(output.suffix + '.meta.json')
    if output.exists() or meta_path.exists():
        p.error('preserve existing evidence; choose a new output path')
    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env['TIDE_RUN_CATCHUP'] = '1'
    sources = [root/'go.mod', root/'go.sum', Path(__file__).resolve(), root/'scripts/summarize_catchup_experiment.py']
    sources += sorted((root/'internal').rglob('*.go')) + sorted((root/'proto').rglob('*.go'))
    with tempfile.TemporaryDirectory(prefix='tide-catchup-') as d:
        temp = Path(d)
        source_path = root/'internal/wsclient/recovery.go'
        transformed = overlay_source(source_path.read_text())
        (temp/'recovery.go').write_text(transformed)
        overlay = temp/'overlay.json'
        overlay.write_text(json.dumps({'Replace': {str(source_path): str(temp/'recovery.go')}}))
        command = ['go', 'test', '-mod=readonly', '-tags=tide_recovery', '-overlay='+str(overlay), './internal/wsclient',
                   '-run', '^TestExperimentRecoveryCatchup$', f'-count={args.count}', '-timeout=5m', '-json']
        if args.race:
            command.insert(2, '-race')
        meta = {
            'experiment': 'EXP-015', 'commit': subprocess.check_output(['git','rev-parse','HEAD'], cwd=root, text=True).strip(),
            'source_sha256': {str(s.relative_to(root)): hashlib.sha256(s.read_bytes()).hexdigest() for s in sources},
            'overlay_source_sha256': hashlib.sha256(transformed.encode()).hexdigest(),
            'overlay': 'only observes coordinator transitions; generated from hashed script; no branch or configuration substitutions',
            'count': args.count, 'race': args.race, 'command': command,
            'go_version': subprocess.check_output(['go','version'], cwd=root, env=env, text=True).strip(),
            'platform': platform.platform(), 'logical_cpu_count': os.cpu_count(),
            'environment': {k: env.get(k) for k in ['GOCACHE','GOPROXY','GOSUMDB','GOGC','GOMEMLIMIT','GOMAXPROCS']},
            'fault_boundary': 'first Worker RPC fails before checkpoint; HTTP admission gate returns 503 for 2 seconds; not silent packet loss',
            'clock': 'real 100ms paced capture, 16s source, default heartbeat/buffer/recovery/processing settings',
        }
        meta_path.write_text(json.dumps(meta, indent=2)+'\n')
        started = time.monotonic()
        with output.open('x') as stream:
            result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
        meta.update(exit_code=result.returncode, elapsed_seconds=time.monotonic()-started, raw_sha256=hashlib.sha256(output.read_bytes()).hexdigest())
        meta_path.write_text(json.dumps(meta, indent=2)+'\n')
    print(f'exit={result.returncode}; results={output}')
    raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
