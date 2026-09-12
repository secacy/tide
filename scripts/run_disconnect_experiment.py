#!/usr/bin/env python3
"""Run EXP-010 baseline and heartbeat candidates without editing production code."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', default='.*')
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--race', action='store_true', help='Correctness only, not formal performance evidence')
    args = parser.parse_args()
    if args.count < 1:
        parser.error('count must be positive')
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    meta = output.with_suffix(output.suffix + '.meta.json')
    if output.exists() or meta.exists():
        parser.error('output exists; preserve prior samples')
    output.parent.mkdir(parents=True, exist_ok=True)
    relative = 'internal/gateway/session.go'
    original = (root / relative).read_text()
    site = '\tresult := waitSessionResult(ctx, events, requestClosing, progress)\n'
    if original.count(site) != 1:
        raise RuntimeError('decision observation site changed')
    overlay_source = original.replace(site, site + '\tdisconnectDecision(s, result)\n')
    overlay_source += '\nfunc init() { disconnectOverlayEnabled = true }\n'
    env = os.environ.copy()
    env['TIDE_RUN_DISCONNECT_EXPERIMENTS'] = '1'
    sources = [root / 'go.mod', root / 'go.sum', Path(__file__).resolve(),
               root / 'scripts/summarize_disconnect_experiment.py', root / 'scripts/summarize_streaming_load.py']
    sources += sorted((root / 'internal').rglob('*.go')) + sorted((root / 'proto').rglob('*.go'))
    with tempfile.TemporaryDirectory(prefix='tide-disconnect-') as temp:
        modified = Path(temp) / 'session.go'
        modified.write_text(overlay_source)
        overlay = Path(temp) / 'overlay.json'
        overlay.write_text(json.dumps({'Replace': {str(root / relative): str(modified)}}))
        command = ['go', 'test', '-mod=readonly', '-tags=tide_disconnect', '-overlay', str(overlay),
                   './internal/gateway', '-run', f'^TestExperimentDisconnect$/^{args.case}$',
                   f'-count={args.count}', '-timeout=15m', '-json']
        if args.race:
            command.insert(2, '-race')
        metadata = {
            'experiment': 'EXP-010',
            'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
            'source_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
            'overlay_sha256': {relative: hashlib.sha256(overlay_source.encode()).hexdigest()},
            'overlay_insertion': site.rstrip() + '\n\tdisconnectDecision(s, result)',
            'case': args.case, 'count': args.count, 'race': args.race, 'command': command,
            'transport': 'WS over loopback TCP relay; gRPC bufconn; bounded ordered forwarding pause, NOT packet loss',
            'go_version': subprocess.check_output(['go', 'version'], cwd=root, env=env, text=True).strip(),
            'platform': platform.platform(), 'logical_cpu_count': os.cpu_count(),
            'environment': {k: env.get(k) for k in ('GOCACHE', 'GOPROXY', 'GOSUMDB', 'GOGC', 'GOMEMLIMIT', 'GOMAXPROCS')},
        }
        with meta.open('x') as stream:
            json.dump(metadata, stream, indent=2)
            stream.write('\n')
        started = time.monotonic()
        with output.open('x') as stream:
            completed = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
        metadata['elapsed_seconds'] = time.monotonic() - started
        metadata['exit_code'] = completed.returncode
        metadata['raw_sha256'] = hashlib.sha256(output.read_bytes()).hexdigest()
        meta.write_text(json.dumps(metadata, indent=2) + '\n')
        print(f'exit={completed.returncode}; results={output}; metadata={meta}')
        raise SystemExit(completed.returncode)


if __name__ == '__main__':
    main()
