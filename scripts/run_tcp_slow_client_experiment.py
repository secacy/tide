#!/usr/bin/env python3
"""Run EXP-005 over loopback TCP; preserve source hashes and raw Go JSON."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', default='.*')
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    if args.count < 1:
        parser.error('count must be positive')
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    meta = output.with_suffix(output.suffix + '.meta.json')
    if output.exists() or meta.exists():
        parser.error('output exists; preserve prior samples')
    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env['TIDE_RUN_TCP_EXPERIMENTS'] = '1'
    command = ['go', 'test', '-mod=readonly', '-tags=tide_tcp', './internal/gateway',
               '-run', f'^TestExperimentTCPSlowClient$/^{args.case}$',
               f'-count={args.count}', '-timeout=10m', '-json']
    if args.race:
        command.insert(2, '-race')
    sources = [root / 'go.mod', root / 'go.sum', Path(__file__).resolve()]
    sources += sorted((root / 'internal').rglob('*.go')) + sorted((root / 'proto').rglob('*.go'))
    metadata = {
        'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'source_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        'case': args.case, 'count': args.count, 'race': args.race, 'command': command,
        'transport': 'WebSocket over 127.0.0.1 TCP; Worker gRPC over bufconn; no production overlays',
        'go_version': subprocess.check_output(['go', 'version'], text=True).strip(),
        'platform': platform.platform(), 'logical_cpu_count': os.cpu_count(),
        'environment': {k: env.get(k) for k in ('GOCACHE', 'GOPROXY', 'GOSUMDB', 'GOGC', 'GOMEMLIMIT', 'GOMAXPROCS')},
    }
    meta.write_text(json.dumps(metadata, indent=2) + '\n')
    with output.open('x') as stream:
        completed = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
    metadata['exit_code'] = completed.returncode
    meta.write_text(json.dumps(metadata, indent=2) + '\n')
    print(f'exit={completed.returncode}; results={output}; metadata={meta}')
    raise SystemExit(completed.returncode)


if __name__ == '__main__':
    main()
