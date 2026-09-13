#!/usr/bin/env python3
"""Run EXP-013 protocol counterexamples and production admission checks (not a benchmark)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--count', type=int, default=3)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    if args.count < 1:
        parser.error('count must be positive')
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    meta_path = output.with_suffix(output.suffix + '.meta.json')
    if output.exists() or meta_path.exists():
        parser.error('preserve existing samples; choose a new output path')
    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env['TIDE_RUN_RECOVERY_EXPERIMENTS'] = '1'
    command = ['go', 'test', '-mod=readonly', '-tags=tide_recovery', './internal/gateway',
               '-run', '^TestExperimentRecovery', f'-count={args.count}', '-timeout=2m', '-json']
    if args.race:
        command.insert(2, '-race')
    sources = [root / 'go.mod', root / 'go.sum', Path(__file__).resolve(),
               root / 'scripts/summarize_recovery_experiment.py']
    sources += sorted((root / 'internal').rglob('*.go')) + sorted((root / 'proto').rglob('*.go'))
    meta = {
        'experiment': 'EXP-013', 'kind': 'protocol model plus production admission regression; not benchmark',
        'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'source_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        'count': args.count, 'race': args.race, 'command': command,
        'go_version': subprocess.check_output(['go', 'version'], cwd=root, env=env, text=True).strip(),
        'platform': platform.platform(), 'logical_cpu_count': os.cpu_count(),
        'environment': {k: env.get(k) for k in ['GOCACHE', 'GOPROXY', 'GOSUMDB', 'GOGC', 'GOMEMLIMIT', 'GOMAXPROCS']},
        'fault_boundary': 'decode result over gRPC then discard before client application; not network packet loss',
        'clock': 'numbered 100ms PCM blocks, sent unpaced; admission uses a real accelerated 500ms budget',
    }
    with meta_path.open('x') as stream:
        json.dump(meta, stream, indent=2)
        stream.write('\n')
    started = time.monotonic()
    with output.open('x') as stream:
        result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
    meta.update(exit_code=result.returncode, elapsed_seconds=time.monotonic() - started,
                raw_sha256=hashlib.sha256(output.read_bytes()).hexdigest())
    meta_path.write_text(json.dumps(meta, indent=2) + '\n')
    print(f'exit={result.returncode}; results={output}; metadata={meta_path}')
    raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
