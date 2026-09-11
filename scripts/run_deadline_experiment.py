#!/usr/bin/env python3
"""Run EXP-004 policies in an ephemeral Go overlay, preserving raw JSON and sources."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--scenario', default='.*')
    parser.add_argument('--policy', default='.*')
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    if args.count < 1:
        parser.error('count must be positive')
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    meta = output.with_suffix(output.suffix + '.meta.json')
    if output.exists() or meta.exists():
        parser.error('output exists; preserve previous samples')
    output.parent.mkdir(parents=True, exist_ok=True)
    edits = {
        'internal/gateway/audio_queue.go': [
            ('\tq.bytes += len(chunk)\n', '\tq.bytes += len(chunk)\n\tdeadlinePush(q)\n'),
            ('\t\t\tchunk := q.slots[q.head]\n', '\t\t\tdeadlinePop()\n\t\t\tchunk := q.slots[q.head]\n'),
            ('\t\tq.closed = true\n', '\t\tq.closed = true\n\t\tdeadlineEnd()\n'),
        ],
        'internal/gateway/session.go': [
            ('\tctx := s.ctx\n', '\tdefer deadlineWatch(s)()\n\tctx := s.ctx\n'),
        ],
    }
    with tempfile.TemporaryDirectory(prefix='tide-deadline-') as temp:
        replacements, overlay_hashes = {}, {}
        for relative, changes in edits.items():
            source = root / relative
            content = source.read_text()
            for before, after in changes:
                if content.count(before) != 1:
                    raise RuntimeError(f'overlay site changed: {relative}: {before!r}')
                content = content.replace(before, after)
            if source.name == 'audio_queue.go':
                content += '\nfunc init() { deadlineOverlayEnabled = true }\n'
            target = Path(temp) / source.name
            target.write_text(content)
            replacements[str(source)] = str(target)
            overlay_hashes[relative] = hashlib.sha256(content.encode()).hexdigest()
        overlay = Path(temp) / 'overlay.json'
        overlay.write_text(json.dumps({'Replace': replacements}))
        command = ['go', 'test', '-mod=readonly', '-tags=tide_load', '-overlay', str(overlay),
                   './internal/gateway', '-run',
                   f'^TestExperimentDeadline$/^{args.scenario}$/^{args.policy}$',
                   f'-count={args.count}', '-timeout=15m', '-json']
        if args.race:
            command.insert(2, '-race')
        env = os.environ.copy()
        env['TIDE_RUN_EXPERIMENTS'] = '1'
        sources = [root / 'go.mod', root / 'go.sum', Path(__file__).resolve()]
        sources += sorted((root / 'internal').rglob('*.go')) + sorted((root / 'proto').rglob('*.go'))
        metadata = {
            'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
            'source_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
            'overlay_sha256': overlay_hashes, 'command': command,
            'scenario': args.scenario, 'policy': args.policy, 'count': args.count, 'race': args.race,
            'platform': platform.platform(), 'logical_cpu_count': os.cpu_count(),
            'go_version': subprocess.check_output(['go', 'version'], text=True).strip(),
            'environment': {key: env.get(key) for key in ('GOCACHE', 'GOPROXY', 'GOSUMDB', 'GOGC', 'GOMEMLIMIT', 'GOMAXPROCS')},
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
