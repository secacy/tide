#!/usr/bin/env python3
"""采集 EXP-006-02：一次构建、独占归档、原始事件与源码摘要；正式条件不可缩短。"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

from summarize_group_resume import FORMAL, PROBE, ROOT_TEST, digest, summarize

REPO = Path(__file__).resolve().parents[3]
PLAN = 'docs/experiments/group-resume-baseline.md'
ARTIFACTS = ['preregistration.md', 'build.log', 'go-events.jsonl', 'go-stderr.log',
             'group-resume.test', 'git-status.txt']


def now():
    return datetime.now(timezone.utc).isoformat()


def source_files(repo, plan=PLAN):
    """只快照代码和本实验工具/条件；不读取用户环境、凭据或未跟踪的音频。"""
    names = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=repo).decode().split('\0')
    return sorted({name for name in names if name and
                   (name.endswith(('.go', '.proto')) or name in ('go.mod', 'go.sum', plan) or
                    name.startswith('docs/experiments/scripts/') and name.endswith('.py'))})


def source_hashes(repo, plan=PLAN):
    return {name: digest(repo / name) for name in source_files(repo, plan)}


def collect(output, mode='formal', rounds=3, repo=REPO):
    return collect_profile(output, mode, rounds, repo, plan=PLAN,
                           order=FORMAL if mode == 'formal' else PROBE,
                           root_test=ROOT_TEST.get(mode), test_count=rounds, planned_sessions=40 * rounds,
                           env_overrides={'TIDE_RUN_GROUP_RESUME_EXPERIMENT': '1' if mode == 'formal' else '0'},
                           analyze=summarize)


def collect_profile(output, mode, rounds, repo, *, plan, order, root_test, test_count,
                    planned_sessions, env_overrides, analyze, experiment=None):
    """失败仍保留 manifest/日志；只终止本函数拥有的保持唤醒进程。"""
    if mode not in ROOT_TEST or type(rounds) is not int or rounds not in (1, 2, 3) or mode == 'formal' and rounds != 3:
        raise ValueError('formal requires exactly three rounds; probe allows one to three')
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = dict(version=2 if experiment else 1, mode=mode, planned_rounds=rounds, planned_sessions=planned_sessions,
                    order=order, started_at=now(), status='initializing',
                    build_exit=None, run_exit=None, source_unchanged=False, source_sha256={}, artifacts_sha256={},
                    host=dict(system=platform.system(), machine=platform.machine(), cpus=os.cpu_count()),
                    runtime_environment={name: os.environ.get(name) for name in ('GOMAXPROCS', 'GOGC', 'GOMEMLIMIT', 'GODEBUG')},
                    scope='same-process controlled loopback; probe does not measure formal performance')
    awake = None
    if experiment:
        manifest.update(experiment=experiment, plan=plan, root_test=root_test, test_count=test_count,
                        experiment_environment=env_overrides, collection_directory=str(output))
    try:
        manifest['commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo).decode().strip()
        (output / 'git-status.txt').write_bytes(subprocess.check_output(['git', 'status', '--short'], cwd=repo))
        manifest['source_sha256'] = source_hashes(repo, plan)
        for name in manifest['source_sha256']:
            target = output / 'source' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(repo / name, target)
        shutil.copyfile(output / 'source' / plan, output / 'preregistration.md')
        manifest['go_env'] = json.loads(subprocess.check_output(['go', 'env', '-json', 'GOVERSION', 'GOOS', 'GOARCH', 'GOFLAGS'], cwd=repo))
        if '-race' in manifest['go_env']['GOFLAGS'].split():
            raise ValueError('collection requires a non-race build; use race separately for correctness')
        if source_hashes(repo, plan) != manifest['source_sha256']:
            raise ValueError('source changed while snapshotting')
        build = ['go', 'test', '-c', '-o', str(output / 'group-resume.test'), './internal/gateway']
        manifest['build_argv'] = build
        with (output / 'build.log').open('wb') as stream:
            manifest['build_exit'] = subprocess.run(build, cwd=repo, stdout=stream, stderr=subprocess.STDOUT, timeout=180).returncode
        if manifest['build_exit'] != 0:
            manifest['status'] = 'build_failed'
        else:
            if source_hashes(repo, plan) != manifest['source_sha256']:
                raise ValueError('source changed during build')
            env = os.environ.copy()
            env.update(env_overrides)
            command = ['go', 'tool', 'test2json', '-t', '-p', 'github.com/secacy/tide-artisan/internal/gateway',
                       str(output / 'group-resume.test'), '-test.run=^' + root_test + '$',
                       '-test.count=' + str(test_count), '-test.timeout=5m', '-test.v=test2json']
            manifest['run_argv'] = command
            manifest['awake_guard'] = 'not_requested_for_probe'
            if mode == 'formal' and platform.system() == 'Darwin':
                awake = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-s'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                manifest['awake_guard'] = 'owned_caffeinate'
            elif mode == 'formal':
                manifest['awake_guard'] = 'host_requires_external_wake_policy'
            manifest['run_started_at'] = now()
            with (output / 'go-events.jsonl').open('wb') as stream, (output / 'go-stderr.log').open('wb') as errors:
                manifest['run_exit'] = subprocess.run(command, cwd=repo, env=env, stdout=stream, stderr=errors, timeout=330).returncode
            manifest['status'] = 'collected' if manifest['run_exit'] == 0 else 'run_failed'
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        manifest['status'] = 'collection_failed'
        manifest['collection_error'] = str(exc)
    finally:
        if awake is not None:
            manifest['awake_guard_exit_before_cleanup'] = awake.poll()
            if awake.poll() is None:
                awake.terminate()
            awake.wait(timeout=5)
        try:
            manifest['source_unchanged'] = bool(manifest['source_sha256']) and source_hashes(repo, plan) == manifest['source_sha256']
        except (OSError, subprocess.SubprocessError):
            manifest['source_unchanged'] = False
        manifest['finished_at'] = now()
        manifest['artifacts_sha256'] = {name: digest(output / name) for name in ARTIFACTS if (output / name).exists()}
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    if (output / 'go-events.jsonl').exists():
        result = analyze(output, output / 'analysis')
        print(json.dumps({k: result[k] for k in ('mode', 'evidence_valid', 'planned_sessions', 'collected_batches', 'verified_batches', 'errors')}))
        return 0 if result['evidence_valid'] else 1
    print(json.dumps(dict(status=manifest['status'], planned_sessions=manifest['planned_sessions'], error=manifest.get('collection_error'))))
    return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=['formal', 'probe'], default='formal')
    parser.add_argument('--count', type=int, default=3)
    args = parser.parse_args()
    raise SystemExit(collect(args.output, args.mode, args.count))
