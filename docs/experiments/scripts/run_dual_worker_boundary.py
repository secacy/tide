#!/usr/bin/env python3
"""在已有双 Worker 18 达标、20 未达标证据基础上，固定细测 19 场三批。"""
import argparse
import contextlib
import io
import json
from pathlib import Path
import shutil
import tempfile

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize

DEFAULT_SOURCE = Path(__file__).resolve().parent.parent/"results/dual-worker-sweep-2026-10-02"


def decision_from_groups(groups):
    """仅接受已完整重复的 16/18/20 档位，按已声明的相邻档位规则选择 19。"""
    require(isinstance(groups, list) and [g['planned_sessions'] for g in groups] == [16,18,20], "requires full dual-worker sweep")
    for g in groups:
        p = g['batches_meeting_criteria']
        require(type(p) is int and 0 <= p <= 3 and g['attempts'] == g['planned_sessions']*3, "invalid sweep repetitions")
        require(type(g['all_repetitions_meet_criteria']) is bool and g['all_repetitions_meet_criteria'] == (p == 3), "inconsistent sweep criteria")
    require([g['batches_meeting_criteria'] for g in groups] == [3,3,0], "source does not justify fixed 19-session refinement")
    return dict(rule_version=1, refinement_sessions=19, lower_repeated_pass=18, upper_repeated_unmet=20)


def source_selection(source):
    """复核历史哈希和原始数据，在临时副本中重算；保留来源而不重写旧证据。"""
    m = json.loads((source/'manifest.json').read_text())
    require(m.get('experiment') == 'dual_worker_sweep' and m.get('status') == 'passed', "not completed dual-worker sweep")
    a = json.loads((source/'analysis-manifest.json').read_text())
    require(digest(source/'manifest.json') == a['input_sha256']['manifest.json'], "source manifest changed")
    require(digest(source/'groups.json') == a['groups_sha256'], "source groups changed")
    with tempfile.TemporaryDirectory(prefix='tide-dual-boundary-source-') as t:
        target=Path(t)/'source';shutil.copytree(source,target)
        with contextlib.redirect_stdout(io.StringIO()):result=summarize(target)
    require(result == json.loads((source/'groups.json').read_text()), "source differs from raw evidence")
    return dict(**decision_from_groups(result['groups']), source_directory=str(source.resolve()),
                source_manifest_sha256=digest(source/'manifest.json'), source_groups_sha256=digest(source/'groups.json'),
                source_commit=m['source_commit'], source_sha256=m['source_sha256'], source_groups=result['groups'])


def run_boundary(directory, source=DEFAULT_SOURCE):
    """先复核选点依据再启动固定三批；任何业务失败都保留，不自动增加重试。"""
    selection=source_selection(source)
    run(directory, pressure=True, profile=pressure_profile('dual_worker_boundary_short'), worker_count=2, selection=selection)
    return summarize(directory)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True,help='new directory; all attempts retained')
    parser.add_argument('--source',type=Path,default=DEFAULT_SOURCE,help='completed dual-worker 16/18/20 sweep')
    parser.add_argument('--analyze-only',action='store_true',help='recheck completed refinement without generating load')
    args=parser.parse_args();directory=args.output.resolve()
    if args.analyze_only:
        require(json.loads((directory/'manifest.json').read_text()).get('experiment') == 'dual_worker_boundary_short', 'not a refinement experiment')
        summarize(directory)
    else:
        run_boundary(directory,args.source.resolve())
