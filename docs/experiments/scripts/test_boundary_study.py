#!/usr/bin/env python3
"""预定义选点与延长窗口的离线检查；不把合成输入当容量证据。"""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from run_boundary_study import choose_points, selection_from_directory
from summarize_pressure_sweep import check_pressure_data, phase_observations, pressure_profile, summarize
from test_pressure_sweep import fixture

HERE = Path(__file__).resolve().parent


def groups(passes):
    return [dict(planned_sessions=n, attempts=n*3, batches_meeting_criteria=p,
                 all_repetitions_meet_criteria=p == 3) for n,p in zip((9,10,11),passes)]


class BoundaryStudyTests(unittest.TestCase):
    def test_fixed_profiles(self):
        old=pressure_profile()
        self.assertEqual(old,dict(experiment='pressure_sweep',counts=(4,8,12),repetitions=3,audio_bytes=640000,timeout_s=35,duration_s=40))
        short=pressure_profile('boundary_short')
        self.assertEqual(short['counts'],(9,10,11))
        self.assertEqual(short['repetitions'],3)
        for n in (9,10,11):
            extended=pressure_profile('boundary_extended',n)
            self.assertEqual(extended,dict(experiment='boundary_extended',counts=(8,n),repetitions=2,audio_bytes=3840000,timeout_s=140,duration_s=150))
        for kind,n in [('unknown',9),('boundary_extended',None),('boundary_extended',True),('boundary_extended',8),('boundary_extended',12)]:
            with self.assertRaises(ValueError):pressure_profile(kind,n)

    def test_predefined_selection_preserves_variation(self):
        for passed,near,overload,short_pass in [((3,0,0),9,10,True),((3,3,0),10,11,True),((3,3,3),11,12,True),
                                                ((2,1,0),9,11,False),((0,0,0),9,9,False),((0,3,3),11,9,True)]:
            with self.subTest(passed=passed):
                g=groups(passed);before=copy.deepcopy(g);r=choose_points(g)
                self.assertEqual((r['normal'],r['near_boundary'],r['overload_candidate'],r['near_boundary_short_passed']),(8,near,overload,short_pass))
                self.assertEqual(g,before)

    def test_selection_rejects_incomplete_or_inconsistent(self):
        for mode in ('missing','reorder','attempts','bool','too_many','disagree'):
            g=groups((3,0,0))
            if mode=='missing':g.pop()
            elif mode=='reorder':g.reverse()
            elif mode=='attempts':g[0]['attempts']=26
            elif mode=='bool':g[0]['batches_meeting_criteria']=True
            elif mode=='too_many':g[0]['batches_meeting_criteria']=4
            elif mode=='disagree':g[0]['all_repetitions_meet_criteria']=False
            with self.assertRaises(ValueError):choose_points(g)

    def test_timeout_is_part_of_fixture(self):
        d=fixture();d['config']['session']['timeout_ns']=140_000_000_000
        self.assertTrue(check_pressure_data(d,1,160000,140)['criteria_met'])
        with self.assertRaises(ValueError):check_pressure_data(d,1,160000,35)

    def test_phase_analysis_covers_full_extended_window(self):
        def stamp(s):return f'2026-10-02T00:{s//60:02d}:{s%60:02d}Z'
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            for kind in ('gateway','worker'):
                q=p/('r1-n9-'+kind);q.mkdir()
                state=dict(active_sessions=9) if kind=='gateway' else dict(processing_limit_enabled=True,processing=dict(in_use=1,waiting=8))
                rows=[dict(started_at=stamp(s),finished_at=stamp(s),error=None,state=state) for s in (20,65,119,120)]
                (q/'samples.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
            result=phase_observations(p,dict(label='r1-n9',started_at=stamp(0),finished_at=stamp(121)),120)
            self.assertEqual(len(result),24)
            self.assertEqual(result[4]['gateway']['samples'],1)
            self.assertEqual(result[13]['worker']['samples'],1)
            self.assertEqual(result[23]['worker']['samples'],1)
            self.assertEqual(sum(v['gateway']['samples'] for v in result),3)

    def test_historical_pressure_reanalysis_unchanged(self):
        source=HERE.parent/'results/pressure-sweep-2026-10-02'
        original=json.loads((source/'groups.json').read_text())
        with tempfile.TemporaryDirectory() as t:
            target=Path(t)/'copy';shutil.copytree(source,target)
            with contextlib.redirect_stdout(io.StringIO()):got=summarize(target)
            self.assertEqual(got,original)
            with self.assertRaises(ValueError):selection_from_directory(target)
            m=json.loads((target/'manifest.json').read_text());m['schedule']['repetitions']=2
            (target/'manifest.json').write_text(json.dumps(m))
            with self.assertRaises(ValueError):summarize(target)


if __name__=='__main__':
    unittest.main(verbosity=2)
