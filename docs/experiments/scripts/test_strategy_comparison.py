"""策略实验契约、真实格式日志、失败分母与固定编排测试；不作为性能证据。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest, save
from run_strategy_comparison import EXPERIMENT, ORDER, POLICIES, aggregate, compare, run_comparison, validate_cohorts
from summarize_pressure_sweep import pressure_profile
from test_heterogeneous_workers import fixture as heterogeneous_fixture, startup
from worker_comparison_contract import validate_manifest, validate_startup_logs


def fixture(policy="round_robin"):
    m = heterogeneous_fixture()
    m.update(experiment="dual_worker_strategy", policy=policy,
             weights=[2, 1] if policy == "weighted_round_robin" else None)
    m["schedule"]["repetitions"] = 1
    m["batches"] = m["batches"][:4]
    m["commands"][2].append("-worker-strategy="+policy)
    if m["weights"]:
        m["commands"][2].append("-worker-weights=2,1")
    m["artifact_sha256"]["gateway.log"] = "hash"
    return m


def gateway_log(m):
    return ('2026/10/03 12:00:00 INFO gateway backend configuration '
            f'strategy={m["policy"]} workers="'+"["+" ".join(w["address"] for w in m["workers"])+']" '
            + ('weights="[2 1]"\n' if m["weights"] else 'weights=[]\n'))


class StrategyComparisonTests(unittest.TestCase):
    def test_fixed_profile_and_both_manifests(self):
        self.assertEqual(pressure_profile("dual_worker_strategy"), dict(experiment="dual_worker_strategy",
                         counts=(8,10,12), repetitions=1, audio_bytes=640000, timeout_s=35, duration_s=40))
        for policy in POLICIES:
            m=fixture(policy); before=copy.deepcopy(m); validate_manifest(m); self.assertEqual(m,before)

    def test_policy_weight_command_or_evidence_drift_rejected(self):
        for mode in ("policy", "weights", "position", "command", "log", "legacy"):
            m=fixture("weighted_round_robin")
            if mode=="policy":m["policy"]="other"
            elif mode=="weights":m["weights"]=None
            elif mode=="position":m["weights"]=[1,2]
            elif mode=="command":m["commands"][2][-1]="-worker-weights=1,2"
            elif mode=="log":del m["artifact_sha256"]["gateway.log"]
            else:m=heterogeneous_fixture();m["policy"]="weighted_round_robin"
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_invalid_policy_or_topology_creates_no_output(self):
        cases=[("dual_worker_strategy",2,"other"),("dual_worker_heterogeneous",2,"weighted_round_robin"),
               ("dual_worker_strategy",1,"weighted_round_robin")]
        with tempfile.TemporaryDirectory() as t:
            for experiment,count,policy in cases:
                p=Path(t)/"unused"
                with self.subTest(experiment=experiment,count=count,policy=policy),self.assertRaises(ValueError):
                    run(p,pressure=True,profile=pressure_profile(experiment),worker_count=count,policy=policy)
                self.assertFalse(p.exists())

    def test_quoted_startup_logs_validate_both_actual_strategies(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            for policy in POLICIES:
                m=fixture(policy)
                for i,w in enumerate(m["workers"],1):(p/f"asr-worker-{i}.log").write_text(startup(w))
                (p/"gateway.log").write_text(gateway_log(m))
                validate_startup_logs(p,m)

    def test_startup_misconfiguration_and_missing_duplicate_rejected(self):
        m=fixture("weighted_round_robin")
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            for i,w in enumerate(m["workers"],1):(p/f"asr-worker-{i}.log").write_text(startup(w))
            original=gateway_log(m)
            variants=["",original*2,original.replace("[2 1]","[1 2]"),
                      original.replace("strategy=weighted_round_robin","strategy=round_robin"),
                      original.replace("127.0.0.1:50051 127.0.0.1:50052","127.0.0.1:50052 127.0.0.1:50051"),
                      original.rstrip()+" weights=[]\n"]
            for text in variants:
                (p/"gateway.log").write_text(text)
                with self.subTest(text=text),self.assertRaises(ValueError):validate_startup_logs(p,m)

    def test_cohorts_equal_conditions_without_mutation(self):
        manifests=[fixture(i["policy"]) for i in ORDER];before=copy.deepcopy(manifests)
        validate_cohorts(list(ORDER),manifests);self.assertEqual(manifests,before)
        self.assertEqual([i["policy"] for i in ORDER], [POLICIES[0],POLICIES[1],POLICIES[1],POLICIES[0],POLICIES[0],POLICIES[1]])

    def test_cohort_condition_or_order_drift_rejected(self):
        for mode in ("binary_sha256", "source_sha256", "environment", "policy", "order", "missing"):
            manifests=[fixture(i["policy"]) for i in ORDER];order=copy.deepcopy(list(ORDER))
            if mode=="order":order.reverse()
            elif mode=="missing":manifests.pop()
            elif mode=="policy":manifests[1]=fixture()
            else:manifests[-1][mode]="changed"
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_cohorts(order,manifests)

    def test_aggregate_recomputes_raw_tail_and_keeps_failures(self):
        results=[]
        for rep in range(3):
            rows=[dict(outcome="completed",error=None,tail_ns=rep*100+i,audio_bytes_written=640000,
                       audio_schedule_samples=200,max_audio_schedule_lag_ns=i) for i in range(7)]
            rows.append(dict(outcome="failed",error="audio backlog exceeded",tail_ns=None,audio_bytes_written=32000,
                             audio_schedule_samples=10,max_audio_schedule_lag_ns=20))
            worker=dict(samples=12,successful_samples=11,failed_samples=1,window_successful_samples=10,
                        window_busy_samples=8,window_waiting_samples=5,waiting_peaks=[3],in_use_peaks=[1],max_success_gap_ns=100)
            g=dict(planned_sessions=8,gateway_observed_peaks=[8],gateway=dict(samples=12,failed_samples=1,window_successful_samples=10,max_success_gap_ns=100),
                   workers={w:copy.deepcopy(worker) for w in ("worker1","worker2")})
            results.append(dict(groups=[g],batches=[dict(planned_sessions=8,load=dict(sessions=rows,criteria_met=False))]))
        before=copy.deepcopy(results);g=aggregate(results,8);self.assertEqual(results,before)
        self.assertEqual(g["attempts"],24);self.assertEqual(g["outcomes"],{"completed":21,"failed":3})
        self.assertEqual(g["failure_errors"],{"audio backlog exceeded":3});self.assertEqual(g["completion_rate"],21/24)
        self.assertEqual(g["tail_completed_only"]["p95_ns"],205)
        self.assertEqual(g["tail_completed_only"]["samples"],21)
        self.assertEqual(g["workers"]["worker2"]["waiting_peaks"],[3,3,3])
        self.assertEqual(g["batches_meeting_criteria"],0)

    def test_driver_fixed_order_and_continues_after_business_failure(self):
        with tempfile.TemporaryDirectory() as t:
            directory=Path(t)/"new"
            def child(path,**kwargs):
                path.mkdir();save(path/"manifest.json",dict(status="passed",business_failure=True))
            with patch("run_strategy_comparison.run",side_effect=child) as invoke,patch("run_strategy_comparison.summarize"),patch("run_strategy_comparison.compare") as compare:
                run_comparison(directory)
            self.assertEqual(invoke.call_count,6)
            self.assertEqual([c.args[0].name for c in invoke.call_args_list],[i["directory"] for i in ORDER])
            self.assertEqual([c.kwargs["policy"] for c in invoke.call_args_list],[i["policy"] for i in ORDER])
            self.assertTrue(all(c.kwargs["worker_count"]==2 for c in invoke.call_args_list))
            compare.assert_called_once_with(directory)
            m=json.loads((directory/"manifest.json").read_text());self.assertEqual(m["status"],"passed")
            self.assertEqual(len(m["artifact_sha256"]),6)

    def test_analyzer_checks_hashes_before_reanalysis(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t);(p/"evidence.log").write_text("original")
            save(p/"manifest.json",dict(status="passed",experiment=EXPERIMENT,order=list(ORDER),
                 artifact_sha256={"evidence.log":digest(p/"evidence.log")}))
            (p/"evidence.log").write_text("changed")
            with patch("run_strategy_comparison.summarize") as summarize_mock:
                with self.assertRaisesRegex(ValueError,"artifact changed"):compare(p)
                summarize_mock.assert_not_called()

    def test_analyzer_rejects_derived_groups_drift(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t);child=p/ORDER[0]["directory"];child.mkdir()
            save(child/"manifest.json",fixture());save(child/"groups.json",{"changed":True})
            save(p/"manifest.json",dict(status="passed",experiment=EXPERIMENT,order=list(ORDER),
                 artifact_sha256={str(f.relative_to(p)):digest(f) for f in child.iterdir()}))
            with patch("run_strategy_comparison.summarize",return_value={"changed":False}),self.assertRaisesRegex(ValueError,"derived groups differ"):
                compare(p)

    def test_acquisition_error_is_retained_without_replacement(self):
        with tempfile.TemporaryDirectory() as t:
            directory=Path(t)/"new"
            def fail(path,**kwargs):
                path.mkdir();(path/"failure.log").write_text("sampling failed");raise RuntimeError("sampling failed")
            with patch("run_strategy_comparison.run",side_effect=fail) as invoke,patch("run_strategy_comparison.compare") as compare:
                with self.assertRaisesRegex(RuntimeError,"sampling failed"):run_comparison(directory)
            invoke.assert_called_once();compare.assert_not_called()
            m=json.loads((directory/"manifest.json").read_text());self.assertEqual(m["status"],"error")
            self.assertIn(ORDER[0]["directory"]+"/failure.log",m["artifact_sha256"])


if __name__=="__main__":unittest.main(verbosity=2)
