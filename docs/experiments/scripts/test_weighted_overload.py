"""过载选点、探测顺序与分母、失败保留和来源保护测试；不作为性能证据。"""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest, distribution, save
from run_weighted_overload import (CONDITION_KEYS, DEFAULT_SOURCE, EXPERIMENT, overload_decision,
                                   run_overload, selection_from_source, validate_overload_conditions, validate_selection)
from summarize_pressure_sweep import pressure_profile, summarize
from test_strategy_comparison import fixture as short_fixture, gateway_log
from test_heterogeneous_workers import startup
from worker_comparison_contract import experiment_schedule, validate_manifest


def fixture():
    m=short_fixture("weighted_round_robin");m["experiment"]=EXPERIMENT
    m["schedule"].update(session_counts=[16],repetitions=3)
    m["recovery_probe"]=dict(sessions=2,audio_bytes=64000,session_timeout_s=5,sampling_duration_s=10)
    template=m["batches"][3];batches=[]
    artifacts={k:v for k,v in m["artifact_sha256"].items() if k in ("gateway.log","asr-worker-1.log","asr-worker-2.log")}
    for label,n,_ in experiment_schedule(EXPERIMENT,2):
        b=copy.deepcopy(template);old=b["label"];b.update(label=label,planned_sessions=n)
        for item in b["sampling"].values():
            item["directory"]=item["directory"].replace(old,label);item["command"][-1]=item["command"][-1].replace(old,label)
            item["command"][4]="-duration="+("10s" if label=="warmup" or label.startswith("recovery-") else "40s")
            item["exit_code"]=0
            for suffix in ("/samples.jsonl","/manifest.json",".log"):artifacts[item["directory"]+suffix]="hash"
        for suffix in (".json",".log"):artifacts[label+suffix]="hash"
        batches.append(b)
    m.update(batches=batches,artifact_sha256=artifacts)
    return m


class WeightedOverloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.selection=selection_from_source(DEFAULT_SOURCE)

    def test_fixed_profile_schedule_and_contract(self):
        self.assertEqual(pressure_profile(EXPERIMENT),dict(experiment=EXPERIMENT,counts=(16,),repetitions=3,audio_bytes=640000,timeout_s=35,duration_s=40))
        self.assertEqual(experiment_schedule(EXPERIMENT,2),[("warmup",2,64000),("r1-n16",16,640000),("recovery-r1",2,64000),("r2-n16",16,640000),("recovery-r2",2,64000),("r3-n16",16,640000),("recovery-r3",2,64000)])
        m=fixture();before=copy.deepcopy(m);validate_manifest(m);self.assertEqual(m,before)

    def test_probe_missing_reordered_wrong_budget_or_policy_rejected(self):
        for mode in ("missing","order","duration","probe_budget","policy","weights"):
            m=fixture()
            if mode=="missing":m["batches"].pop()
            elif mode=="order":m["batches"][1:3]=reversed(m["batches"][1:3])
            elif mode=="duration":m["batches"][2]["sampling"]["worker2"]["command"][4]="-duration=40s"
            elif mode=="probe_budget":m["recovery_probe"]["session_timeout_s"]=35
            elif mode=="policy":m["policy"]="round_robin"
            else:m["weights"]=[1,2]
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_invalid_topology_and_policy_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as t:
            for count,policy in ((1,"weighted_round_robin"),(2,"round_robin")):
                p=Path(t)/"unused"
                with self.subTest(count=count,policy=policy),self.assertRaises(ValueError):run(p,pressure=True,profile=pressure_profile(EXPERIMENT),worker_count=count,policy=policy)
                self.assertFalse(p.exists())

    def test_selection_requires_complete_extended_pass(self):
        groups=self.selection["source_groups"];before=copy.deepcopy(groups)
        self.assertEqual(overload_decision(groups)["sessions"],16);self.assertEqual(groups,before)
        for mode in ("missing","count","passes","outcomes","lag","tail"):
            g=copy.deepcopy(groups)
            if mode=="missing":g=[]
            elif mode=="count":g[0]["attempts"]=12
            elif mode=="passes":g[0]["batches_meeting_criteria"]=1
            elif mode=="outcomes":g[0]["outcomes"]={"completed":23,"failed":1}
            elif mode=="lag":g[0]["max_audio_schedule_lag_ns"]=100_000_001
            else:g[0]["tail_completed_only"]["p95_ns"]=250_000_001
            with self.subTest(mode=mode),self.assertRaises(ValueError):overload_decision(g)

    def test_selection_conditions_and_reference_tampering(self):
        validate_selection(self.selection)
        for mode in ("sessions","source","hash","short","condition"):
            s=copy.deepcopy(self.selection)
            if mode=="sessions":s["sessions"]=18
            elif mode=="source":s["source_experiment"]="other"
            elif mode=="hash":del s["source_artifact_sha256"]["r2-n12-worker2/samples.jsonl"]
            elif mode=="short":s["short_reference"]["sessions"]=10
            else:s["source_conditions"]["weights"]=[1,2]
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_selection(s)

    def test_changed_source_files_rejected_without_reanalysis_of_bad_data(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/"copy";shutil.copytree(DEFAULT_SOURCE,p)
            groups=p/"groups.json";original=groups.read_bytes();groups.write_bytes(b"{}")
            with self.assertRaisesRegex(ValueError,"reference changed"):selection_from_source(p)
            groups.write_bytes(original);(p/"r2-n12-worker2/samples.jsonl").write_bytes(b"")
            with self.assertRaisesRegex(ValueError,"artifact changed"):selection_from_source(p)

    def test_runtime_source_drift_stops_before_port_or_process(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/"attempt"
            with patch("run_joint_observation_baseline.capture",return_value=""),patch("run_joint_observation_baseline.digest",return_value="hash"),patch("run_joint_observation_baseline.check_gateway_port") as port,patch("run_joint_observation_baseline.subprocess.Popen") as process:
                with self.assertRaisesRegex(ValueError,"overload condition differs: source_sha256"):
                    run(p,pressure=True,profile=pressure_profile(EXPERIMENT),worker_count=2,policy="weighted_round_robin",selection=self.selection)
                port.assert_not_called();process.assert_not_called()
            self.assertEqual(json.loads((p/"manifest.json").read_text())["status"],"error")

    def test_analyzer_keeps_formal_and_probe_denominators_separate_even_if_probe_fails(self):
        observed=json.loads((DEFAULT_SOURCE/"groups.json").read_text())["batches"][0]
        for fail_probe in (False,True):
            with self.subTest(fail_probe=fail_probe),tempfile.TemporaryDirectory() as t:
                p=Path(t);m=fixture();m["selection"]=copy.deepcopy(self.selection)
                for key in CONDITION_KEYS:m[key]=copy.deepcopy(self.selection["source_conditions"][key])
                for b in m["batches"]:b.update(started_at="2026-10-03T00:00:00Z",finished_at="2026-10-03T00:00:02Z",exit_code=0 if b["label"]=="warmup" or (b["label"].startswith("recovery-") and not fail_probe) else 1)
                for name in m["artifact_sha256"]:
                    f=p/name;f.parent.mkdir(parents=True,exist_ok=True);f.write_text("")
                for i,w in enumerate(m["workers"],1):(p/f"asr-worker-{i}.log").write_text(startup(w))
                (p/"gateway.log").write_text(gateway_log(m))
                m["artifact_sha256"]={name:digest(p/name) for name in m["artifact_sha256"]};save(p/"manifest.json",m)
                def load(path,n,size,timeout):
                    recovery=path.stem.startswith("recovery-");formal=path.stem.startswith("r") and not recovery
                    self.assertEqual(timeout,5 if recovery else 35);self.assertEqual(size,640000 if formal else 64000)
                    failed=formal or (recovery and fail_probe)
                    rows=[dict(outcome="completed",error=None,tail_ns=20_000_000,audio_bytes_written=size,audio_schedule_samples=size//3200,max_audio_schedule_lag_ns=0) for _ in range(n)]
                    if failed:rows[-1].update(outcome="failed",error="test failure",tail_ns=None,audio_bytes_written=3200)
                    return dict(sessions=rows,criteria_met=not failed,expected_exit_code=int(failed))
                def gateway(*args):return copy.deepcopy(observed["gateway"])
                def worker(*args):return copy.deepcopy(observed["workers"]["worker1"])
                with patch("summarize_pressure_sweep.check_pressure_report",side_effect=load),patch("summarize_pressure_sweep.check_pressure_gateway",side_effect=gateway),patch("summarize_pressure_sweep.check_worker_sampling",side_effect=worker),patch("summarize_pressure_sweep.phase_observations",return_value=[]),contextlib.redirect_stdout(io.StringIO()):result=summarize(p)
                g=result["groups"][0];self.assertEqual(g["attempts"],48);self.assertEqual(g["outcomes"],{"completed":45,"failed":3})
                self.assertEqual(len(result["batches"]),3);self.assertEqual(g["tail_completed_only"]["samples"],45)
                self.assertEqual(len(result["recovery_checks"]),3);self.assertEqual(sum(len(b["load"]["sessions"]) for b in result["recovery_checks"]),6)
                self.assertEqual(result["all_recovery_meet_criteria"],not fail_probe)
                self.assertEqual(sum(s["outcome"]=="failed" for b in result["recovery_checks"] for s in b["load"]["sessions"]),3 if fail_probe else 0)

    def test_wrapper_binds_source_and_fixed_policy(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/"new"
            with patch("run_weighted_overload.selection_from_source",return_value=self.selection) as source,patch("run_weighted_overload.run") as invoke,patch("run_weighted_overload.summarize",return_value={"done":True}):
                self.assertEqual(run_overload(p),{"done":True})
            source.assert_called_once_with(DEFAULT_SOURCE)
            invoke.assert_called_once_with(p,pressure=True,profile=pressure_profile(EXPERIMENT),worker_count=2,policy="weighted_round_robin",selection=self.selection)


if __name__=="__main__":unittest.main(verbosity=2)
