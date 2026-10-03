"""加权两分钟观察的选点、来源和预算契约；合成数据不用于性能结果。"""
import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from run_joint_observation_baseline import run
from run_weighted_extended import (CONDITION_KEYS, DEFAULT_SOURCE, EXPERIMENT, extended_decision,
                                   run_extended, selection_from_source, validate_extended_conditions, validate_selection)
from summarize_pressure_sweep import pressure_profile
from test_strategy_comparison import fixture as short_fixture, gateway_log
from test_heterogeneous_workers import startup
from worker_comparison_contract import validate_manifest, validate_startup_logs


def fixture():
    m=short_fixture("weighted_round_robin");m["experiment"]=EXPERIMENT
    m["schedule"].update(session_counts=[12],repetitions=2,audio_bytes=3840000,session_timeout_s=140)
    m["sampling"]["duration_ns"]=150_000_000_000
    m["batches"]=[m["batches"][0],m["batches"][3],copy.deepcopy(m["batches"][3])]
    artifacts={k:v for k,v in m["artifact_sha256"].items() if k in ("asr-worker-1.log","asr-worker-2.log","gateway.log")}
    for i,b in enumerate(m["batches"]):
        old=b["label"];new="warmup" if i==0 else f"r{i}-n12";b["label"]=new
        for item in b["sampling"].values():
            item["directory"]=item["directory"].replace(old,new);item["command"][-1]=item["command"][-1].replace(old,new)
            if i:item["command"][4]="-duration=150s"
        for k,v in m["artifact_sha256"].items():
            if k.startswith(old+"-") or k in (old+".json",old+".log"):artifacts[new+k[len(old):]]=v
    m["artifact_sha256"]=artifacts
    return m


class WeightedExtendedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.selection=selection_from_source(DEFAULT_SOURCE)

    def test_fixed_profile_manifest_and_actual_log_format(self):
        self.assertEqual(pressure_profile(EXPERIMENT),dict(experiment=EXPERIMENT,counts=(12,),repetitions=2,audio_bytes=3840000,timeout_s=140,duration_s=150))
        m=fixture();before=copy.deepcopy(m);validate_manifest(m);self.assertEqual(m,before)
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            for i,w in enumerate(m["workers"],1):(p/f"asr-worker-{i}.log").write_text(startup(w))
            (p/"gateway.log").write_text(gateway_log(m));validate_startup_logs(p,m)
            (p/"gateway.log").write_text(gateway_log(m).replace("[2 1]","[1 2]"))
            with self.assertRaises(ValueError):validate_startup_logs(p,m)

    def test_short_budget_missing_repetition_or_wrong_strategy_rejected(self):
        for mode in ("audio","timeout","duration","sampler_duration","missing","policy","weights"):
            m=fixture()
            if mode=="audio":m["schedule"]["audio_bytes"]=640000
            elif mode=="timeout":m["schedule"]["session_timeout_s"]=35
            elif mode=="duration":m["sampling"]["duration_ns"]=40_000_000_000
            elif mode=="sampler_duration":m["batches"][2]["sampling"]["worker2"]["command"][4]="-duration=40s"
            elif mode=="missing":m["batches"].pop()
            elif mode=="policy":m["policy"]="round_robin"
            else:m["weights"]=[1,2]
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_invalid_topology_or_policy_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as t:
            for count,policy in ((1,"weighted_round_robin"),(2,"round_robin")):
                p=Path(t)/"unused"
                with self.subTest(count=count,policy=policy),self.assertRaises(ValueError):
                    run(p,pressure=True,profile=pressure_profile(EXPERIMENT),worker_count=count,policy=policy)
                self.assertFalse(p.exists())

    def test_selection_requires_complete_repeated_short_pass(self):
        groups=self.selection["source_groups"];before=copy.deepcopy(groups)
        self.assertEqual(extended_decision(groups)["sessions"],12);self.assertEqual(groups,before)
        for mode in ("missing","passes","attempts","boolean","outcomes","completion"):
            changed=copy.deepcopy(groups)
            if mode=="missing":changed.pop()
            elif mode=="passes":changed[2].update(batches_meeting_criteria=2,all_repetitions_meet_criteria=False)
            elif mode=="attempts":changed[2]["attempts"]=24
            elif mode=="boolean":changed[2]["batches_meeting_criteria"]=True
            elif mode=="outcomes":changed[2]["outcomes"]={"completed":35,"failed":1}
            else:changed[2]["completion_rate"]=0.9
            with self.subTest(mode=mode),self.assertRaises(ValueError):extended_decision(changed)

    def test_real_source_selection_and_metadata_tampering(self):
        validate_selection(self.selection)
        for mode in ("sessions","experiment","hash","worker","weights"):
            s=copy.deepcopy(self.selection)
            if mode=="sessions":s["sessions"]=10
            elif mode=="experiment":s["source_experiment"]="other"
            elif mode=="hash":del s["source_artifact_sha256"]["pair-3-weighted_round_robin/manifest.json"]
            elif mode=="worker":s["source_conditions"]["worker_configs"][1]["processing_delay_ns"]=10_000_000
            else:s["source_conditions"]["weights"]=[1,2]
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_selection(s)

    def test_source_file_changes_rejected_without_mutating_archive(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/"copy";shutil.copytree(DEFAULT_SOURCE,p)
            comparison=p/"comparison.json";original=comparison.read_bytes();comparison.write_bytes(b"{}")
            with self.assertRaisesRegex(ValueError,"reference changed"):selection_from_source(p)
            comparison.write_bytes(original)
            raw=p/"pair-3-weighted_round_robin/r1-n12-worker2/samples.jsonl";raw.write_bytes(b"")
            with self.assertRaisesRegex(ValueError,"artifact changed"):selection_from_source(p)

    def test_long_conditions_match_source_and_reject_drift(self):
        m={k:copy.deepcopy(self.selection["source_conditions"][k]) for k in CONDITION_KEYS};m["selection"]=copy.deepcopy(self.selection)
        validate_extended_conditions(m)
        for key in ("source_sha256","environment","go","worker_configs","weights","criteria"):
            changed=copy.deepcopy(m);changed[key]="changed"
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,"extended condition differs"):validate_extended_conditions(changed)

    def test_runtime_source_drift_stops_before_port_or_process_creation(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/"attempt"
            with patch("run_joint_observation_baseline.capture",return_value=""),patch("run_joint_observation_baseline.digest",return_value="hash"),patch("run_joint_observation_baseline.check_gateway_port") as port,patch("run_joint_observation_baseline.subprocess.Popen") as process:
                with self.assertRaisesRegex(ValueError,"extended condition differs: source_sha256"):
                    run(p,pressure=True,profile=pressure_profile(EXPERIMENT),worker_count=2,policy="weighted_round_robin",selection=self.selection)
                port.assert_not_called();process.assert_not_called()
            m=json.loads((p/"manifest.json").read_text())
            self.assertEqual(m["status"],"error");self.assertEqual(m["commands"],[])

    def test_wrapper_verifies_source_and_fixes_policy_and_budget(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/"new"
            with patch("run_weighted_extended.selection_from_source",return_value=self.selection) as source,patch("run_weighted_extended.run") as invoke,patch("run_weighted_extended.summarize",return_value={"done":True}):
                self.assertEqual(run_extended(p),{"done":True})
            source.assert_called_once_with(DEFAULT_SOURCE)
            invoke.assert_called_once_with(p,pressure=True,profile=pressure_profile(EXPERIMENT),worker_count=2,policy="weighted_round_robin",selection=self.selection)


if __name__=="__main__":unittest.main(verbosity=2)
