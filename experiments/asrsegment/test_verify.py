import copy
import hashlib
import struct
import unittest

from prepare import sample_digest
from verify import check_callbacks, check_native, profile_seconds, window_trends


class SegmentVerification(unittest.TestCase):
    def test_explicit_duration_profiles_and_legacy(self):
        self.assertEqual(profile_seconds({"schema": "tide.segment120.v1"}), 120)
        self.assertEqual(profile_seconds({"schema": "tide.segment600.v1", "formal_seconds": 600}), 600)
        for value in (0, 119, 601):
            with self.assertRaises(ValueError):
                profile_seconds({"schema": "tide.segment600.v1", "formal_seconds": value})
        with self.assertRaises(ValueError):
            profile_seconds({"schema": "tide.segment120.v1", "formal_seconds": 600})

    def test_twenty_segments_in_five_windows(self):
        samples = [{"phase": "formal", "elapsed_ns": (i*120+1)*1000000000, "worker_rss_bytes": 100+i,
                    "driver_heap_bytes": 50, "driver_goroutines": 21} for i in range(5)]
        timing = [{"segment": i, "source_read_to_final_ns": i} for i in range(1, 21)]
        result = window_trends(samples, {"FirstReadNS": 0}, timing, 600)
        self.assertEqual([r["boundary_wait_ns"]["median"] for r in result], [2.5, 6.5, 10.5, 14.5, 18.5])
        self.assertEqual(result[-1]["last_rss_bytes"], 104)
        with self.assertRaises(ValueError):
            window_trends(samples, {"FirstReadNS": 0}, timing[:-1], 600)

    def test_independent_sample_encoding(self):
        pcm = struct.pack("<hhh", 0, 16384, -32768)
        expected = hashlib.sha256(bytes.fromhex("000000000000003f000080bf")).hexdigest()
        self.assertEqual(sample_digest(pcm), expected)

    def lifecycle(self):
        reference = [{"segments": [{"samples": 2, "sample_f32le_sha256": "h"}]}]
        rows = [{"msg": "model_created", "model_id": 1, "load_ms": 1.0},
                {"msg": "stream_created", "model_id": 1, "segment_id": 1, "initial": True, "create_ns": 1},
                {"msg": "stream_closed", "model_id": 1, "segment_id": 1, "reason": "segment_release", "samples": 2, "sample_f32le_sha256": "h", "release_ns": 1},
                {"msg": "engine_closed", "model_id": 1, "close_ms": 1.0}]
        return rows, reference

    def test_complete_native_lifecycle(self):
        rows, reference = self.lifecycle()
        self.assertEqual(len(check_native(rows, reference)[1]["closed"]), 1)

    def test_missing_stream_close(self):
        rows, reference = self.lifecycle()
        del rows[2]
        with self.assertRaisesRegex(ValueError, "stream release"):
            check_native(rows, reference)

    def test_wrong_sample_digest(self):
        rows, reference = self.lifecycle()
        rows[2]["sample_f32le_sha256"] = "wrong"
        with self.assertRaisesRegex(ValueError, "sample digest"):
            check_native(rows, reference)

    def test_overlapping_streams(self):
        rows, reference = self.lifecycle()
        rows.insert(2, copy.deepcopy(rows[1]))
        with self.assertRaisesRegex(ValueError, "overlapping native stream"):
            check_native(rows, reference)

    def callback(self):
        msg = {"type": "result", "seq": "1", "segmentId": "1", "text": "字", "isFinal": True}
        rows = [{"message": msg, "current_text_bytes": 0}]
        rpc = [{"has_progress": False, "segment_id": "1", "text": "字", "final": True}]
        return rows, rpc

    def test_final_callback(self):
        rows, rpc = self.callback()
        check_callbacks(rows, rpc, ["1"])

    def test_revision_after_final(self):
        rows, rpc = self.callback()
        rows.append(copy.deepcopy(rows[0]))
        rows[1]["message"]["seq"] = "2"
        rpc.append(copy.deepcopy(rpc[0]))
        with self.assertRaisesRegex(ValueError, "revision after final"):
            check_callbacks(rows, rpc, ["1"])

    def test_callback_changed(self):
        rows, rpc = self.callback()
        rows[0]["message"]["text"] = "changed"
        with self.assertRaisesRegex(ValueError, "snapshot changed"):
            check_callbacks(rows, rpc, ["1"])


if __name__ == "__main__":
    unittest.main()
