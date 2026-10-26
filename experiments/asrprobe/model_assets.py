"""Pinned model identities used by v3 probes; historical assets stay fixed."""
import hashlib
import json
from pathlib import Path

LOCK_PATH = Path(__file__).with_name("model-assets.json")
LEGACY = "zipformer-bilingual-2023"
CANDIDATE = "zipformer-zh-2025"
PROFILES = {
    LEGACY: {"id": LEGACY, "encoder": "encoder-epoch-99-avg-1.int8.onnx",
             "decoder": "decoder-epoch-99-avg-1.int8.onnx", "joiner": "joiner-epoch-99-avg-1.int8.onnx",
             "tokens": "tokens.txt", "model_type": "", "modeling_unit": ""},
    CANDIDATE: {"id": CANDIDATE, "encoder": "encoder.int8.onnx", "decoder": "decoder.onnx",
                "joiner": "joiner.int8.onnx", "tokens": "tokens.txt", "model_type": "", "modeling_unit": "cjkchar"},
}
STREAM_CONFIG = {"sample_rate": 16000, "feature_dim": 80, "chunk_samples": 1600,
                 "leading_padding_samples": 4800, "trailing_padding_samples": 9600, "enable_endpoint": 0}


def expected_model(profile_id):
    """Read expectations from the source lock, never from a probe report."""
    raw = LOCK_PATH.read_bytes()
    lock = json.loads(raw)
    if lock["schema"] != "tide.asrprobe.model-assets.v1":
        raise ValueError("unsupported model asset lock")
    return {"profile": dict(PROFILES[profile_id]), "files": dict(lock["profiles"][profile_id]["files"]),
            "asset_lock_sha256": hashlib.sha256(raw).hexdigest()}
