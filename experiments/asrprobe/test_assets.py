"""Downloader integrity checks use local tiny archives, never network assets."""
import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

from fetch_candidate import extract_verified


class CandidateArchiveTests(unittest.TestCase):
    def test_verified_archive_extracts_pinned_members_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "model.tar.bz2"
            data = b"pinned model contents"
            with tarfile.open(archive, "w:bz2") as bundle:
                for name in ["model/encoder.onnx", "ignored/unselected.txt"]:
                    member = tarfile.TarInfo(name)
                    member.size = len(data)
                    bundle.addfile(member, io.BytesIO(data))
            entry = {"source": {"sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "size": archive.stat().st_size},
                     "files": {"encoder.onnx": hashlib.sha256(data).hexdigest()}, "auxiliary_files": {}}
            extract_verified(archive, root / "output", entry)
            self.assertEqual((root / "output/encoder.onnx").read_bytes(), data)
            self.assertEqual([p.name for p in (root / "output").iterdir()], ["encoder.onnx"])
            with self.assertRaises(FileExistsError):
                extract_verified(archive, root / "output", entry)
            entry["source"]["sha256"] = "wrong"
            with self.assertRaisesRegex(ValueError, "archive size/SHA256"):
                extract_verified(archive, root / "bad-archive", entry)
            self.assertFalse((root / "bad-archive").exists())
            entry["source"]["sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
            entry["files"]["encoder.onnx"] = "wrong"
            with self.assertRaisesRegex(ValueError, "file SHA256"):
                extract_verified(archive, root / "bad-file", entry)
            self.assertFalse((root / "bad-file/encoder.onnx").exists())


if __name__ == "__main__":
    unittest.main()
