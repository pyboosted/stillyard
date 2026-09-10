#!/usr/bin/env python3
"""Contract controls for the consumer artifacts; schedule this with Stillyard."""
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


consumer = module("consumer", "machine-resource-consumer.py")
installer = module("installer", "install-windows-daemon.py")


class Contract(unittest.TestCase):
    def test_final_result_and_negative_controls(self):
        raw = {"type": "result", "subtype": "success", "is_error": False,
               "modelUsage": {"claude-sonnet-test": {}},
               "structured_output": {"verdict": "pass", "summary": "fixture", "findings": []}}
        self.assertEqual(consumer.validate_review(raw, "claude-sonnet-")["verdict"]["verdict"], "pass")
        for mutation in [{"is_error": True}, {"subtype": "error_max_turns"}, {"modelUsage": {}},
                         {"modelUsage": {"different-model": {}}},
                         {"structured_output": {"verdict": "pass", "summary": "", "findings": []}},
                         {"structured_output": {"verdict": "findings", "summary": "x", "findings": []}}]:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                consumer.validate_review(raw | mutation, "claude-sonnet-")

    def test_measurement_checks_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source").write_bytes(b"real measured bytes")
            manifest = {"files": {"source": hashlib.sha256(b"real measured bytes").hexdigest()},
                        "files_sha256": "fixture"}
            result = consumer.measure(root, manifest)
            self.assertEqual(result["bytes_hashed"], len(b"real measured bytes") * 256)
            (root / "source").write_bytes(b"changed")
            with self.assertRaises(ValueError):
                consumer.measure(root, manifest)

    def test_downgrade_cannot_bypass_authority(self):
        help_text = "  authority  Inspect authority\n  bootstrap  Run bootstrap\n"
        installer.check_candidate_version("stillyard 0.1.0-alpha.15", "0.1.0-alpha.14", help_text)
        for candidate, prior, commands in [("0.1.0-alpha.14", "0.1.0-alpha.15", help_text),
                                           ("0.1.0-alpha.15", "0.1.0-alpha.16", help_text),
                                           ("0.1.0-alpha.15", "0.1.0-alpha.15", "")]:
            with self.subTest(candidate=candidate, prior=prior), self.assertRaises(ValueError):
                installer.check_candidate_version("stillyard " + candidate, prior, commands)


if __name__ == "__main__":
    unittest.main()
