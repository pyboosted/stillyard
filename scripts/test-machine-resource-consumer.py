#!/usr/bin/env python3
"""Contract controls for the consumer artifacts; schedule this with Stillyard."""
import hashlib
import copy
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
    def test_managed_calls_reject_background_exit_duplicate_and_foreign_receipt(self):
        identity = {"STILLYARD_JOB_ID": "parent", "STILLYARD_ATTEMPT": "attempt",
                    "STILLYARD_INVOCATION_ID": "primary", "STILLYARD_ENDPOINT": "socket"}
        call = {"invocation": identity, "completed": True, "exit_code": 0,
                "idempotency_key": "key", "spec_sha256": "digest",
                "started_unix_ns": 10, "finished_unix_ns": 20,
                "receipt": {"accepted": {"job_id": "child", "parent": {
                    "job_id": "parent", "attempt_id": "attempt", "invocation_id": "primary"}}}}
        calls = [call, copy.deepcopy(call) | {"started_unix_ns": 21, "finished_unix_ns": 22}]
        self.assertEqual(consumer.validate_managed_calls(calls, "parent", "attempt"), "child")
        mutations = [calls[:1], calls + [call], [call | {"completed": False}, calls[1]],
                     [call | {"exit_code": 1}, calls[1]],
                     [call, calls[1] | {"idempotency_key": "other"}],
                     [call, calls[1] | {"started_unix_ns": 19}]]
        foreign = copy.deepcopy(calls)
        foreign[1]["receipt"]["accepted"]["job_id"] = "different-child"
        mutations.append(foreign)
        foreign_parent = copy.deepcopy(calls)
        foreign_parent[1]["receipt"]["accepted"]["parent"]["invocation_id"] = "other-primary"
        mutations.append(foreign_parent)
        for changed in mutations:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                consumer.validate_managed_calls(changed, "parent", "attempt")

    def test_final_result_and_negative_controls(self):
        raw = {"type": "result", "subtype": "success", "is_error": False,
               "modelUsage": {"claude-sonnet-test": {}},
               "structured_output": {"verdict": "pass", "summary": "fixture", "findings": []}}
        self.assertEqual(consumer.validate_review(raw, "claude-sonnet-")["verdict"]["verdict"], "pass")
        mixed = raw | {"modelUsage": {"claude-sonnet-test": {}, "claude-haiku-helper": {}}}
        checked = consumer.validate_review(mixed, "claude-sonnet-")
        self.assertEqual(checked["selected_family_models"], ["claude-sonnet-test"])
        self.assertEqual(checked["other_reported_models"], ["claude-haiku-helper"])
        for mutation in [{"is_error": True}, {"subtype": "error_max_turns"}, {"modelUsage": {}},
                         {"modelUsage": {"different-model": {}}},
                         {"structured_output": {"verdict": "pass", "summary": "", "findings": []}},
                         {"structured_output": {"verdict": "findings", "summary": "x", "findings": []}}]:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                consumer.validate_review(raw | mutation, "claude-sonnet-")

    def test_measurement_checks_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Enough real work to exceed Windows process-time resolution.
            payload = b"real measured bytes" * 65536
            (root / "source").write_bytes(payload)
            manifest = {"files": {"source": hashlib.sha256(payload).hexdigest()},
                        "files_sha256": "fixture"}
            result = consumer.measure(root, manifest)
            self.assertEqual(result["bytes_hashed"], len(payload) * 256)
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
