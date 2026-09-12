#!/usr/bin/env python3
"""Native installation publication controls; run as a default Stillyard Job.

These tests use temporary files and a mocked stopped-store CLI. They do not
claim real service, native-host, cgroup or consumer acceptance.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('native_service', Path(__file__).with_name('native-linux-service.py'))
service = importlib.util.module_from_spec(spec)
spec.loader.exec_module(service)


class Installation(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.daemon = self.root / 'candidate'
        self.daemon.write_bytes(b'not-an-executable: CLI is mocked')
        self.executors = Path('/sys/fs/cgroup/test-only/executors')
        self.request = self.root / 'native-install-request.json'
        self.request.write_text(json.dumps({
            'daemon_sha256': hashlib.sha256(self.daemon.read_bytes()).hexdigest(),
            'store': str(self.root), 'executor_cgroup': str(self.executors),
        }))
        self.request.chmod(0o600)

    def test_request_is_claimed_before_cli_and_never_replayed_after_failure(self):
        def fail(*args, **kwargs):
            self.assertFalse(self.request.exists())
            self.assertTrue((self.root / 'native-install-started.json').is_file())
            raise subprocess.TimeoutExpired(args[0], 30)
        with patch.object(service.subprocess, 'run', side_effect=fail) as call:
            with self.assertRaises(subprocess.TimeoutExpired):
                service.initialize(self.root, self.daemon, self.executors)
            with self.assertRaises(FileNotFoundError):
                service.initialize(self.root, self.daemon, self.executors)
            self.assertEqual(call.call_count, 1)
        self.assertFalse((self.root / 'native-install-result.json').exists())

    def test_completed_anchor_can_start_without_reinitializing(self):
        def install(*args, **kwargs):
            directory = self.root / 'native-linux'
            directory.mkdir(mode=0o700)
            anchor = directory / 'anchor.json'
            anchor.write_text('{}')  # Rust, not this helper, validates the payload.
            anchor.chmod(0o600)
            return subprocess.CompletedProcess(args[0], 0, json.dumps({'store_path': str(self.root)}), '')
        with patch.object(service.subprocess, 'run', side_effect=install) as call:
            service.initialize(self.root, self.daemon, self.executors)
            service.initialize(self.root, self.daemon, self.executors)
            self.assertEqual(call.call_count, 1)
        self.assertTrue((self.root / 'native-install-result.json').is_file())

    def test_changed_candidate_and_unsafe_request_are_rejected_before_execution(self):
        with patch.object(service.subprocess, 'run') as call:
            self.daemon.write_bytes(b'changed')
            with self.assertRaises(RuntimeError):
                service.initialize(self.root, self.daemon, self.executors)
            self.request.chmod(0o644)
            with self.assertRaises(RuntimeError):
                service.initialize(self.root, self.daemon, self.executors)
            original = self.root / 'request-target'
            self.request.rename(original)
            self.request.symlink_to(original)
            with self.assertRaises(OSError):
                service.initialize(self.root, self.daemon, self.executors)
            call.assert_not_called()

    def test_crash_between_claim_and_unlink_blocks_replay(self):
        os.link(self.request, self.root / 'native-install-started.json')
        with patch.object(service.subprocess, 'run') as call:
            with self.assertRaises(RuntimeError):
                service.initialize(self.root, self.daemon, self.executors)
            call.assert_not_called()


if __name__ == '__main__':
    unittest.main()
