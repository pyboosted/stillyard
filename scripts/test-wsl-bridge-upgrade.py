#!/usr/bin/env python3
"""Real SQLite/file crash controls for explicit pin rotation; run as a system Job."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("rotate", Path(__file__).with_name("rotate-wsl-bridge-pin.py"))
rotate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rotate)


class Rotation(unittest.TestCase):
    def test_crash_after_sql_commit_resumes_without_changing_identity_or_jobs(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory)
            db = sqlite3.connect(path / "history.sqlite3", isolation_level=None)
            db.executescript("create table attached_meta(key text primary key,value text); create table jobs(id text primary key); insert into jobs values('retained-job');")
            old = {"pairing": {"secret": [42] * 32, "store": "same-store"}, "journal": "same-journal", "bridge_sha256": "a" * 64}
            new = old | {"bridge_sha256": "b" * 64}
            intent = {"old_image": "a" * 64, "old_anchor": rotate.fingerprint(old), "new_anchor": rotate.fingerprint(new)}
            anchor = path / "anchor.json"
            anchor.write_text(json.dumps({"configuration": old, "sha256": intent["old_anchor"]}))
            db.execute("insert into attached_meta values('installation_sha256',?)", [intent["old_anchor"]])
            db.execute("BEGIN IMMEDIATE")
            def fail_publish(*_):
                raise OSError("injected crash before anchor publication")
            with self.assertRaisesRegex(OSError, "injected crash"):
                rotate.commit_rotation(db, anchor, intent, new, fail_publish)
            db.close()
            db = sqlite3.connect(path / "history.sqlite3", isolation_level=None)
            self.assertEqual(db.execute("select value from attached_meta").fetchone()[0], intent["new_anchor"])
            self.assertEqual(json.loads(anchor.read_text())["configuration"], old)
            def publish(file, value):
                stage = file.with_suffix(".next")
                stage.write_text(json.dumps(value))
                stage.replace(file)
            # Repeating exactly the recorded intent reconciles the split state.
            db.execute("BEGIN IMMEDIATE")
            rotate.commit_rotation(db, anchor, intent, new, publish)
            self.assertEqual(json.loads(anchor.read_text())["configuration"], new)
            self.assertEqual(db.execute("select id from jobs").fetchall(), [("retained-job",)])
            # A second matching resume is harmless; an unrelated SQL anchor is not.
            db.execute("BEGIN IMMEDIATE")
            rotate.commit_rotation(db, anchor, intent, new, publish)
            db.execute("update attached_meta set value='unknown-history'")
            db.execute("BEGIN IMMEDIATE")
            with self.assertRaisesRegex(RuntimeError, "SQLite anchor changed"):
                rotate.commit_rotation(db, anchor, intent, new, publish)
            db.rollback()
            self.assertEqual(db.execute("select value from attached_meta").fetchone()[0], "unknown-history")
            # Even a self-consistent anchor with different pairing identities is refused.
            foreign = new | {"journal": "different-journal"}
            anchor.write_text(json.dumps({"configuration": foreign, "sha256": rotate.fingerprint(foreign)}))
            db.execute("BEGIN IMMEDIATE")
            with self.assertRaisesRegex(RuntimeError, "anchor changed"):
                rotate.commit_rotation(db, anchor, intent, new, publish)
            db.rollback()
            self.assertEqual(db.execute("select id from jobs").fetchall(), [("retained-job",)])
            db.close()


if __name__ == "__main__":
    unittest.main()
