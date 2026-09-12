#!/usr/bin/env python3
"""Small, explicit live-consumer commands for W-C2..4, run as Stillyard Jobs."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import time
import uuid


def publish(path, value):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def validate_review(raw, expected_model_prefix):
    if raw.get("type") != "result" or raw.get("is_error") is not False or raw.get("subtype") != "success":
        raise ValueError("CLI did not return a successful final result")
    models = raw.get("modelUsage", {})
    selected_models = [model for model in models if model.startswith(expected_model_prefix)]
    if not selected_models:
        raise ValueError("actual modelUsage is absent or differs from selected model family")
    verdict = raw.get("structured_output")
    if verdict is None:
        verdict = json.loads(raw["result"])
    if not isinstance(verdict, dict) or set(verdict) != {"verdict", "summary", "findings"}:
        raise ValueError("verdict has an unexpected shape")
    if verdict["verdict"] not in ("pass", "findings") or not isinstance(verdict["summary"], str) or not verdict["summary"].strip():
        raise ValueError("empty or invalid verdict")
    if not isinstance(verdict["findings"], list) or any(not isinstance(item, str) or not item.strip() for item in verdict["findings"]):
        raise ValueError("findings must be nonempty strings")
    if (verdict["verdict"] == "findings") != bool(verdict["findings"]):
        raise ValueError("verdict and findings disagree")
    return {"actual_models": sorted(models), "selected_family_models": sorted(selected_models),
            "other_reported_models": sorted(set(models) - set(selected_models)), "verdict": verdict}


def validate_managed_calls(calls, parent_job, parent_attempt):
    if len(calls) != 2:
        raise ValueError("managed acceptance requires exactly two adapter calls")
    ordered = sorted(calls, key=lambda call: call.get("started_unix_ns", 0))
    first = ordered[0]
    children = set()
    for call in ordered:
        identity = call.get("invocation", {})
        accepted = call.get("receipt", {}).get("accepted", {})
        expected_parent = {"job_id": parent_job, "attempt_id": parent_attempt,
                           "invocation_id": identity.get("STILLYARD_INVOCATION_ID")}
        if (call.get("completed") is not True or call.get("exit_code") != 0
                or identity.get("STILLYARD_JOB_ID") != parent_job
                or identity.get("STILLYARD_ATTEMPT") != parent_attempt
                or not expected_parent["invocation_id"]
                or accepted.get("parent") != expected_parent
                or not accepted.get("job_id")
                or not call.get("idempotency_key") or not call.get("spec_sha256")
                or call.get("idempotency_key") != first.get("idempotency_key")
                or call.get("spec_sha256") != first.get("spec_sha256")
                or identity != first.get("invocation")
                or call.get("finished_unix_ns", 0) < call.get("started_unix_ns", 1)):
            raise ValueError("managed calls are incomplete, failed, or disagree on their parent/key/spec")
        children.add(accepted["job_id"])
    if len(children) != 1 or ordered[1]["started_unix_ns"] < first["finished_unix_ns"]:
        raise ValueError("calls did not sequentially recover the same child Job")
    return next(iter(children))


def measure(root, manifest, rounds=256):
    files = manifest["files"]
    expected = hashlib.sha256()
    total_bytes = 0
    selected = []
    for name, digest in sorted(files.items()):
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()) or path.is_symlink():
            raise ValueError("manifest path escapes the source root")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("source differs from manifest: " + name)
        expected.update(data)
        total_bytes += len(data)
        selected.append((path, digest))
    if total_bytes == 0 or rounds != 256:
        raise ValueError("acceptance requires nonempty source and exactly 256 rounds")
    start_wall, start_cpu = time.perf_counter(), time.process_time()
    for _ in range(rounds):
        combined = hashlib.sha256()
        for path, digest in selected:
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("source changed during measurement")
            combined.update(data)
        if combined.digest() != expected.digest():
            raise ValueError("measurement digest differs")
    wall, cpu = time.perf_counter() - start_wall, time.process_time() - start_cpu
    if not all(math.isfinite(value) and value > 0 for value in (wall, cpu)):
        raise ValueError("measurement durations must be finite and positive")
    return {"rounds": rounds, "bytes_per_round": total_bytes, "bytes_hashed": rounds * total_bytes,
            "payload_sha256": expected.hexdigest(), "source_files_sha256": manifest["files_sha256"],
            "wall_seconds": wall, "cpu_seconds": cpu}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    review = commands.add_parser("review")
    review.add_argument("--cli", type=Path, required=True)
    review.add_argument("--brief", type=Path, required=True)
    review.add_argument("--output", type=Path, required=True)
    review.add_argument("--model", default="sonnet")
    review.add_argument("--effort", choices=("low", "medium", "high"), default="low")
    validate = commands.add_parser("validate-review")
    validate.add_argument("--input", type=Path, required=True)
    validate.add_argument("--expected-model-prefix", default="claude-sonnet-")
    managed_validation = commands.add_parser("validate-managed-build")
    managed_validation.add_argument("--cli", type=Path, required=True)
    managed_validation.add_argument("--evidence-directory", type=Path, required=True)
    measurement = commands.add_parser("measure")
    measurement.add_argument("--repository-root", type=Path, required=True)
    measurement.add_argument("--source-manifest", type=Path, required=True)
    measurement.add_argument("--output", type=Path, required=True)
    child = commands.add_parser("managed-build")
    child.add_argument("--cli", type=Path, required=True)
    child.add_argument("--spec", type=Path, required=True)
    child.add_argument("--operation", required=True)
    child.add_argument("--evidence-directory", type=Path, required=True)
    args = parser.parse_args()
    identity = {key: os.environ.get(key) for key in (
        "STILLYARD_JOB_ID", "STILLYARD_ATTEMPT", "STILLYARD_INVOCATION_ID", "STILLYARD_ENDPOINT")}
    if not all(identity.values()):
        parser.error("run this consumer as a Stillyard Invocation")
    if args.command == "validate-review":
        print(json.dumps(validate_review(json.loads(args.input.read_text()), args.expected_model_prefix)))
        return 0
    if args.command == "validate-managed-build":
        calls = [json.loads(path.read_text()) for path in args.evidence_directory.glob("call-*.json")]
        job = validate_managed_calls(calls, identity["STILLYARD_JOB_ID"], identity["STILLYARD_ATTEMPT"])
        status = json.loads(subprocess.check_output([str(args.cli), "--endpoint", identity["STILLYARD_ENDPOINT"],
                                                    "status", job, "--deadline-seconds", "10"], timeout=15))
        if status["state"] != "final" or status["outcome"] != "succeeded" or len(status["attempts"]) != 1:
            raise ValueError("managed child did not finish successfully in one Attempt")
        print(json.dumps({"validated_child_job": job, "completed_calls": 2, "attempts": 1}))
        return 0
    if args.command == "measure":
        result = measure(args.repository_root, json.loads(args.source_manifest.read_text()))
        result["invocation"] = identity
        publish(args.output, result)
        print(json.dumps(result))
        return 0
    if args.command == "review":
        # The Job supplies HOME and CLAUDE_CONFIG_DIR explicitly. No API-key fallback.
        forbidden = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN",
                     "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY")
        if not os.environ.get("CLAUDE_CONFIG_DIR") or any(os.environ.get(key) for key in forbidden):
            raise ValueError("review requires an explicit subscription-only profile")
        auth = subprocess.run([str(args.cli), "auth", "status"], capture_output=True, check=True)
        status = json.loads(auth.stdout)
        if not status.get("loggedIn") or status.get("authMethod") != "claude.ai" or status.get("apiProvider") != "firstParty":
            raise ValueError("selected Claude profile is not authenticated through subscription OAuth")
        prompt = args.brief.read_text() + '\nReturn only JSON with keys verdict (pass or findings), summary (nonempty), findings (array of nonempty strings). A pass has no findings. Do not run tools, change files, or invoke other agents.'
        command = [str(args.cli), "-p", "--model", args.model, "--output-format", "json",
                   "--effort", args.effort, "--permission-mode", "dontAsk", "--tools=", "--setting-sources", ""]
        command += ["--json-schema", json.dumps({
            "type": "object", "additionalProperties": False,
            "required": ["verdict", "summary", "findings"],
            "properties": {"verdict": {"enum": ["pass", "findings"]},
                           "summary": {"type": "string", "minLength": 1},
                           "findings": {"type": "array", "items": {"type": "string", "minLength": 1}}}})]
        publish(args.output.with_suffix(".provenance.json"), {
            "requested_model": args.model, "effort": args.effort, "profile": os.environ["CLAUDE_CONFIG_DIR"],
            "route": "claude.ai/firstParty", "invocation": identity,
            "brief_sha256": hashlib.sha256(args.brief.read_bytes()).hexdigest(),
            "executable_sha256": hashlib.sha256(args.cli.resolve().read_bytes()).hexdigest()})
        # Stderr stays canonical; raw final output is also emitted without synthetic verdicts.
        with args.output.open("xb") as output:
            completed = subprocess.run(command, input=prompt.encode(), stdout=output)
            output.flush()
            os.fsync(output.fileno())
        print(args.output.read_text(), end="")
        return completed.returncode
    # Existing ensure performs query/recovery before any replay. OS peer authentication
    # remains the source of parent identity; these environment values only name artifacts.
    namespace = "\0".join([identity["STILLYARD_JOB_ID"], identity["STILLYARD_ATTEMPT"],
                            identity["STILLYARD_INVOCATION_ID"], args.operation])
    key = str(uuid.uuid5(uuid.NAMESPACE_URL, namespace))
    args.evidence_directory.mkdir(parents=True, exist_ok=True)
    receipt = args.evidence_directory / (key + ".receipt.json")
    call = args.evidence_directory / ("call-" + uuid.uuid4().hex + ".json")
    audit = {"invocation": identity, "idempotency_key": key,
             "spec_sha256": hashlib.sha256(args.spec.read_bytes()).hexdigest(),
             "pid": os.getpid(), "started_unix_ns": time.time_ns(), "completed": False}
    publish(call, audit)
    result = subprocess.run([str(args.cli), "--endpoint", identity["STILLYARD_ENDPOINT"],
                           "ensure", "--spec", str(args.spec), "--wait", "--passthrough",
                           "--idempotency-key", key, "--result-file", str(receipt),
                           "--deadline-seconds", "3600"]).returncode
    audit.update({"completed": True, "exit_code": result, "finished_unix_ns": time.time_ns()})
    if receipt.exists():
        audit["receipt"] = json.loads(receipt.read_text()).get("receipt")
    publish(call, audit)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
