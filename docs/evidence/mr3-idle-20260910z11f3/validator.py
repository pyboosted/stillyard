"""Compose recorded idle conditions; missing helper evidence never becomes PASS."""
import argparse
from collections import Counter
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence_directory', type=Path)
    parser.add_argument('--installation-plan', type=Path, required=True)
    args = parser.parse_args()
    root = args.evidence_directory
    result = json.loads((root / 'result.json').read_text())
    before = json.loads((root / 'before.json').read_text())
    after = json.loads((root / 'after.json').read_text())
    settled = json.loads((root / 'settlement.json').read_text())
    plan = json.loads(args.installation_plan.read_text())
    installed = json.loads(args.installation_plan.with_name('after.json').read_text())
    checks = []

    def check(name, passed, observed):
        checks.append({'condition': name, 'passed': bool(passed), 'observed': observed})

    check('at_least_five_minutes_excluding_collection', result['duration_seconds'] >= 300,
          result['duration_seconds'])
    check('no_invalidated_interval', not (root / 'invalidated.json').exists(),
          (root / 'invalidated.json').exists())
    for side in ('windows', 'linux'):
        expected = installed[side]['daemon_generation']
        generations = [frame['doctor'][side]['daemon']['daemon_generation'] for frame in (before, after)]
        check(side + '_accepted_generation', generations == [expected, expected], generations)
    check('accepted_installed_images', before['binary_sha256'] == after['binary_sha256'] == plan['candidate_sha256'],
          {'before': before['binary_sha256'], 'after': after['binary_sha256']})
    check('stable_quiescent_interval', all(result['preconditions'][key] for key in (
        'unchanged_job_history', 'unchanged_interop', 'unchanged_process_identities', 'unchanged_daemon_generations')),
        result['preconditions'])
    check('boundary_settlement', settled['passed'] and settled['duration_seconds'] >= 30
          and settled['delayed_transport_or_backoff_expirations'] == 0, settled)
    roles = Counter(p['role'] for p in result['processes'])
    check('complete_expected_process_inventory', roles == Counter({
        'coordinator_daemon': 1, 'executor_daemon': 1, 'native_bridge': 1,
        'windows_keepalive': 1, 'interop_proxy': 1, 'interop_init': 1,
        'python_keepalive_or_supervisor': 2}), dict(roles))
    for p in result['processes']:
        if p['role'] in ('interop_init', 'python_keepalive_or_supervisor'):
            check('helper_zero_voluntary_switches_' + str(p['pid']), p['voluntary_switches'] == 0,
                  p['voluntary_switches'])
        if p['role'] == 'windows_keepalive':
            check('opaque_keepalive_zero_execution', p['process_cycles'] == 0
                  and p['thread_context_switches'] == 0,
                  {'lifetime_cycle_delta': p['process_cycles'], 'thread_switch_delta': p['thread_context_switches']})
    timers = result['daemon_timer_expirations']
    for side, names in [('linux', ('transport', 'backoff', 'subscriber')), ('windows', ('backoff', 'subscriber'))]:
        for name in names:
            check(side + '_' + name + '_zero', timers[side][name] == 0, timers[side][name])
    for side, buckets in result['inapplicable_buckets'].items():
        check(side + '_inapplicable_zero', all(v == 0 for v in buckets.values()), buckets)
    rate = sum(sum(buckets.values()) for buckets in timers.values()) * 60 / result['duration_seconds']
    check('aggregate_timer_budget_with_conditional_zero_helpers', rate < 6, rate)
    check('cpu_memory_budget', result['cpu_memory_pass'], {
        'cpu_percent_one_core': result['aggregate_cpu_percent'],
        'endpoint_sample_memory_mib': result['aggregate_memory_mib'],
        'per_role': result['cpu_memory_checks']})
    report = {'passed': all(c['passed'] for c in checks), 'checks': checks,
              'source_files_sha256': plan['source_files_sha256'],
              'aggregate_timer_expirations_per_minute': rate,
              'helper_basis': 'Complete source wait-path audit plus lifetime process cycles, identities and settlement; no cycles-to-time conversion.',
              'memory_scope': 'Endpoint samples of Windows private bytes and Linux RssAnon, as specified by MR-0; not an interval peak measurement.'}
    (root / 'acceptance.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
