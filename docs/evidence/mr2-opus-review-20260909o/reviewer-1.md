| F4 `Uncertain` never assigned | **High** | Exhaustive over the supplied set: six read sites, zero write sites. The only residual risk is a writer in an unsupplied file, which would have to be outside `machine_*`. |
| F5 no offer delivery | **Medium** | Verifiable that `Command`/`Outcome`/`ProtocolRecord` contain no offer-push and no `Offer` request. Downgraded from High only because the reactor is unsupplied and could carry an out-of-band channel. |
| F6 fence namespace split | **Medium-High** | The `authorize` side is fully supplied and provable. The native-admission side rests on `remote_debits` having the native blocker computation as its consumer (unsupplied), and on `expand` scoping fences the way `physical_claims` assumes. |
| F7 pairing abandon path | **Medium-High** | Code path fully supplied and the dead end is provable. Depends on whether the pairing CLI regenerates the secret per invocation — if it derives it deterministically, the exact-replay branch saves it and severity drops to Low. |
| F8 unbounded `retired_allocations` | **High** | Insert site supplied, no removal site anywhere in the file. Only the time-to-exhaustion estimate is approximate. |
| F9 oversized reconcile intent | **Medium-High** | Bounds and the intent construction are supplied; reachability depends on a manager actually accumulating thousands of sealed releases before reconnecting, which is exactly the outage scenario §5 targets. |
| F10 connect-finish ordering | **Medium** | The ordering defect is certain; I could not construct a reachable non-crash trigger, and both variants self-heal on the next `ConnectBegin`. Severity, not existence, is the uncertain part. |
| F11 registry validation posture | **Medium** | Certain as an inconsistency; impact is bounded by the checksum and atomic publication, so this is a judgement call about defence in depth rather than a demonstrated failure. |
| F12a fsync under the write lock | **High** (that it happens) / **Low** (that it matters) | The call graph is fully supplied. Whether it binds is an empirical question for W-C1 and the MR-3 idle budget. |
| F12b event poison pill | **Medium** | The error path is supplied and certain; reachability depends on whether `leases` rows are ever written in a state other than `granted`/`released`, which I could not confirm — `native_allocations` making the same assumption suggests they are not. |
| F12c ticket freshness | **High** | Directly readable from `machine_ticket.rs` and `manager.rs`; the deferral is stated in the code's own doc comment. |

**Overall:** high confidence that the four traced crash boundaries are correct for the implemented operations, and high confidence in the four convergence dead ends (F1–F4), which are the findings that should drive the next slice. My weakest area is anything that depends on the transport/reactor or on `crate::admission` — F5 and the native half of F6 are the two findings most likely to be void if those files contradict my stated assumptions.

**Questions that would change my view:**

1. Does the reactor push an unsolicited frame when `machine_offer_before_native` creates an offer, or does the manager long-poll `Inspect`? (Decides F5 entirely.)
2. Does native admission compute blockers from `ResolvedClaims` fed by `remote_debits`, or does it expand through `ResourceTopology` like `machine_offer_before_native` does? (Decides the native half of F6.)
3. Does the pairing CLI derive the 256-bit secret deterministically from the installation nonce, or call `PairingSecret::generate()` on each attempt? (Decides F7's severity.)
4. Is there any writer of `GrantState::Uncertain` outside the `machine_*` modules — for example in a reconciliation sweep not in this snapshot? (Would void F4.)
5. Was the choice to retire the manager's unapplied responses in `prepare_machine_reset_recovery` deliberate, with a manager-side counterpart planned for the next slice? If so, F3 is a sequencing note rather than a defect, and only the `bind` predicate needs to land with it.
