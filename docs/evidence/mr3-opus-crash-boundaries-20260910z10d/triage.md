# Scoped M-A04 review disposition

Review confirms actual process deaths at all five checkpoints, no repeated user
launch and retained Lease until actual cleanup and acknowledgement. No product
defect found. This is not the overall MR-3 exit review.

1. Accept explicit assertion of the recovered Release TicketCleanup and its
actual seal digests. Reject equating two distinct evidence bits: executor
possibly_released means release_intent was recorded before manager SQL; local
continuous SQL consumed=false independently proves the kernel release callback
could not have run. The release-intent checkpoint intentionally has executor
possibly_released=true and manager user_code_released=false. The consumed
checkpoint has both true even though the real start count is zero; a lost
transient NeverReleased proof cannot downgrade durable consumption. z10e checks
both contracts and the exact cleanup hashes, plus empty Ticket set at ready.

2. Existing native acceptance already waits 5.5 s after Arm and asserts the
canary stays pending beyond the 5 s Offer TTL (isolated_daemon.rs). The omitted
helper body was not included in the review brief. Still accept an additional
5.5 s after actual Ticket issuance with the same pending-canary assertion;
this pins the stronger issued-Ticket case directly rather than inferring it.

Both additions are test-only and pending z10e gates. Other MR-3 rows, final
consumer confirmation, idle helpers and lifecycle coverage remain separate.
