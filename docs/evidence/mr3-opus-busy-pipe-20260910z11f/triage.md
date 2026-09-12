# Independent native busy-pipe review disposition

The reviewer confirms the finite remaining-deadline Win32 API change. The full
unmodified verdict and canonical Job are retained beside this disposition.

- The actual old-loop mutant fails with two waits instead of one. This is a
  regression control for periodic retries, not exhaustive mutation coverage of
  every timeout clamp. A 0/default-value mutant is not claimed tested. Additional
  default-value and readiness-race controls are possible test improvements.
- The unique one-instance pipe remains occupied for the fixture's scope. The
  test Job has an outer execution timeout; an elapsed upper assertion by itself
  could not interrupt an infinite blocked call. No infinite-wait mutant passed.
- On ERROR_FILE_NOT_FOUND the old loop's next Open also returns Unavailable;
  there was no general missing-server retry-until-deadline guarantee to preserve.
- Cancellation is handled by Client::request on its receiving thread in bounded
  25 ms waits, independently of the transport worker. That behavior is unchanged;
  the worker never received the cancellation token. Every bridge entry point
  explicitly passes None, using one full remaining-deadline recv_timeout instead.
- The initial brief omitted Client::request. The new helper audit includes that
  second timed wait and every bridge client entry point. The old claim that
  WaitNamedPipe was the only timed wait is withdrawn.
- Stable native identity alone would not exclude a surviving orphan. The final
  observation must retain both the actual Linux child/proxy identity and native
  identity, plus daemon generation and zero reconnect/backoff counters. Driver
  failure drops and kills its direct Linux child. No Windows kill-propagation
  guarantee is inferred from Linux SIGKILL.
- Acceptance counts actual timer expirations, not timers armed then satisfied by
  I/O. No claim of zero armed waits is made. Linux submillisecond poll rounding
  may count two actual expirations for one logical deadline: that is conservative
  for this metric, not an undercount. Satisfied polls are not timer expirations.
- protocol::write_frame explicitly flushes its writer; the hypothesized missing
  bridge flush is absent. Unsafe comment adjacency is style feedback; native
  Clippy passed, and the reviewer found no unsafe API misuse.

No aggregate idle PASS is asserted by this review. That requires the final
installed interval combined with the full helper wait-path audit.
