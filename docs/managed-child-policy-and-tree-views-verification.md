# Managed-child policy and tree views: verification record

Date: 2026-08-29

This record closes the alpha.9 implementation gate defined by
`managed-child-policy-and-tree-views.md`. All Cargo-backed validation below ran as Jobs on the
system default Stillyard daemon through `scripts/run-stillyard-job.ps1`; no direct Cargo command
was used.

## Independent review

The design brief and the implementation both received final approval from the required independent
reviewers. Review history and finding dispositions are preserved in
`managed-child-policy-and-tree-views-review-disposition.md`.

- Fable implementation closure: `APPROVE`, session
  `d9a85502-fcdf-4faf-8531-a9096d750f97`, model usage `claude-fable-5`; artifact
  `C:\Users\User\AppData\Local\Temp\fable-stillyard-managed-child-tree-implementation-closure2-20260829.json`.
- Grok implementation closure: `APPROVE`, `grok-4.6` at high reasoning, no findings; artifact
  `C:\Users\User\AppData\Local\Temp\grok-stillyard-managed-child-tree-implementation-closure2-20260829.json`.

## System-daemon validation

The promoted daemon used store UUID `01a04dcf-9568-7952-ad05-ab209d61282f`, generation
`01a04dcf-9573-7722-bd46-95faf8e1f519`, protocol 14, and version `0.1.0-alpha.9`.

The final post-promotion Job set was:

| Gate | Job ID | Result |
| --- | --- | --- |
| `fmt` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dcf-b9e3-71d0-9589-a246bb0005e5` | succeeded |
| `check` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dcf-ca6e-70a2-913a-1140cc3ca087` | succeeded |
| `test` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dcf-d8cd-7110-9dad-19aef6fdff5f` | succeeded |
| `clippy` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dd0-1019-7420-8bee-81b48ab6ba85` | succeeded |
| `schema-update` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dd0-1fa1-7842-972a-eb739ca848d5` | succeeded |
| `build-release` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dd0-2f64-71d2-95af-bce9b428f77c` | succeeded |

The test Job passed 184 library tests with 3 ignored, 24 TUI tests, and all integration/public API
tests. The Job receipts use JobSpec v2 and contain the new `child_submission_policy` field.

## Installed-image acceptance

The release image and installed canonical executable both had SHA-256
`7511D6D52F7E2B5B83E1084489AA551B59CBC0661EFDD465A9A3081B7AB0E99A` at acceptance time.

The following checks ran through
`C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe`:

- Parent Job `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dd2-36c2-7001-94a0-bc742e381b65`
  submitted and waited for child Job
  `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dd2-378a-7db1-bc1c-3493970abb9f`.
  The durable result recorded the exact parent Job, attempt, and invocation plus the resolved
  effective policy and policy ancestor.
- Parent Job `01a04dcf-9568-7952-ad05-ab209d61282f~01a04dd3-5952-7d52-a93c-fffc91cff980`
  attempted a child with `cargo_slots: 1` under a claimless policy. The child CLI exited 27 and its
  durable recovery result recorded `child_claim_not_permitted`; the parent succeeded only after
  checking both values. No child Job was created for the rejected submission.
- `stillyard tree` in JSON and ASCII modes returned the retained positive family with depth 0/1,
  the exact parent link, `has_children`, and the expected ASCII connector.
- `stillyard list --tree --json` returned the new tree summary fields from the installed daemon.
- `stillyard watch --job <parent>` rendered successfully in a real terminal and detached on `q`
  with exit 0. The pre-existing user watch process was left running.

The pre-promotion alpha.8 image remains recoverable at
`C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe.alpha8-20260829.bak` with
SHA-256 `30F3876A9D82500A9EF0E3EAC21B08B25482B02B3DBA0239E70D0937E1F34664`.

## Corrective review follow-up

A post-delivery review identified four contract gaps. The corrective change:

- prioritizes continuation work by its actual depth-first position and rebuilds the visible order
  from the loaded relation graph, including left-to-right sibling branches;
- discards every partially loaded page and restarts with no root cursor after `ViewStale`;
- expands the active/queued node itself and renders bounded direct-child outcome summaries such as
  `3 children: 2 ok, 1 failed` for collapsed branches;
- canonicalizes VRAM custom claim names in new and retained policies, requested-claim lookup, and
  effective admission evidence.

Deterministic tests cover ancestor-versus-sibling continuation, two unfinished sibling branches,
root-cursor staleness, nested active-branch expansion, collapsed outcome text, mixed-case requested
VRAM UUIDs, and legacy retained alpha.9 policy spelling.

| Corrective gate | Job ID | Result |
| --- | --- | --- |
| `fmt-write` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e3b-3389-7d80-a29c-3dc59fc60db1` | succeeded |
| `check` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e3b-353b-7511-887e-9cdeb702f797` | succeeded |
| `test` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e3b-3b55-75e1-9632-ceb418761910` | succeeded |
| `clippy` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e3b-6bcc-7381-be5a-3cc27faa0403` | succeeded |
| `fmt` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e3c-5a78-71e1-a3eb-8b636629f512` | succeeded |
| `build-release` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e3c-5c42-75d3-ab86-372f983ba825` | succeeded |

The corrective test Job passed 186 library tests with 3 ignored, 28 TUI/CLI tests, and all
integration/public API tests. The corrective release artifact has SHA-256
`732D99C61A76F814D2705114AC2A4334C86A9A664363AEFF2513D962AC36D8FF`.

The artifact was then promoted to the canonical client/daemon executable. The prior `fccd01e`
alpha.9 image is recoverable as
`C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe.alpha9-fccd01e-20260829.bak`
with SHA-256 `7511D6D52F7E2B5B83E1084489AA551B59CBC0661EFDD465A9A3081B7AB0E99A`.
The replacement daemon started as PID 59804 with generation
`01a04e49-0615-71d1-acb2-34193b39b2e8`, retained the existing store UUID, and successfully ran
post-promotion `fmt` Job
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04e49-32b2-7ab0-9a84-74048d9e68c5`.
The promoted client also completed a JSON tree smoke against the retained managed family.

## Stable expansion follow-up

Operator feedback superseded the original finished-subtree default: represented branches now
start expanded and remain expanded across running, queued, and final state transitions. Completion
therefore cannot make child rows disappear or cause an automatic layout jump. Only an explicit
Left action collapses a branch; its bounded outcome summary remains available in that state.

The TUI regression covers a finished branch visible by default and verifies that an explicit
collapse still produces `3 children: 2 ok, 1 failed`.

| Stable-expansion gate | Job ID | Result |
| --- | --- | --- |
| `fmt-write` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e4d-85a5-7911-b48f-4551da9c2315` | succeeded |
| `check` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e4d-8754-7e10-ac49-acd45991dd81` | succeeded |
| `test` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e4d-8992-76b3-83a9-ca1ab2ed13b7` | succeeded |
| `clippy` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e4d-ba15-7010-80dc-0685f137083d` | succeeded |
| `build-release` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e4d-bdd9-7a93-ab47-7f136ccd1716` | succeeded |

The release and canonical installed executable have SHA-256
`647CEF61461FEE03B36EBBEB9FD817FA81D9D719D3334489D222849CC5679EF3`. The prior corrective image
is recoverable at
`C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe.alpha9-dd39de7-stable-expansion-20260829.bak`
with SHA-256 `732D99C61A76F814D2705114AC2A4334C86A9A664363AEFF2513D962AC36D8FF`.
The replacement daemon runs as PID 46564 with generation
`01a04e4e-7b11-7f82-bcc1-c1b7c292df49`; the installed image then passed post-promotion `fmt` Job
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04e4f-25e9-7872-867a-aa4ea76bc37a` after respecting an
already-running external Cargo workload's `cargo_slots` claim.

## Finished-history visibility follow-up

The retained completed family
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04e4b-3170-7e01-a153-d472538d53a5`
proved that daemon storage and the public tree API still returned both depth-1 fmt and Clippy
children after completion. The remaining visibility problem was TUI-only: a branch row had no
disclosure marker, and attention-bucket movement could pin the selected parent to the final
viewport row with its expanded children just below the screen.

The correction renders `▾`/`▸` on expanded/collapsed branches and keeps as much of a selected
expanded subtree beside its parent as the viewport permits. A terminal snapshot regression checks
the completed parent and adjacent child rows plus the explicit collapsed state; a scroll regression
checks both small and viewport-sized subtrees.

A later pass (2026-08-29) moved the tree out of COMMAND into a rail gutter before STATE, closed
the last sibling with `└─`, banded expanded families, and added mouse support (wheel per pane,
click to focus/select, click on the disclosure cell to toggle, click on a log tab to switch).
Regressions: `tree_guides_close_last_siblings_and_carry_ancestor_rails`,
`mouse_targets_panes_rows_gutter_and_log_tabs`, and the updated
`expanded_finished_family_is_visibly_a_tree` snapshot. Validation Jobs: fmt-write
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04e70-852a-7811-9fe8-b2d3c8f6e716`, clippy
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04e70-c9ba-78b3-88db-8d9a1204d895`, test
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04e70-ce49-7932-ba3b-4812d1097945`, build-release
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04edb-5a62-7be2-988a-f8ca88946223` (commit `52e4539`).

The release and installed executable have SHA-256
`F12E25AD39AA1CEA776A38ED59CE03C7A888B3A4BCE752FEAC2877D5748CDA9C`. The prior image is
recoverable at
`C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe.alpha9-95681ca-history-visible-20260829.bak`
with SHA-256 `EFCFD8A5D3ED0F5773DB25596156D548FE9035586AD9840D406705D69B300B78`. The promotion
waited for an idle daemon; the replacement daemon runs as PID 58120 with generation
`01a04edc-183c-7dc3-bab9-cefdb86c243e`, retained store `01a04dcf-9568-7952-ad05-ab209d61282f`,
and passed post-promotion `fmt` Job
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04ee0-7f71-79c2-8aa8-ff156b6e3d23`.

| Finished-history gate | Job ID | Result |
| --- | --- | --- |
| `fmt-write` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e5c-576b-7ec2-831c-17bad7e56709` | succeeded |
| `check` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e5e-7303-7a00-8d72-eb1609856d4c` | succeeded |
| `test` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e5e-7894-7202-a13f-fe66d3f7f6f1` | succeeded |
| `clippy` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e5e-b049-7f30-9ba0-d4a18d768bc9` | succeeded |
| `build-release` | `01a04dcf-9568-7952-ad05-ab209d61282f~01a04e5e-b48a-7fa3-b1e5-02152d8eebff` | succeeded |

The release and installed executable have SHA-256
`EFCFD8A5D3ED0F5773DB25596156D548FE9035586AD9840D406705D69B300B78`. The prior image is
recoverable at
`C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe.alpha9-1c0731a-finished-history-20260829.bak`.
The replacement daemon runs as PID 58784 with generation
`01a04e5f-583c-7b32-89e5-ee2315b26aa3` and passed post-promotion `fmt` Job
`01a04dcf-9568-7952-ad05-ab209d61282f~01a04e5f-efbf-7970-8ddd-69968de7ea4c`.

## Verdict

The frozen brief is implemented, the independent implementation-review gate is closed, all
system-daemon Jobs pass on the promoted protocol/version, and the installed image satisfies both
positive and negative managed-child flows plus CLI/TUI tree smoke acceptance.
