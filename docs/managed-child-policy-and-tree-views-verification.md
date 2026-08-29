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

## Verdict

The frozen brief is implemented, the independent implementation-review gate is closed, all
system-daemon Jobs pass on the promoted protocol/version, and the installed image satisfies both
positive and negative managed-child flows plus CLI/TUI tree smoke acceptance.
