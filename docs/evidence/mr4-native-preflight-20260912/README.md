# WSL is not a native Linux acceptance host

Default WSL Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0964d-a2d4-7af0-b63a-e31d4ca2f347`
succeeded with Windows Grant `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0964d-a30a-70d3-af55-140255ff67fa`, now released.
The wrapper expected prerequisite exit 2, `native_linux=false` and
`native_installation_accepted=false`. The real nested bubblewrap namespace/
ptrace exec-stop control passed; ext4 and cgroup v2 were visible. The user bus
and linger were unavailable inside the managed private `/run`; this is not a
host service inventory. Canonical evidence is under `wsl-control/`.
