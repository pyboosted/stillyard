# Discarded preparation v

The snapshot copier detected a concurrent ledger update and rejected its source
identity. A fmt-write Job was submitted before that failure was inspected:
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a087e5-11e8-7c83-a9bd-dd830a0ebd8a`.
It completed only in the separate v directory. Its output was not copied to the
working checkout and is not acceptance evidence. No check/test/build ran on v.
A fresh immutable snapshot w was prepared successfully and is validated separately.
