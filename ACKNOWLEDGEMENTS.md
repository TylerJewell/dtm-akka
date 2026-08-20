# Acknowledgements

This project is a port of **[dtm-labs/dtm](https://github.com/dtm-labs/dtm)**.

## Licence of the original

**BSD 3-Clause**, © 2021 yedf. Read from the `LICENSE` file at the root of the repository
at commit `18146ee`, not from a badge.

## What was copied

**No source was copied.** No file, function, class or fragment of `dtm-labs/dtm` appears in
this project. Everything here is written against a behavioural specification —
`dtm-port/specs/SPEC-001-dtm.md` in the harness repository — and the Java in `src/main`
shares no text with the Go it was derived from.

Two things did cross over, and neither is source:

- **The behaviour itself.** The Saga ordering and compensation rules, the Try-Confirm-Cancel
  and two-phase-commit phase split, and the sub-transaction barrier's null-compensation and
  idempotency guarantees are all derived from `dtm-labs/dtm`, and reproduce it deliberately.
  That is what a port is, and it is not something to be coy about.
- **Scenario inputs.** `dtm-port/bench/scenarios.json` in the harness repository holds
  transfer scenarios fed through both systems to compare their answers. They were written
  for that comparison; none is taken from the original's own tests.
- **The demo domain's naming.** The account-transfer example (`TransOut`/`TransIn`) follows
  the naming dtm's own `qs/` quick-start and documentation teach the concepts with, because
  it is the example a reader of dtm's own docs already knows. It was reimplemented as Akka
  entities, not translated from the Go handler bodies.

The probes and benchmark runners in the harness repository build and run `dtm-labs/dtm`
unmodified, from a clone kept beside the harness. They live there, not here, and this
project does not depend on it at build time or at run time.

## What that means for this project's licence

BSD 3-Clause is a permissive licence and imposes no share-alike obligation, so nothing about
the original constrains what this project may be licensed as. Its attribution and
no-endorsement clauses apply to redistributed copies of its own source, and none is included
here; the attribution above is given because it is owed to the work this was derived from,
not because a copied file forces it.

## Also used

- **[Akka](https://akka.io)** — the SDK and runtime this port is built on
  (`akka-javasdk` 3.6.3, Business Source License 1.1).
