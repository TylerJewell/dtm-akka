# dtm-akka

Coordinates a transfer between two accounts to one outcome — every step applied, or every
step undone — using three different agreement protocols, and stays correct when a step is
retried or arrives twice.

A port of [dtm-labs/dtm](https://github.com/dtm-labs/dtm) onto **Akka**, built with **Akka
Specify**.

---

## Where it came from

`dtm-labs/dtm` is a server that coordinates a change spread across several independent
services so that either all of them take effect or none do, even when one of those services
is slow, unreachable, or answers twice. It offers three ways of doing this — Saga (a list of
steps, each with its own undo), Try-Confirm-Cancel (reserve, then make final or release), and
two-phase commit (prepare everyone, then tell everyone to finish or give up) — plus a guard
every step goes through so a retried or out-of-order call cannot double-apply or undo
something that never happened.

Only those three protocols and that guard were ported. Left alone: dtm's own admin
dashboard (a read-only browser over stored transaction records — see `gui/manifest.json` in
the specification), its `msg` and `workflow` transaction types, a Saga branch order declared
as a graph rather than a plain list, and real two-phase commit against a database's own
transaction manager — this port coordinates prepare/commit/rollback as a protocol, not a
specific database's `XA` statements.

The written specification this was built from lives in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `dtm-port/`.

---

## dtm-labs/dtm → this port

📉 2,064 Go lines → **720 Java lines**<br>
📁 24 files → **20 files**<br>
⚡ 23.0 → **43.0** milliseconds, one transfer, submit to finished<br>
🎯 2 scenarios compared → **2 of 2 agree**<br>
🧪 0 rules broken on purpose to check a test notices → **5**

The timing is each system doing a different amount of work, not only the same decision
twice: dtm's branch stub in that measurement keeps nothing, and this port writes an entry to
each account's own durable history before answering. How each number was measured, and the
ones that did not make this list, are written up next to the specification in
`akka-specify-harness` under `dtm-port/bench/REPORT.md`.

---

## What it took to build

⏱️ **{HOURS} hours** from the first command to the published repository, **{ACTIVE}** of them active<br>
💬 **{TURNS}** exchanges with the model<br>
✍️ **{WRITTEN}** tokens written by the model, **{TOTAL}** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **27** tests

```bash
python toolkit/tokens.py --port dtm
```

The record of every question, and where the time went, lives with the specification.

---

## What it does

- **A Saga's steps run one at a time, in the order given.** The second step does not start
  until the first has finished successfully.
- **The first step that fails undoes everything attempted so far, in exact reverse order —
  including its own attempt.** A step whose own attempt failed is still sent its own undo;
  the guard (below) is what makes that safe when nothing was actually done.
- **A step answers one of three ways: done, failed for good, or still working.** Only the
  middle one stops retries — "still working" is tried again, not treated as a failure.
- **Try-Confirm-Cancel and two-phase commit both wait for every branch's first phase to
  succeed before telling any branch to finish.** One branch's first phase failing sends
  every branch that was asked — including that one — the other instruction instead.
- **A retried or out-of-order call is caught before it does anything twice, or undoes
  something that was never done.** The same undo delivered twice only takes effect once; an
  undo that arrives with no matching attempt does nothing at all.
- **Only a Saga's own steps count against a retry limit, and only while they are still
  being attempted.** Undoing is retried without limit, the same as dtm.

---

## Design decisions

**The coordinator makes every call itself, in both phases.** dtm lets the caller's own code
make the first-phase call directly and only tells dtm about it afterwards; here, the same
Akka component that decides the outcome also places every call. That way one place owns the
whole decision, and nothing has to trust a caller to report back honestly.

**A step is addressed by naming an account, not a web address.** dtm can call any address on
the network; this port can only call an account it already knows about. That trade gets a
call that cannot be misspelled or sent somewhere unexpected, at the cost of only working
with parties known to this service.

**The guard and the balance change together, or neither does.** dtm records its guard and
runs the caller's own database work in one shared save; here, both live in the same account
and are written down in the same step. A step that fails leaves no trace of having been
tried, so trying it again is a real second try.

**Two-phase commit is a protocol here, not a specific database's own commit.** dtm's version
can use a real database's built-in two-phase commit; this port asks each side to prepare,
then commit or roll back, without needing a database that offers that natively. That keeps
the same three-step agreement while working with any account, not only ones backed by a
particular database.

**A step that is still working is tried again right away, not on a countdown.** dtm waits
longer between each retry of a stuck step; this port tries again on the next opportunity and
relies on an overall time limit as a backstop. Both give up eventually — this port just
does not slow down on the way there.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/dtm-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3.** The service answers on **port 9014**. There is no page to open — it is a service
other programs talk to.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

### Try it

Open two accounts and move money between them with a Saga:

```bash
curl -X POST localhost:9014/accounts/alice -H 'Content-Type: application/json' \
  -d '{"initialBalance": 100}'
curl -X POST localhost:9014/accounts/bob -H 'Content-Type: application/json' \
  -d '{"initialBalance": 0}'

curl -X POST localhost:9014/saga/transfer-1 -H 'Content-Type: application/json' -d '{
  "branches": [
    {"participantId": "alice", "role": "DEBIT", "amount": 30},
    {"participantId": "bob", "role": "CREDIT", "amount": 30}
  ],
  "retryLimit": null
}'

curl localhost:9014/saga/transfer-1
curl localhost:9014/accounts/alice
```

Do the same transfer as Try-Confirm-Cancel or two-phase commit by posting to `/tcc/{gid}`
or `/twopc/{gid}` with the same body shape.

Watch a step fail and everything already done get undone: ask for more than an account
holds, and the whole transfer ends `FAILED` with every balance back where it started.

---

## What it answers

| Request | What it does |
|---|---|
| `POST /accounts/{id}` | Open an account with a starting balance |
| `GET /accounts/{id}` | Its balance, and how much of that is on hold |
| `POST /saga/{gid}` | Start a Saga transfer across the given branches |
| `GET /saga/{gid}` | The transfer's progress and outcome |
| `POST /tcc/{gid}` | Start the same kind of transfer as Try-Confirm-Cancel |
| `GET /tcc/{gid}` | Its progress and outcome |
| `POST /twopc/{gid}` | Start the same kind of transfer as two-phase commit |
| `GET /twopc/{gid}` | Its progress and outcome |

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | | The port is set in `src/main/resources/application.conf`; nothing else is configurable |

---

## Where it differs from dtm-labs/dtm

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **Who makes the first-phase call, for Try-Confirm-Cancel and two-phase commit.** dtm has
  the caller's own business code make that call directly and only learns about the branch
  when the caller reports it; this port's coordinator makes every call itself, in both
  phases, because the Akka component this is built on is meant to drive every step rather
  than sit and wait to be told about one.
- **How a step is addressed.** dtm calls any web address; this port calls an account it
  already knows about, because the Akka tool used to make that call only works that way.
  Nothing that talks to dtm today can be pointed at this port without changes.
- **How long a stuck step waits before being tried again.** dtm waits longer each time, up
  to ten seconds and doubling; this port tries again at the next opportunity, relying on an
  overall time limit rather than a lengthening wait, because matching that exact wait was
  not needed to keep the same guarantee: give up eventually, never do the step twice.
  `not checked`: whether either pacing is kinder to an overloaded downstream service under
  real load.
- **How long a finished transfer's guard record is kept.** dtm ages old guard records out
  after a set number of days. This port keeps every guard record on the account it belongs
  to for as long as the account exists, because building an equivalent ageing-out mechanism
  was judged more work than this port's scope calls for; see the specification's open
  decisions for the reasoning.
- **Real two-phase commit against a database's own transaction manager.** `not attempted` in
  the sense that matters here: dtm can hand this off to a real database's built-in
  two-phase commit, and this port always plays that role itself, in application code, for
  any account regardless of what stores it.

---

## Licence

`dtm-labs/dtm` is BSD 3-Clause, © 2021 yedf. This port copies no source from it and
reimplements the behaviour from a written specification; see
[`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
