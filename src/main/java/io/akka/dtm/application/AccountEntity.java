package io.akka.dtm.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import io.akka.dtm.domain.Account;
import io.akka.dtm.domain.AccountEvent;
import io.akka.dtm.domain.BranchCommand;
import io.akka.dtm.domain.BranchGuard;
import io.akka.dtm.domain.BranchOutcome;
import java.util.HashSet;
import java.util.Map;

/**
 * A participant account, guarded by the sub-transaction barrier (SPEC-001 §3 rules 7-10, §4
 * decision #6). {@link #apply} is the one entry point every Saga/TCC/2PC branch call goes
 * through: the guard claim and the business mutation are decided together and persisted as one
 * event only when the business call actually succeeds, so a failed or still-ongoing call leaves
 * nothing behind for a retry to trip over.
 */
@Component(id = "account")
public class AccountEntity extends EventSourcedEntity<Account, AccountEvent> {

  public Effect<Done> open(Long initialBalance) {
    if (currentState() != null) {
      return effects().error("account already open");
    }
    return effects().persist(new AccountEvent.Opened(initialBalance)).thenReply(s -> Done.getInstance());
  }

  public ReadOnlyEffect<Account> get() {
    if (currentState() == null) {
      return effects().error("account does not exist");
    }
    return effects().reply(currentState());
  }

  public Effect<BranchOutcome> apply(BranchCommand cmd) {
    if (currentState() == null) {
      return effects().error("account does not exist");
    }
    String guardKey = cmd.guardKey();
    BranchGuard guard = currentState().guardFor(guardKey);
    BranchGuard.Decision claim = guard.claim(cmd.op().name(), cmd.op().originOp());

    if (!claim.shouldRun()) {
      if (claim.reason() == BranchGuard.Reason.DUPLICATE) {
        return effects().reply(BranchOutcome.SUCCEEDED);
      }
      // NULL_COMPENSATION: nothing to compensate, but the attempt is still recorded.
      return persistGuardOnly(guardKey, cmd, claim);
    }

    Account.Decision business = currentState().decide(cmd.op(), cmd.role(), cmd.amount());
    if (business.outcome() != BranchOutcome.SUCCEEDED) {
      // Mirrors dtm's local-transaction rollback: nothing persisted, so a retry is a real retry.
      return effects().reply(business.outcome());
    }

    var event =
        new AccountEvent.BranchApplied(
            guardKey,
            cmd.op().name(),
            claim.reason(),
            business.balance(),
            business.held(),
            claim.next().claimedOps());
    return effects().persist(event).thenReply(s -> BranchOutcome.SUCCEEDED);
  }

  private Effect<BranchOutcome> persistGuardOnly(
      String guardKey, BranchCommand cmd, BranchGuard.Decision claim) {
    var event =
        new AccountEvent.BranchApplied(
            guardKey,
            cmd.op().name(),
            claim.reason(),
            currentState().balance(),
            currentState().held(),
            claim.next().claimedOps());
    return effects().persist(event).thenReply(s -> BranchOutcome.SUCCEEDED);
  }

  @Override
  public Account applyEvent(AccountEvent event) {
    return switch (event) {
      case AccountEvent.Opened opened -> Account.opened(opened.initialBalance());
      case AccountEvent.BranchApplied applied -> {
        Map<String, BranchGuard> guards = new java.util.HashMap<>(currentState().guards());
        guards.put(applied.guardKey(), new BranchGuard(new HashSet<>(applied.claimedOpsAfter())));
        yield new Account(applied.balanceAfter(), applied.heldAfter(), Map.copyOf(guards));
      }
    };
  }
}
