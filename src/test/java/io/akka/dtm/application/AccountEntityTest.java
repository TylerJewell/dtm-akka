package io.akka.dtm.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.dtm.domain.BranchCommand;
import io.akka.dtm.domain.BranchOp;
import io.akka.dtm.domain.BranchOutcome;
import io.akka.dtm.domain.BranchRole;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 4, 7-10, 13 — one participant's side of every branch call, and the barrier
 * that guards it, exercised through the real entity so persistence (or its absence) is checked
 * too, not just the pure decision (see {@code BranchGuardTest}).
 */
class AccountEntityTest {

  private static EventSourcedTestKit<
          io.akka.dtm.domain.Account, io.akka.dtm.domain.AccountEvent, AccountEntity>
      kit() {
    return EventSourcedTestKit.of("acct-1", AccountEntity::new);
  }

  private static BranchOutcome apply(
      EventSourcedTestKit<io.akka.dtm.domain.Account, io.akka.dtm.domain.AccountEvent, AccountEntity>
          kit,
      String branchId,
      BranchOp op,
      BranchRole role,
      long amount) {
    return kit.method(AccountEntity::apply)
        .invoke(new BranchCommand("gid-1", branchId, op, role, amount))
        .getReply();
  }

  @Test
  void sagaDebitActionMovesFundsImmediatelyAndFailsWhenShort() {
    var kit = kit();
    kit.method(AccountEntity::open).invoke(100L);

    assertThat(apply(kit, "01", BranchOp.ACTION, BranchRole.DEBIT, 30)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(70);

    // Rule 4: insufficient funds is a permanent failure, not retried.
    assertThat(apply(kit, "02", BranchOp.ACTION, BranchRole.DEBIT, 1000)).isEqualTo(BranchOutcome.FAILED);
    assertThat(kit.getState().balance()).isEqualTo(70);
  }

  @Test
  void sagaDebitCompensationCreditsBack() {
    var kit = kit();
    kit.method(AccountEntity::open).invoke(100L);
    apply(kit, "01", BranchOp.ACTION, BranchRole.DEBIT, 30);

    assertThat(apply(kit, "01", BranchOp.COMPENSATE, BranchRole.DEBIT, 30)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(100);
  }

  @Test
  void creditSideTryIsATrivialSuccessAndReservesNothing() {
    // Rule 13, question-log #18: nothing to reserve on the credit side.
    var kit = kit();
    kit.method(AccountEntity::open).invoke(0L);

    assertThat(apply(kit, "01", BranchOp.TRY, BranchRole.CREDIT, 50)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(0);
    assertThat(kit.getState().held()).isEqualTo(0);
  }

  @Test
  void debitSideTccHoldsThenConfirmsIntoARealDebit() {
    var kit = kit();
    kit.method(AccountEntity::open).invoke(100L);

    assertThat(apply(kit, "01", BranchOp.TRY, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(100);
    assertThat(kit.getState().held()).isEqualTo(40);
    assertThat(kit.getState().available()).isEqualTo(60);

    assertThat(apply(kit, "01", BranchOp.CONFIRM, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(60);
    assertThat(kit.getState().held()).isEqualTo(0);
  }

  @Test
  void debitSideHoldCannotExceedAvailableFunds() {
    var kit = kit();
    kit.method(AccountEntity::open).invoke(50L);

    assertThat(apply(kit, "01", BranchOp.PREPARE, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.SUCCEEDED);
    // A second branch trying to hold more than what remains available is rejected.
    assertThat(apply(kit, "02", BranchOp.PREPARE, BranchRole.DEBIT, 20)).isEqualTo(BranchOutcome.FAILED);
  }

  @Test
  void debitSideCancelReleasesAHoldWithoutMovingBalance() {
    var kit = kit();
    kit.method(AccountEntity::open).invoke(100L);
    apply(kit, "01", BranchOp.PREPARE, BranchRole.DEBIT, 40);

    assertThat(apply(kit, "01", BranchOp.ROLLBACK, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(100);
    assertThat(kit.getState().held()).isEqualTo(0);
  }

  @Test
  void nullCompensationThroughTheRealEntityIsANoOpThatStillPersists() {
    // Question-log #4, through the entity: cancel with no prior try changes no funds, but the
    // guard claim is still recorded (dtm inserts the origin-op row unconditionally).
    var kit = kit();
    kit.method(AccountEntity::open).invoke(100L);

    assertThat(apply(kit, "01", BranchOp.CANCEL, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(100);
    assertThat(kit.getState().held()).isEqualTo(0);

    // A genuine try for the same branch/op-pair afterwards is now a no-op too (question-log #5).
    assertThat(apply(kit, "01", BranchOp.TRY, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().held()).isEqualTo(0);
  }

  @Test
  void aRetriedConfirmIsIdempotentAndDoesNotDoubleSpend() {
    // Question-log #6, through the entity.
    var kit = kit();
    kit.method(AccountEntity::open).invoke(100L);
    apply(kit, "01", BranchOp.PREPARE, BranchRole.DEBIT, 40);
    apply(kit, "01", BranchOp.COMMIT, BranchRole.DEBIT, 40);
    assertThat(kit.getState().balance()).isEqualTo(60);

    assertThat(apply(kit, "01", BranchOp.COMMIT, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.SUCCEEDED);
    assertThat(kit.getState().balance()).isEqualTo(60);
  }

  @Test
  void confirmWithoutAPriorHoldIsOngoingNotFailedAndPersistsNothing() {
    // Rule 4: an unexpected state (a confirm racing ahead of its own hold) is reported as
    // still-in-progress, not a permanent failure, and — like a failure — leaves no trace so a
    // correctly-ordered retry can still land.
    var kit = kit();
    kit.method(AccountEntity::open).invoke(100L);

    assertThat(apply(kit, "01", BranchOp.CONFIRM, BranchRole.DEBIT, 40)).isEqualTo(BranchOutcome.ONGOING);
    assertThat(kit.getState().balance()).isEqualTo(100);
    assertThat(kit.getAllEvents()).hasSize(1); // only Opened — the ongoing attempt persisted nothing
  }

  @Test
  void aFailedCallPersistsNothingSoARetryIsARealRetry() {
    var kit = kit();
    kit.method(AccountEntity::open).invoke(10L);

    assertThat(apply(kit, "01", BranchOp.ACTION, BranchRole.DEBIT, 100)).isEqualTo(BranchOutcome.FAILED);
    assertThat(kit.getAllEvents()).hasSize(1); // only the Opened event
  }
}
