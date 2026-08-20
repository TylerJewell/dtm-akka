package io.akka.dtm.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 7-10, question-log #4-7 — the same four cases checked against dtm's own
 * barrier directly (a Go program against a disposable Postgres), here against the pure
 * decision this port carries the same guarantees with.
 */
class BranchGuardTest {

  @Test
  void nullCompensationSkipsABusinessCallWithNoMatchingForwardClaim() {
    // Question-log #4: cancel arrives, try never did.
    var guard = BranchGuard.empty();
    var decision = guard.claim("CANCEL", "TRY");

    assertThat(decision.shouldRun()).isFalse();
    assertThat(decision.reason()).isEqualTo(BranchGuard.Reason.NULL_COMPENSATION);
  }

  @Test
  void aForwardCallArrivingAfterItsCancelAlreadyClaimedTheSlotIsANoOp() {
    // Question-log #5: cancel first (claims the guard), try arrives late.
    var afterCancel = BranchGuard.empty().claim("CANCEL", "TRY").next();
    var decision = afterCancel.claim("TRY", null);

    assertThat(decision.shouldRun()).isFalse();
  }

  @Test
  void aRetriedDeliveryOfTheSameOpRunsTheBusinessCallOnlyOnce() {
    // Question-log #6: two identical "try" deliveries.
    var first = BranchGuard.empty().claim("TRY", null);
    assertThat(first.shouldRun()).isTrue();

    var second = first.next().claim("TRY", null);
    assertThat(second.shouldRun()).isFalse();
    assertThat(second.reason()).isEqualTo(BranchGuard.Reason.DUPLICATE);
  }

  @Test
  void aGenuineCompensationAfterARealForwardCallRunsTheBusinessCall() {
    // Question-log #7: try really happened, then cancel.
    var afterTry = BranchGuard.empty().claim("TRY", null).next();
    var decision = afterTry.claim("CANCEL", "TRY");

    assertThat(decision.shouldRun()).isTrue();
    assertThat(decision.reason()).isEqualTo(BranchGuard.Reason.CLAIMED);
  }
}
