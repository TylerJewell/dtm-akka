package io.akka.dtm.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rule 11: the retry-limit arithmetic {@code SagaWorkflow.runForward} drives.
 * dtm allows {@code RetryCount} to reach but not exceed {@code RetryLimit} before aborting
 * (dtmsvr/trans_type_saga.go:179) — checked here at the domain level because the demo
 * participant this port ships never itself returns ONGOING from a saga forward action (see
 * {@code AccountEntityTest#confirmWithoutAPriorHoldIsOngoingNotFailedAndPersistsNothing} for
 * where ONGOING is exercised), so the threshold is verified directly rather than by driving a
 * real stuck branch through a full workflow.
 */
class SagaStateTest {

  private static final List<BranchSpec> ONE_BRANCH =
      List.of(new BranchSpec("acct-1", BranchRole.DEBIT, 10));

  @Test
  void staysUnderTheLimitWhileRetryCountIsBelowIt() {
    var state = SagaState.start("g1", TransferBranch.from(ONE_BRANCH), Optional.of(3));
    assertThat(state.retryLimitExceeded()).isFalse();

    var afterTwo = state.withRetryIncremented().withRetryIncremented();
    assertThat(afterTwo.retryCount()).isEqualTo(2);
    assertThat(afterTwo.retryLimitExceeded()).isFalse();
  }

  @Test
  void exceedsOnceRetryCountReachesTheLimit() {
    var state = SagaState.start("g1", TransferBranch.from(ONE_BRANCH), Optional.of(2))
        .withRetryIncremented()
        .withRetryIncremented();
    assertThat(state.retryCount()).isEqualTo(2);
    assertThat(state.retryLimitExceeded()).isTrue();
  }

  @Test
  void noLimitSetNeverExceeds() {
    var state = SagaState.start("g1", TransferBranch.from(ONE_BRANCH), Optional.empty());
    for (int i = 0; i < 50; i++) {
      state = state.withRetryIncremented();
    }
    assertThat(state.retryLimitExceeded()).isFalse();
  }

  @Test
  void compensationStartsAtTheBranchThatJustFailedNotOneBefore() {
    // Confirmed against real dtm (bench/compare.py, saga_3_last_fails): the branch whose own
    // action just failed is compensated too, not skipped -- the barrier makes that call a
    // no-op when nothing was actually committed.
    var branches =
        TransferBranch.from(
            List.of(
                new BranchSpec("a", BranchRole.DEBIT, 10),
                new BranchSpec("b", BranchRole.CREDIT, 10),
                new BranchSpec("c", BranchRole.CREDIT, 10)));
    // Branches 0 and 1 succeeded; the cursor (2) is the one that just failed.
    var state =
        SagaState.start("g1", branches, Optional.empty())
            .withBranchSucceeded(0)
            .withCursor(1)
            .withBranchSucceeded(1)
            .withCursor(2)
            .beginCompensation("branch 03 failed");

    assertThat(state.phase()).isEqualTo(SagaPhase.COMPENSATING);
    assertThat(state.cursor()).isEqualTo(2); // compensate branch 2 (the one that just failed) first
  }
}
