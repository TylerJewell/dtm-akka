package io.akka.dtm.domain;

import java.util.List;
import java.util.Optional;

/**
 * SPEC-001 §2, §3 rules 1-3, 11. {@code cursor} means two different things depending on
 * {@code phase}: while {@code RUNNING} it is the next branch to run forward, in list order
 * (rule 1); while {@code COMPENSATING} it is the next branch to compensate, counting down from
 * the last one that ever succeeded (rule 3).
 */
public record SagaState(
    String gid,
    List<TransferBranch> branches,
    SagaPhase phase,
    int cursor,
    int retryCount,
    Optional<Integer> retryLimit,
    Optional<String> failureReason) {

  public static SagaState start(String gid, List<TransferBranch> branches, Optional<Integer> retryLimit) {
    return new SagaState(gid, branches, SagaPhase.RUNNING, 0, 0, retryLimit, Optional.empty());
  }

  public SagaState withBranchSucceeded(int index) {
    var next = new java.util.ArrayList<>(branches);
    next.set(index, next.get(index).succeeded());
    return new SagaState(gid, List.copyOf(next), phase, cursor, retryCount, retryLimit, failureReason);
  }

  public SagaState withCursor(int newCursor) {
    return new SagaState(gid, branches, phase, newCursor, retryCount, retryLimit, failureReason);
  }

  public SagaState withRetryIncremented() {
    return new SagaState(gid, branches, phase, cursor, retryCount + 1, retryLimit, failureReason);
  }

  public SagaState withPhase(SagaPhase newPhase) {
    return new SagaState(gid, branches, newPhase, cursor, retryCount, retryLimit, failureReason);
  }

  public SagaState beginCompensation(String reason) {
    // The branch at `cursor` is compensated too, not skipped -- confirmed by running dtm
    // (bench/compare.py): it calls every attempted branch's compensate, including the one
    // whose action just failed, and relies on the barrier to make that a no-op when nothing
    // was actually committed (question-log #4).
    return new SagaState(
        gid, branches, SagaPhase.COMPENSATING, cursor, retryCount, retryLimit, Optional.of(reason));
  }

  public boolean retryLimitExceeded() {
    return retryLimit.isPresent() && retryLimit.get() > 0 && retryCount >= retryLimit.get();
  }
}
