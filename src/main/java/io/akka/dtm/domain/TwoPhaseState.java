package io.akka.dtm.domain;

import java.util.List;
import java.util.Optional;

/**
 * State shared by TCC and 2PC (SPEC-001 §3 rule 6, question-log #9): both register branches,
 * run everyone's phase-1 call, then dispatch one uniform phase-2 call to every branch. {@code
 * cursor} is the next branch to call in whichever phase is current.
 */
public record TwoPhaseState(
    String gid, List<TransferBranch> branches, TwoPhasePhase phase, int cursor, Optional<String> failureReason) {

  public static TwoPhaseState start(String gid, List<TransferBranch> branches) {
    return new TwoPhaseState(gid, branches, TwoPhasePhase.PHASE1, 0, Optional.empty());
  }

  public TwoPhaseState withBranchSucceeded(int index) {
    var next = new java.util.ArrayList<>(branches);
    next.set(index, next.get(index).succeeded());
    return new TwoPhaseState(gid, List.copyOf(next), phase, cursor, failureReason);
  }

  public TwoPhaseState withCursor(int newCursor) {
    return new TwoPhaseState(gid, branches, phase, newCursor, failureReason);
  }

  public TwoPhaseState withPhase(TwoPhasePhase newPhase) {
    return new TwoPhaseState(gid, branches, newPhase, cursor, failureReason);
  }

  /**
   * Every branch tried, including `cursor` itself even though its own Try/Prepare just
   * failed -- confirmed by running dtm (bench/compare.py): a branch is registered with dtm
   * before its phase-1 call is made (client/dtmcli/trans_tcc.go:63-75), so it is already on
   * dtm's list by the time that call can fail, and dtm's rollback dispatch does not filter
   * it back out. The barrier makes calling Cancel/Rollback on it a safe no-op.
   */
  public TwoPhaseState beginRollback(String reason) {
    return new TwoPhaseState(gid, branches, TwoPhasePhase.CANCELLING, cursor, Optional.of(reason));
  }
}
