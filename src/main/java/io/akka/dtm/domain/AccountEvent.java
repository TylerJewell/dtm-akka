package io.akka.dtm.domain;

import akka.javasdk.annotations.TypeName;

/**
 * What happened to an account. A {@link BranchApplied} event is only ever persisted for an
 * outcome of {@link BranchOutcome#SUCCEEDED} — a call that failed or is still ongoing leaves no
 * trace, so a retry gets a genuine second attempt (SPEC-001 §4 decision #6).
 */
public sealed interface AccountEvent {

  @TypeName("opened")
  record Opened(long initialBalance) implements AccountEvent {}

  @TypeName("branch-applied")
  record BranchApplied(
      String guardKey,
      String op,
      BranchGuard.Reason reason,
      long balanceAfter,
      long heldAfter,
      java.util.Set<String> claimedOpsAfter)
      implements AccountEvent {}
}
