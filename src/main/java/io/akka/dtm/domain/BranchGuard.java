package io.akka.dtm.domain;

import java.util.HashSet;
import java.util.Set;

/**
 * The sub-transaction barrier (SPEC-001 §3 rules 7-10, question-log #4-7): guards a
 * participant's business call against two failure modes of at-least-once delivery —
 * "null compensation" (a cancel/compensate/rollback arriving with no matching forward call
 * ever having claimed its slot) and plain duplicate delivery of the same call.
 *
 * <p>A pure decision, deliberately kept free of entity/persistence concerns so it is tested
 * the same way dtm's own barrier was checked in question-log #4-7: by calling it directly.
 */
public record BranchGuard(Set<String> claimedOps) {

  public static BranchGuard empty() {
    return new BranchGuard(Set.of());
  }

  public enum Reason {
    CLAIMED,
    NULL_COMPENSATION,
    DUPLICATE
  }

  public record Decision(BranchGuard next, boolean shouldRun, Reason reason) {}

  /**
   * @param op the operation being requested now (e.g. "cancel")
   * @param originOp the forward operation this op reverses (e.g. "try" for "cancel"), or
   *     {@code null} if {@code op} is itself a forward operation with nothing to reverse
   */
  public Decision claim(String op, String originOp) {
    boolean originAlreadyClaimed = originOp != null && claimedOps.contains(originOp);
    boolean opAlreadyClaimed = claimedOps.contains(op);
    // dtm's barrier.go:76-77 always attempts both inserts (the op's own slot, and — for a
    // reversing op — the origin slot too) before deciding anything; a slot already claimed is
    // simply a no-op insert. Mirrored here: both slots end up claimed regardless of outcome.
    BranchGuard next = withClaimed(op, originOp);

    if (originOp != null && !originAlreadyClaimed) {
      // The forward op's slot was untouched until this call claimed it: nothing to compensate.
      return new Decision(next, false, Reason.NULL_COMPENSATION);
    }
    if (opAlreadyClaimed) {
      return new Decision(next, false, Reason.DUPLICATE);
    }
    return new Decision(next, true, Reason.CLAIMED);
  }

  private BranchGuard withClaimed(String op, String originOp) {
    Set<String> next = new HashSet<>(claimedOps);
    next.add(op);
    if (originOp != null) {
      next.add(originOp);
    }
    return new BranchGuard(Set.copyOf(next));
  }
}
