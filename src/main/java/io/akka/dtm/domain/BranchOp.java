package io.akka.dtm.domain;

/**
 * The eight branch operations across the three protocols (question-log #9, dtmimp/consts.go):
 * Saga's action/compensate, TCC's try/confirm/cancel, and this port's prepare/commit/rollback
 * for two-phase commit (renamed from dtm's own internal XA op names — XA's phase 1 reuses
 * Saga's "action" literal and has no op of its own; see SPEC-001 §1 and question-log #9).
 */
public enum BranchOp {
  ACTION(null),
  COMPENSATE("ACTION"),
  TRY(null),
  CONFIRM(null),
  CANCEL("TRY"),
  PREPARE(null),
  COMMIT(null),
  ROLLBACK("PREPARE");

  private final String originOp;

  BranchOp(String originOp) {
    this.originOp = originOp;
  }

  /** The forward op this op reverses, or {@code null} if this op is itself forward-only. */
  public String originOp() {
    return originOp;
  }
}
