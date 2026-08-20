package io.akka.dtm.domain;

import java.util.Map;

/**
 * A participant account. {@code balance} is settled funds; {@code held} is the portion of
 * {@code balance} tentatively reserved by an in-flight TCC/2PC debit branch that has not yet
 * been confirmed or released. {@code guards} is this account's own share of the sub-transaction
 * barrier (SPEC-001 §4 decision #6): one {@link BranchGuard} per {@code gid|branchId} this
 * account has ever been a branch of, folded into the same entity as the balance so a claim and
 * its business effect are one atomic event, mirroring dtm's single local database transaction.
 */
public record Account(long balance, long held, Map<String, BranchGuard> guards) {

  public static Account opened(long initialBalance) {
    return new Account(initialBalance, 0, Map.of());
  }

  public long available() {
    return balance - held;
  }

  public BranchGuard guardFor(String guardKey) {
    return guards.getOrDefault(guardKey, BranchGuard.empty());
  }

  /** Business effect of one guarded branch call. Only meaningful when outcome is SUCCEEDED. */
  public record Decision(BranchOutcome outcome, long balance, long held) {}

  /**
   * The business rules behind each op, by role (SPEC-001 §3 rule 13). Only the debit side
   * reserves ({@code held}); a credit can never overdraw, so its Try/Prepare is a trivial
   * success and its Cancel/Rollback has nothing to release.
   */
  public Decision decide(BranchOp op, BranchRole role, long amount) {
    return switch (role) {
      case DEBIT -> decideDebit(op, amount);
      case CREDIT -> decideCredit(op, amount);
    };
  }

  private Decision decideDebit(BranchOp op, long amount) {
    return switch (op) {
      case ACTION ->
          balance >= amount
              ? new Decision(BranchOutcome.SUCCEEDED, balance - amount, held)
              : new Decision(BranchOutcome.FAILED, balance, held);
      case TRY, PREPARE ->
          available() >= amount
              ? new Decision(BranchOutcome.SUCCEEDED, balance, held + amount)
              : new Decision(BranchOutcome.FAILED, balance, held);
      case CONFIRM, COMMIT ->
          held >= amount
              ? new Decision(BranchOutcome.SUCCEEDED, balance - amount, held - amount)
              : new Decision(BranchOutcome.ONGOING, balance, held);
      case CANCEL, ROLLBACK ->
          held >= amount
              ? new Decision(BranchOutcome.SUCCEEDED, balance, held - amount)
              : new Decision(BranchOutcome.ONGOING, balance, held);
      case COMPENSATE -> new Decision(BranchOutcome.SUCCEEDED, balance + amount, held);
    };
  }

  private Decision decideCredit(BranchOp op, long amount) {
    return switch (op) {
      case ACTION, CONFIRM, COMMIT -> new Decision(BranchOutcome.SUCCEEDED, balance + amount, held);
      case TRY, PREPARE, CANCEL, ROLLBACK -> new Decision(BranchOutcome.SUCCEEDED, balance, held);
      case COMPENSATE ->
          balance >= amount
              ? new Decision(BranchOutcome.SUCCEEDED, balance - amount, held)
              : new Decision(BranchOutcome.ONGOING, balance, held);
    };
  }
}
