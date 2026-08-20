package io.akka.dtm.domain;

/**
 * Which side of a transfer a branch plays. Only the debit side ever needs to reserve funds
 * (SPEC-001 §3 rule 13): a credit can't overdraw, so its Try/Prepare phase is a trivial
 * success and its Confirm/Commit is the only phase that moves money.
 */
public enum BranchRole {
  DEBIT,
  CREDIT
}
