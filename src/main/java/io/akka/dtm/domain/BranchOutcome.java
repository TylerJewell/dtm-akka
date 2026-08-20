package io.akka.dtm.domain;

/**
 * The three-way result of a single branch call. SPEC-001 §3 rule 4, question-log #8: dtm
 * distinguishes success, permanent failure and still-in-progress, and only the middle one
 * stops the coordinator from trying again.
 */
public enum BranchOutcome {
  SUCCEEDED,
  FAILED,
  ONGOING
}
