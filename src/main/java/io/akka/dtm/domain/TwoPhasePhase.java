package io.akka.dtm.domain;

/** Shared by TCC and 2PC (SPEC-001 §3 rules 5-6): phase 1 for every branch, then one uniform phase 2. */
public enum TwoPhasePhase {
  PHASE1,
  CONFIRMING,
  CANCELLING,
  SUCCEEDED,
  FAILED
}
