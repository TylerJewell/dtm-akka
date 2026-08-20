package io.akka.dtm.domain;

/** Shared across Saga/TCC/2PC branch tracking: has this branch's current phase call landed. */
public enum BranchStatus {
  PENDING,
  SUCCEEDED
}
