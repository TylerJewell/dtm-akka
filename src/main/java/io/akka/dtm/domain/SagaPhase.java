package io.akka.dtm.domain;

public enum SagaPhase {
  RUNNING,
  COMPENSATING,
  SUCCEEDED,
  FAILED
}
