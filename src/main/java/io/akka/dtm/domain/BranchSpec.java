package io.akka.dtm.domain;

/** What a caller supplies to name one branch of a transfer when starting a transaction. */
public record BranchSpec(String participantId, BranchRole role, long amount) {}
