package io.akka.dtm.domain;

/** One guarded branch call, as a coordinator (SPEC-001 §4 decision #5) sends it to a participant. */
public record BranchCommand(String gid, String branchId, BranchOp op, BranchRole role, long amount) {

  public String guardKey() {
    return gid + "|" + branchId;
  }
}
