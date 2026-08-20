package io.akka.dtm.domain;

import java.util.List;

/** One branch of a transfer: a participant account, its role, and how far it has got. */
public record TransferBranch(
    String branchId, String participantId, BranchRole role, long amount, BranchStatus status) {

  public TransferBranch succeeded() {
    return new TransferBranch(branchId, participantId, role, amount, BranchStatus.SUCCEEDED);
  }

  /** Branch ids "01", "02", ... in caller order (question-log #1: forward order is list order). */
  public static List<TransferBranch> from(List<BranchSpec> specs) {
    var branches = new java.util.ArrayList<TransferBranch>();
    for (int i = 0; i < specs.size(); i++) {
      var spec = specs.get(i);
      branches.add(
          new TransferBranch(
              String.format("%02d", i + 1),
              spec.participantId(),
              spec.role(),
              spec.amount(),
              BranchStatus.PENDING));
    }
    return List.copyOf(branches);
  }
}
