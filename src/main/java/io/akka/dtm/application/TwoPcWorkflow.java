package io.akka.dtm.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import akka.javasdk.workflow.WorkflowContext;
import io.akka.dtm.domain.BranchCommand;
import io.akka.dtm.domain.BranchOp;
import io.akka.dtm.domain.BranchOutcome;
import io.akka.dtm.domain.BranchSpec;
import io.akka.dtm.domain.TransferBranch;
import io.akka.dtm.domain.TwoPhasePhase;
import io.akka.dtm.domain.TwoPhaseState;
import java.time.Duration;
import java.util.List;

/**
 * Two-phase commit coordination — Prepare/Commit/Rollback (SPEC-001 §3 rules 5, 6, 9-10,
 * §1). Generalized from dtm's XA transaction type into a driver-agnostic prepare/commit/
 * rollback protocol (question-log #9-10, session log event A-1) — this is dtm's XA
 * coordination shape with its phase renamed from the internal "action" literal it shares
 * with Saga (question-log #9) to the standard two-phase-commit vocabulary. Structurally the
 * same shape as {@link TccWorkflow}; kept separate for the same reason dtm keeps
 * {@code trans_type_xa.go} separate from {@code trans_type_tcc.go}.
 */
@Component(id = "two-pc")
public class TwoPcWorkflow extends Workflow<TwoPhaseState> {

  private final ComponentClient componentClient;
  private final String gid;

  public TwoPcWorkflow(ComponentClient componentClient, WorkflowContext context) {
    this.componentClient = componentClient;
    this.gid = context.workflowId();
  }

  public record Start(List<BranchSpec> branches) {}

  @Override
  public WorkflowSettings settings() {
    return WorkflowSettings.builder()
        .defaultStepTimeout(Duration.ofSeconds(10))
        .timeout(Duration.ofMinutes(5))
        .build();
  }

  public Effect<Done> start(Start start) {
    if (currentState() != null) {
      return effects().error("two-pc already started");
    }
    List<TransferBranch> branches = TransferBranch.from(start.branches());
    if (branches.isEmpty()) {
      return effects()
          .updateState(TwoPhaseState.start(gid, branches).withPhase(TwoPhasePhase.SUCCEEDED))
          .end()
          .thenReply(Done.getInstance());
    }
    return effects()
        .updateState(TwoPhaseState.start(gid, branches))
        .transitionTo(TwoPcWorkflow::preparePhase)
        .thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<TwoPhaseState> getState() {
    if (currentState() == null) {
      return effects().error("two-pc not started");
    }
    return effects().reply(currentState());
  }

  private StepEffect preparePhase() {
    TwoPhaseState state = currentState();
    int idx = state.cursor();
    if (idx >= state.branches().size()) {
      return stepEffects()
          .updateState(state.withPhase(TwoPhasePhase.CONFIRMING).withCursor(0))
          .thenTransitionTo(TwoPcWorkflow::commitPhase);
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.PREPARE, branch);
    return switch (outcome) {
      case SUCCEEDED ->
          stepEffects()
              .updateState(state.withBranchSucceeded(idx).withCursor(idx + 1))
              .thenTransitionTo(TwoPcWorkflow::preparePhase);
      case FAILED ->
          stepEffects()
              .updateState(state.beginRollback("branch " + branch.branchId() + " prepare failed"))
              .thenTransitionTo(TwoPcWorkflow::rollbackPhase);
      case ONGOING -> stepEffects().thenTransitionTo(TwoPcWorkflow::preparePhase);
    };
  }

  private StepEffect commitPhase() {
    TwoPhaseState state = currentState();
    int idx = state.cursor();
    if (idx >= state.branches().size()) {
      return stepEffects().updateState(state.withPhase(TwoPhasePhase.SUCCEEDED)).thenEnd();
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.COMMIT, branch);
    if (outcome == BranchOutcome.SUCCEEDED) {
      return stepEffects().updateState(state.withCursor(idx + 1)).thenTransitionTo(TwoPcWorkflow::commitPhase);
    }
    return stepEffects().thenTransitionTo(TwoPcWorkflow::commitPhase);
  }

  private StepEffect rollbackPhase() {
    TwoPhaseState state = currentState();
    int idx = state.cursor();
    if (idx < 0) {
      return stepEffects().updateState(state.withPhase(TwoPhasePhase.FAILED)).thenEnd();
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.ROLLBACK, branch);
    if (outcome == BranchOutcome.SUCCEEDED) {
      return stepEffects().updateState(state.withCursor(idx - 1)).thenTransitionTo(TwoPcWorkflow::rollbackPhase);
    }
    return stepEffects().thenTransitionTo(TwoPcWorkflow::rollbackPhase);
  }

  private BranchOutcome callBranch(BranchOp op, TransferBranch branch) {
    var cmd = new BranchCommand(gid, branch.branchId(), op, branch.role(), branch.amount());
    return componentClient
        .forEventSourcedEntity(branch.participantId())
        .method(AccountEntity::apply)
        .invoke(cmd);
  }
}
