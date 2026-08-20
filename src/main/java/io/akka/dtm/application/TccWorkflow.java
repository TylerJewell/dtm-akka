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
 * TCC coordination — Try/Confirm/Cancel (SPEC-001 §3 rules 5, 6, 9). Every branch's Try must
 * succeed before any branch's Confirm is issued (rule 5); one Try failure sends every
 * already-tried branch to Cancel instead. Structurally the same shape as {@link TwoPcWorkflow}
 * (question-log #9) — kept as a separate component because Akka's step method references are
 * bound to a concrete workflow class, the same choice dtm's own {@code trans_type_tcc.go} /
 * {@code trans_type_xa.go} split makes (SPEC-001 §4 decision #1 area).
 */
@Component(id = "tcc")
public class TccWorkflow extends Workflow<TwoPhaseState> {

  private final ComponentClient componentClient;
  private final String gid;

  public TccWorkflow(ComponentClient componentClient, WorkflowContext context) {
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
      return effects().error("tcc already started");
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
        .transitionTo(TccWorkflow::tryPhase)
        .thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<TwoPhaseState> getState() {
    if (currentState() == null) {
      return effects().error("tcc not started");
    }
    return effects().reply(currentState());
  }

  private StepEffect tryPhase() {
    TwoPhaseState state = currentState();
    int idx = state.cursor();
    if (idx >= state.branches().size()) {
      return stepEffects()
          .updateState(state.withPhase(TwoPhasePhase.CONFIRMING).withCursor(0))
          .thenTransitionTo(TccWorkflow::confirmPhase);
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.TRY, branch);
    return switch (outcome) {
      case SUCCEEDED ->
          stepEffects()
              .updateState(state.withBranchSucceeded(idx).withCursor(idx + 1))
              .thenTransitionTo(TccWorkflow::tryPhase);
      case FAILED ->
          stepEffects()
              .updateState(state.beginRollback("branch " + branch.branchId() + " try failed"))
              .thenTransitionTo(TccWorkflow::cancelPhase);
      // Rule 9/11: dtm checks no retry limit for TCC's phase 1 — it retries until timeout.
      case ONGOING -> stepEffects().thenTransitionTo(TccWorkflow::tryPhase);
    };
  }

  private StepEffect confirmPhase() {
    TwoPhaseState state = currentState();
    int idx = state.cursor();
    if (idx >= state.branches().size()) {
      return stepEffects().updateState(state.withPhase(TwoPhasePhase.SUCCEEDED)).thenEnd();
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.CONFIRM, branch);
    if (outcome == BranchOutcome.SUCCEEDED) {
      return stepEffects().updateState(state.withCursor(idx + 1)).thenTransitionTo(TccWorkflow::confirmPhase);
    }
    // dtm gives confirm/commit no special failure handling either — retry until it lands.
    return stepEffects().thenTransitionTo(TccWorkflow::confirmPhase);
  }

  private StepEffect cancelPhase() {
    TwoPhaseState state = currentState();
    int idx = state.cursor();
    if (idx < 0) {
      return stepEffects().updateState(state.withPhase(TwoPhasePhase.FAILED)).thenEnd();
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.CANCEL, branch);
    if (outcome == BranchOutcome.SUCCEEDED) {
      return stepEffects().updateState(state.withCursor(idx - 1)).thenTransitionTo(TccWorkflow::cancelPhase);
    }
    return stepEffects().thenTransitionTo(TccWorkflow::cancelPhase);
  }

  private BranchOutcome callBranch(BranchOp op, TransferBranch branch) {
    var cmd = new BranchCommand(gid, branch.branchId(), op, branch.role(), branch.amount());
    return componentClient
        .forEventSourcedEntity(branch.participantId())
        .method(AccountEntity::apply)
        .invoke(cmd);
  }
}
