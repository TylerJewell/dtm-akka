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
import io.akka.dtm.domain.SagaPhase;
import io.akka.dtm.domain.SagaState;
import io.akka.dtm.domain.TransferBranch;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Saga coordination — SPEC-001 §3 rules 1-3, 11, 12.
 *
 * <p>Forward actions run one at a time, in list order (rule 1); the first failure or
 * exhausted retry limit switches every branch that ever succeeded into compensation, in
 * exact reverse order (rules 2, 3, 11). {@code runForward}/{@code runCompensate} each
 * self-transition to the next branch, which is how a workflow expresses a caller-supplied,
 * dynamic branch count (question-log #14).
 *
 * <p>Unlike dtm, retrying an ongoing branch does not wait on dtm's own doubling backoff
 * (question-log #12) — the next attempt runs on the next step tick, bounded only by the
 * generous workflow timeout below, which is a safety net rather than a working limit (see
 * {@code opal-akka}'s {@code FanOutWorkflow} for the same framing). This is a disclosed
 * divergence, not a gap: the coordination guarantees under test do not depend on the pacing
 * between retries, only on retries happening and eventually giving up.
 */
@Component(id = "saga")
public class SagaWorkflow extends Workflow<SagaState> {

  private final ComponentClient componentClient;
  private final String gid;

  public SagaWorkflow(ComponentClient componentClient, WorkflowContext context) {
    this.componentClient = componentClient;
    this.gid = context.workflowId();
  }

  public record Start(List<BranchSpec> branches, Optional<Integer> retryLimit) {}

  @Override
  public WorkflowSettings settings() {
    return WorkflowSettings.builder()
        .defaultStepTimeout(Duration.ofSeconds(10))
        .timeout(Duration.ofMinutes(5))
        .build();
  }

  public Effect<Done> start(Start start) {
    if (currentState() != null) {
      return effects().error("saga already started");
    }
    List<TransferBranch> branches = TransferBranch.from(start.branches());
    if (branches.isEmpty()) {
      return effects()
          .updateState(SagaState.start(gid, branches, start.retryLimit()).withPhase(SagaPhase.SUCCEEDED))
          .end()
          .thenReply(Done.getInstance());
    }
    return effects()
        .updateState(SagaState.start(gid, branches, start.retryLimit()))
        .transitionTo(SagaWorkflow::runForward)
        .thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<SagaState> getState() {
    if (currentState() == null) {
      return effects().error("saga not started");
    }
    return effects().reply(currentState());
  }

  private StepEffect runForward() {
    SagaState state = currentState();
    int idx = state.cursor();
    if (idx >= state.branches().size()) {
      return stepEffects().updateState(state.withPhase(SagaPhase.SUCCEEDED)).thenEnd();
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.ACTION, branch);
    return switch (outcome) {
      case SUCCEEDED ->
          stepEffects()
              .updateState(state.withBranchSucceeded(idx).withCursor(idx + 1))
              .thenTransitionTo(SagaWorkflow::runForward);
      case FAILED ->
          stepEffects()
              .updateState(state.beginCompensation("branch " + branch.branchId() + " failed"))
              .thenTransitionTo(SagaWorkflow::runCompensate);
      case ONGOING -> {
        if (state.retryLimitExceeded()) {
          yield stepEffects()
              .updateState(state.beginCompensation("retry limit exceeded on branch " + branch.branchId()))
              .thenTransitionTo(SagaWorkflow::runCompensate);
        }
        yield stepEffects()
            .updateState(state.withRetryIncremented())
            .thenTransitionTo(SagaWorkflow::runForward);
      }
    };
  }

  private StepEffect runCompensate() {
    SagaState state = currentState();
    int idx = state.cursor();
    if (idx < 0) {
      return stepEffects().updateState(state.withPhase(SagaPhase.FAILED)).thenEnd();
    }
    TransferBranch branch = state.branches().get(idx);
    BranchOutcome outcome = callBranch(BranchOp.COMPENSATE, branch);
    if (outcome == BranchOutcome.SUCCEEDED) {
      return stepEffects().updateState(state.withCursor(idx - 1)).thenTransitionTo(SagaWorkflow::runCompensate);
    }
    // Rule 11: compensation has no retry limit in dtm either — it keeps trying.
    return stepEffects().thenTransitionTo(SagaWorkflow::runCompensate);
  }

  private BranchOutcome callBranch(BranchOp op, TransferBranch branch) {
    var cmd = new BranchCommand(gid, branch.branchId(), op, branch.role(), branch.amount());
    return componentClient
        .forEventSourcedEntity(branch.participantId())
        .method(AccountEntity::apply)
        .invoke(cmd);
  }
}
