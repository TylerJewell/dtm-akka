package io.akka.dtm.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.dtm.domain.Account;
import io.akka.dtm.domain.BranchRole;
import io.akka.dtm.domain.BranchSpec;
import io.akka.dtm.domain.SagaPhase;
import io.akka.dtm.domain.SagaState;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 1-3, against a real running Saga workflow and real account entities, the
 * same shape the transfer was checked against dtm itself in question-log #1-3.
 */
public class SagaWorkflowTest extends TestKitSupport {

  private String openAccount(long initialBalance) {
    String id = "acct-" + UUID.randomUUID();
    componentClient.forEventSourcedEntity(id).method(AccountEntity::open).invoke(initialBalance);
    return id;
  }

  private Account account(String id) {
    return componentClient.forEventSourcedEntity(id).method(AccountEntity::get).invoke();
  }

  private SagaState awaitTerminal(String gid) {
    var box = new SagaState[1];
    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              var state = componentClient.forWorkflow(gid).method(SagaWorkflow::getState).invoke();
              assertThat(state.phase()).isIn(SagaPhase.SUCCEEDED, SagaPhase.FAILED);
              box[0] = state;
            });
    return box[0];
  }

  @Test
  void movesFundsThroughEveryBranchInOrderWhenAllSucceed() {
    String from = openAccount(100);
    String to = openAccount(0);
    String gid = "saga-" + UUID.randomUUID();

    componentClient
        .forWorkflow(gid)
        .method(SagaWorkflow::start)
        .invoke(
            new SagaWorkflow.Start(
                List.of(
                    new BranchSpec(from, BranchRole.DEBIT, 30),
                    new BranchSpec(to, BranchRole.CREDIT, 30)),
                Optional.empty()));

    SagaState state = awaitTerminal(gid);
    assertThat(state.phase()).isEqualTo(SagaPhase.SUCCEEDED);
    assertThat(account(from).balance()).isEqualTo(70);
    assertThat(account(to).balance()).isEqualTo(30);
  }

  @Test
  void aFailedBranchCompensatesEveryEarlierOneInReverseOrder() {
    // Rule 2, 3: the debit succeeds (funds move), the credit branch is fine too, but a third
    // branch that overdraws the destination account fails, so both are undone, most-recent first.
    String from = openAccount(100);
    String to = openAccount(0);
    String third = openAccount(0);
    String gid = "saga-" + UUID.randomUUID();

    componentClient
        .forWorkflow(gid)
        .method(SagaWorkflow::start)
        .invoke(
            new SagaWorkflow.Start(
                List.of(
                    new BranchSpec(from, BranchRole.DEBIT, 30),
                    new BranchSpec(to, BranchRole.CREDIT, 30),
                    // A debit on an empty account: always fails.
                    new BranchSpec(third, BranchRole.DEBIT, 999)),
                Optional.empty()));

    SagaState state = awaitTerminal(gid);
    assertThat(state.phase()).isEqualTo(SagaPhase.FAILED);
    // Both earlier branches were compensated back to their starting balances.
    assertThat(account(from).balance()).isEqualTo(100);
    assertThat(account(to).balance()).isEqualTo(0);
  }

  @Test
  void aTransactionWithNoBranchesSucceedsImmediately() {
    String gid = "saga-" + UUID.randomUUID();
    componentClient.forWorkflow(gid).method(SagaWorkflow::start).invoke(new SagaWorkflow.Start(List.of(), Optional.empty()));

    SagaState state = awaitTerminal(gid);
    assertThat(state.phase()).isEqualTo(SagaPhase.SUCCEEDED);
  }
}
