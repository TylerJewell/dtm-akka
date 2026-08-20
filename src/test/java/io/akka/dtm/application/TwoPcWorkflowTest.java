package io.akka.dtm.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.dtm.domain.Account;
import io.akka.dtm.domain.BranchRole;
import io.akka.dtm.domain.BranchSpec;
import io.akka.dtm.domain.TwoPhasePhase;
import io.akka.dtm.domain.TwoPhaseState;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 5, 6, 9-10 — the same coordination shape as TCC (question-log #9), with
 * Prepare/Commit/Rollback in place of Try/Confirm/Cancel.
 */
public class TwoPcWorkflowTest extends TestKitSupport {

  private String openAccount(long initialBalance) {
    String id = "acct-" + UUID.randomUUID();
    componentClient.forEventSourcedEntity(id).method(AccountEntity::open).invoke(initialBalance);
    return id;
  }

  private Account account(String id) {
    return componentClient.forEventSourcedEntity(id).method(AccountEntity::get).invoke();
  }

  private TwoPhaseState awaitTerminal(String gid) {
    var box = new TwoPhaseState[1];
    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              var state = componentClient.forWorkflow(gid).method(TwoPcWorkflow::getState).invoke();
              assertThat(state.phase()).isIn(TwoPhasePhase.SUCCEEDED, TwoPhasePhase.FAILED);
              box[0] = state;
            });
    return box[0];
  }

  @Test
  void everyBranchIsPreparedBeforeAnyIsCommitted() {
    String from = openAccount(100);
    String to = openAccount(0);
    String gid = "twopc-" + UUID.randomUUID();

    componentClient
        .forWorkflow(gid)
        .method(TwoPcWorkflow::start)
        .invoke(
            new TwoPcWorkflow.Start(
                List.of(
                    new BranchSpec(from, BranchRole.DEBIT, 25),
                    new BranchSpec(to, BranchRole.CREDIT, 25))));

    TwoPhaseState state = awaitTerminal(gid);
    assertThat(state.phase()).isEqualTo(TwoPhasePhase.SUCCEEDED);
    assertThat(account(from).balance()).isEqualTo(75);
    assertThat(account(to).balance()).isEqualTo(25);
  }

  @Test
  void aFailedPrepareRollsBackEveryBranchAlreadyPrepared() {
    String from = openAccount(100);
    String tooSmall = openAccount(5);
    String gid = "twopc-" + UUID.randomUUID();

    componentClient
        .forWorkflow(gid)
        .method(TwoPcWorkflow::start)
        .invoke(
            new TwoPcWorkflow.Start(
                List.of(
                    new BranchSpec(from, BranchRole.DEBIT, 50),
                    new BranchSpec(tooSmall, BranchRole.DEBIT, 50))));

    TwoPhaseState state = awaitTerminal(gid);
    assertThat(state.phase()).isEqualTo(TwoPhasePhase.FAILED);
    assertThat(account(from).balance()).isEqualTo(100);
    assertThat(account(from).held()).isEqualTo(0);
  }
}
