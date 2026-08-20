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
 * SPEC-001 §3 rules 5, 6, 9 — every branch's Try must land before any Confirm, and one Try
 * failure cancels every branch already tried.
 */
public class TccWorkflowTest extends TestKitSupport {

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
              var state = componentClient.forWorkflow(gid).method(TccWorkflow::getState).invoke();
              assertThat(state.phase()).isIn(TwoPhasePhase.SUCCEEDED, TwoPhasePhase.FAILED);
              box[0] = state;
            });
    return box[0];
  }

  @Test
  void everyBranchIsTriedBeforeAnyIsConfirmed() {
    String from = openAccount(100);
    String to = openAccount(0);
    String gid = "tcc-" + UUID.randomUUID();

    componentClient
        .forWorkflow(gid)
        .method(TccWorkflow::start)
        .invoke(
            new TccWorkflow.Start(
                List.of(
                    new BranchSpec(from, BranchRole.DEBIT, 40),
                    new BranchSpec(to, BranchRole.CREDIT, 40))));

    TwoPhaseState state = awaitTerminal(gid);
    assertThat(state.phase()).isEqualTo(TwoPhasePhase.SUCCEEDED);
    assertThat(account(from).balance()).isEqualTo(60);
    assertThat(account(from).held()).isEqualTo(0);
    assertThat(account(to).balance()).isEqualTo(40);
  }

  @Test
  void aFailedTryCancelsEveryBranchAlreadyTried() {
    String from = openAccount(100);
    String tooSmall = openAccount(10);
    String gid = "tcc-" + UUID.randomUUID();

    componentClient
        .forWorkflow(gid)
        .method(TccWorkflow::start)
        .invoke(
            new TccWorkflow.Start(
                List.of(
                    new BranchSpec(from, BranchRole.DEBIT, 40), // tries and holds successfully
                    new BranchSpec(tooSmall, BranchRole.DEBIT, 40)))); // try fails: not enough held

    TwoPhaseState state = awaitTerminal(gid);
    assertThat(state.phase()).isEqualTo(TwoPhasePhase.FAILED);
    // The first branch's hold was released, not confirmed.
    assertThat(account(from).balance()).isEqualTo(100);
    assertThat(account(from).held()).isEqualTo(0);
  }
}
