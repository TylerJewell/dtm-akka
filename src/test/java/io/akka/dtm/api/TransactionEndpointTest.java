package io.akka.dtm.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.dtm.application.AccountEntity;
import io.akka.dtm.domain.BranchRole;
import io.akka.dtm.domain.BranchSpec;
import io.akka.dtm.domain.SagaPhase;
import io.akka.dtm.domain.SagaState;
import io.akka.dtm.domain.TwoPhasePhase;
import io.akka.dtm.domain.TwoPhaseState;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 1-3, 5-6 — the same coordination guarantees as {@code SagaWorkflowTest} and
 * {@code TccWorkflowTest}, driven through the real HTTP surface ({@code httpClient}) instead of
 * {@code componentClient}, so the wire shapes in {@link TransactionEndpoint} and
 * {@link AccountEndpoint} are exercised too, not just the components behind them.
 */
public class TransactionEndpointTest extends TestKitSupport {

  private String openAccount(long initialBalance) {
    String id = "acct-" + UUID.randomUUID();
    componentClient.forEventSourcedEntity(id).method(AccountEntity::open).invoke(initialBalance);
    return id;
  }

  private AccountEndpoint.AccountView accountView(String id) {
    return httpClient
        .GET("/accounts/" + id)
        .responseBodyAs(AccountEndpoint.AccountView.class)
        .invoke()
        .body();
  }

  @Test
  void startingASagaOverHttpMovesFundsThroughEveryBranch() {
    String from = openAccount(100);
    String to = openAccount(0);
    String gid = "saga-http-" + UUID.randomUUID();

    var start =
        new io.akka.dtm.application.SagaWorkflow.Start(
            List.of(new BranchSpec(from, BranchRole.DEBIT, 20), new BranchSpec(to, BranchRole.CREDIT, 20)),
            Optional.empty());
    var response = httpClient.POST("/saga/" + gid).withRequestBody(start).invoke();
    assertThat(response.status()).isEqualTo(akka.http.javadsl.model.StatusCodes.ACCEPTED);

    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              SagaState state =
                  httpClient.GET("/saga/" + gid).responseBodyAs(SagaState.class).invoke().body();
              assertThat(state.phase()).isEqualTo(SagaPhase.SUCCEEDED);
            });

    assertThat(accountView(from).balance()).isEqualTo(80);
    assertThat(accountView(to).balance()).isEqualTo(20);
  }

  @Test
  void startingATwoPcOverHttpCommitsEveryBranch() {
    String from = openAccount(100);
    String to = openAccount(0);
    String gid = "twopc-http-" + UUID.randomUUID();

    var start =
        new io.akka.dtm.application.TwoPcWorkflow.Start(
            List.of(new BranchSpec(from, BranchRole.DEBIT, 15), new BranchSpec(to, BranchRole.CREDIT, 15)));
    var response = httpClient.POST("/twopc/" + gid).withRequestBody(start).invoke();
    assertThat(response.status()).isEqualTo(akka.http.javadsl.model.StatusCodes.ACCEPTED);

    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              TwoPhaseState state =
                  httpClient.GET("/twopc/" + gid).responseBodyAs(TwoPhaseState.class).invoke().body();
              assertThat(state.phase()).isEqualTo(TwoPhasePhase.SUCCEEDED);
            });

    assertThat(accountView(from).balance()).isEqualTo(85);
    assertThat(accountView(to).balance()).isEqualTo(15);
  }
}
