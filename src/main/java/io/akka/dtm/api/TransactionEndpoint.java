package io.akka.dtm.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpResponses;
import io.akka.dtm.application.SagaWorkflow;
import io.akka.dtm.application.TccWorkflow;
import io.akka.dtm.application.TwoPcWorkflow;
import io.akka.dtm.domain.SagaState;
import io.akka.dtm.domain.TwoPhaseState;

/**
 * Start and inspect a distributed transaction. One route family per protocol (SPEC-001 §4
 * decision #5): dtm answers one unified query for any {@code gid} because every transaction
 * type shares one server-side table; here each protocol is its own typed Workflow component,
 * so a caller names which kind of transaction it is asking about.
 */
@HttpEndpoint
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class TransactionEndpoint {

  private final ComponentClient componentClient;

  public TransactionEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Post("/saga/{gid}")
  public HttpResponse startSaga(String gid, SagaWorkflow.Start start) {
    componentClient.forWorkflow(gid).method(SagaWorkflow::start).invoke(start);
    return HttpResponses.accepted();
  }

  @Get("/saga/{gid}")
  public SagaState getSaga(String gid) {
    return componentClient.forWorkflow(gid).method(SagaWorkflow::getState).invoke();
  }

  @Post("/tcc/{gid}")
  public HttpResponse startTcc(String gid, TccWorkflow.Start start) {
    componentClient.forWorkflow(gid).method(TccWorkflow::start).invoke(start);
    return HttpResponses.accepted();
  }

  @Get("/tcc/{gid}")
  public TwoPhaseState getTcc(String gid) {
    return componentClient.forWorkflow(gid).method(TccWorkflow::getState).invoke();
  }

  @Post("/twopc/{gid}")
  public HttpResponse startTwoPc(String gid, TwoPcWorkflow.Start start) {
    componentClient.forWorkflow(gid).method(TwoPcWorkflow::start).invoke(start);
    return HttpResponses.accepted();
  }

  @Get("/twopc/{gid}")
  public TwoPhaseState getTwoPc(String gid) {
    return componentClient.forWorkflow(gid).method(TwoPcWorkflow::getState).invoke();
  }
}
