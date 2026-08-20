package io.akka.dtm.api;

import akka.Done;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.dtm.application.AccountEntity;
import io.akka.dtm.domain.Account;

/**
 * Set up and inspect the participant accounts a transaction transfers between.
 *
 * <p>Opened up for access from the public internet to make this port easy to try out; a
 * production service would scope this more tightly (see {@code akka-sdk} access-control docs).
 */
@HttpEndpoint("/accounts")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class AccountEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;

  public AccountEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public record OpenBody(long initialBalance) {}

  /** The public view of an account: balance and held funds. The barrier's guard bookkeeping
   * ({@link Account#guards()}) is internal and not exposed over HTTP. */
  public record AccountView(long balance, long held, long available) {
    static AccountView of(Account account) {
      return new AccountView(account.balance(), account.held(), account.available());
    }
  }

  @Post("/{id}")
  public Done open(String id, OpenBody body) {
    return componentClient
        .forEventSourcedEntity(id)
        .method(AccountEntity::open)
        .invoke(body.initialBalance());
  }

  @Get("/{id}")
  public AccountView get(String id) {
    Account account = componentClient.forEventSourcedEntity(id).method(AccountEntity::get).invoke();
    return AccountView.of(account);
  }
}
