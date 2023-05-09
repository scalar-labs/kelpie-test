package kelpie.scalardb.transfer.graphql;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import kelpie.scalardb.transfer.TransferCommon;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HttpTransferProcessorWithRetry extends TimeBasedProcessor {

  public static final MediaType JSON = MediaType.get("application/json");

  private static final int INITIAL_INTERNAL_MILLS = 10;
  private static final double MULTIPLIER = 2.0;
  private static final int MAX_RETRIES = 10;

  private static final String GET_BALANCES =
      "query balances($from_id: Int!, $from_type: Int!, $to_id: Int!, $to_type: Int!) @transaction {\n"
          + "  from_account: tx_transfer_get(\n"
          + "    get: {key: {account_id: $from_id, account_type: $from_type}}\n"
          + "  ) {\n"
          + "    tx_transfer {\n"
          + "      balance\n"
          + "    }\n"
          + "  }\n"
          + "\n"
          + "  to_account: tx_transfer_get(\n"
          + "    get: {key: {account_id: $to_id, account_type: $to_type}}\n"
          + "  ) {\n"
          + "    tx_transfer {\n"
          + "      balance\n"
          + "    }\n"
          + "  }\n"
          + "}";

  private static final String PUT_BALANCES =
      "mutation updateBalances($tx_id: String!,\n"
          + "                  $from_id: Int!, $from_type: Int!, $from_new_balance: Int,\n"
          + "                  $to_id: Int!, $to_type: Int!, $to_new_balance: Int)"
          + "  @transaction(id: $tx_id, commit: true) {\n"
          + "  from_put: tx_transfer_put(\n"
          + "    put: {key: {account_id: $from_id, account_type: $from_type},\n"
          + "          values: {balance: $from_new_balance}}\n"
          + "  )\n"
          + "  to_put: tx_transfer_put(\n"
          + "    put: {key: {account_id: $to_id, account_type: $to_type},\n"
          + "          values: {balance: $to_new_balance}}\n"
          + "  )\n"
          + "}";

  private final int numAccounts;
  private final String endpointUrl;
  private final OkHttpClient client;
  private final ThreadLocalCookieJar threadLocalCookieJar;
  private final Retry retry;
  private final LongAdder retryCount = new LongAdder();

  public HttpTransferProcessorWithRetry(Config config) {
    super(config);

    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    endpointUrl = config.getUserString("graphql_http", "endpoint_url");

    threadLocalCookieJar = new ThreadLocalCookieJar();

    int concurrency = (int) config.getUserLong("common", "concurrency");
    client =
        new OkHttpClient.Builder()
            .readTimeout(Duration.ofSeconds(60))
            .connectTimeout(Duration.ofSeconds(60))
            .connectionPool(new ConnectionPool(concurrency, 5, TimeUnit.SECONDS))
            .cookieJar(threadLocalCookieJar)
            .build();

    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(MAX_RETRIES)
            .intervalFunction(
                IntervalFunction.ofExponentialBackoff(INITIAL_INTERNAL_MILLS, MULTIPLIER))
            .retryOnException(
                t ->
                    t instanceof GraphQlFailureException
                        && ((GraphQlFailureException) t).isRetryable())
            .build();
    retry = Retry.of("HttpTransferProcessorWithRetry", retryConfig);
    retry
        .getEventPublisher()
        .onRetry(
            e -> {
              logWarn(e.toString(), e.getLastThrowable());
              retryCount.increment();
            });
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try {
      Retry.decorateCheckedRunnable(retry, () -> transfer(fromId, toId, amount)).run();
    } catch (Throwable t) {
      if (t instanceof Exception) {
        throw (Exception) t;
      }

      throw new Exception(t);
    }
  }

  @Override
  public void close() {
    setState(Json.createObjectBuilder().add("retry_count", retryCount.longValue()).build());
  }

  private void transfer(int fromId, int toId, int amount) throws Exception {
    try {
      int fromType = 0;
      int toType = 0;
      if (fromId == toId) {
        toType = 1; // transfer between the same account
      }

      JsonObject variables1 =
          Json.createObjectBuilder()
              .add("from_id", fromId)
              .add("from_type", fromType)
              .add("to_id", toId)
              .add("to_type", toType)
              .build();
      JsonObject response1 = sendRequest(GET_BALANCES, variables1);

      JsonArray errors = response1.getJsonArray("errors");
      if (errors != null) {
        throw new GraphQlFailureException("", errors);
      }

      String txId =
          response1.getJsonObject("extensions").getJsonObject("transaction").getString("id");
      JsonObject data = response1.getJsonObject("data");
      int fromBalance =
          data.getJsonObject("from_account")
              .getJsonObject(TransferCommon.TABLE)
              .getInt(TransferCommon.BALANCE);
      int toBalance =
          data.getJsonObject("to_account")
              .getJsonObject(TransferCommon.TABLE)
              .getInt(TransferCommon.BALANCE);

      JsonObject variables2 =
          Json.createObjectBuilder()
              .add("from_id", fromId)
              .add("from_type", fromType)
              .add("to_id", toId)
              .add("to_type", toType)
              .add("tx_id", txId)
              .add("from_new_balance", fromBalance - amount)
              .add("to_new_balance", toBalance - amount)
              .build();
      JsonObject response2 = sendRequest(PUT_BALANCES, variables2);

      JsonArray errors2 = response2.getJsonArray("errors");
      if (errors2 != null) {
        throw new GraphQlFailureException(txId, errors2);
      }
    } finally {
      // Clear cookies per transaction for load-balancing in the session affinity case.
      threadLocalCookieJar.clearCookies();
    }
  }

  private JsonObject sendRequest(String query, JsonObject variables) throws IOException {
    String requestJsonString;
    JsonObject requestJson =
        Json.createObjectBuilder().add("query", query).add("variables", variables).build();
    try (StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = Json.createWriter(stringWriter)) {
      jsonWriter.write(requestJson);
      requestJsonString = stringWriter.toString();
    }

    RequestBody body = RequestBody.create(requestJsonString, JSON);
    Request request =
        new Request.Builder()
            .url(endpointUrl)
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .post(body)
            .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException("Unexpected code " + response);
      }

      ResponseBody responseBody = response.body();
      assert responseBody != null;
      return Json.createReader(responseBody.charStream()).readObject();
    }
  }

  private static class GraphQlFailureException extends Exception {

    private final String txId;
    private final JsonArray errors;

    public GraphQlFailureException(String txId, JsonArray errors) {
      this.txId = txId;
      this.errors = errors;
    }

    public boolean isRetryable() {
      for (JsonValue error : errors) {
        String exception = error.asJsonObject().getJsonObject("extensions").getString("exception");
        if (exception.contains("CrudConflictException")
            || exception.contains("CommitConflictException")
            || exception.contains("UncommittedRecordException")
            || exception.contains("TransactionNotFoundException")) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String getMessage() {
      return "txId: "
          + txId
          + ", errors: ["
          + errors.stream().map(JsonValue::toString).collect(Collectors.joining(", "))
          + "]";
    }
  }
}
