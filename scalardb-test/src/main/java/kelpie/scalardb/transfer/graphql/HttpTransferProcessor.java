package kelpie.scalardb.transfer.graphql;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import kelpie.scalardb.transfer.TransferCommon;

public class HttpTransferProcessor extends TimeBasedProcessor {

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
  private final boolean isVerification;
  private final boolean useCompactLog;
  private final URL endpointUrl;

  public HttpTransferProcessor(Config config) throws Exception {
    super(config);

    endpointUrl =
        new URL("http", "localhost", (int) config.getUserLong("graphql_http", "port"), "/graphql");

    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    this.isVerification = config.getUserBoolean("test_config", "is_verification", false);
    this.useCompactLog = config.getUserBoolean("test_config", "use_compact_log", true);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    logStart(fromId, toId, amount);

    String txId;
    try {
      txId = transfer(fromId, toId, amount);
    } catch (Exception e) {
      logFailure(fromId, toId, amount, e);
      throw e;
    }
    logSuccess(txId, fromId, toId, amount);
  }

  @Override
  public void close() {}

  private String transfer(int fromId, int toId, int amount) throws Exception {
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

    return txId;
  }

  private JsonObject sendRequest(String query, JsonObject variables) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) endpointUrl.openConnection();
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("Accept", "application/json");
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");

    JsonObject requestJson =
        Json.createObjectBuilder().add("query", query).add("variables", variables).build();
    try (JsonWriter writer = Json.createWriter(connection.getOutputStream())) {
      writer.write(requestJson);
    }

    try (JsonReader jsonReader = Json.createReader(connection.getInputStream())) {
      return jsonReader.readObject();
    } catch (IOException e) {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(connection.getErrorStream()))) {
        logError(reader.lines().collect(Collectors.joining()));
      }
      throw e;
    } finally {
      connection.disconnect();
    }
  }

  private void logStart(int fromId, int toId, int amount) {
    if (isVerification) {
      logTxInfo("started", null, fromId, toId, amount);
    }
  }

  private void logSuccess(String txId, int fromId, int toId, int amount) {
    if (isVerification) {
      logTxInfo("succeeded", txId, fromId, toId, amount);
    }
  }

  private void logFailure(int fromId, int toId, int amount, Throwable e) {
    if (!isVerification) {
      return;
    }

    if (e instanceof GraphQlFailureException) {
      GraphQlFailureException e2 = (GraphQlFailureException) e;
      logTxWarn(e2.getTxId() + " failed", e2);
      logTxInfo("failed", e2.getTxId(), fromId, toId, amount);
    } else {
      logTxWarn("failed", e);
      logTxInfo("failed", null, fromId, toId, amount);
    }
  }

  private void logTxInfo(String status, String txId, int fromId, int toId, int amount) {
    logInfo(
        status
            + " - id: "
            + txId
            + " from: "
            + fromId
            + ",0"
            + " to: "
            + toId
            + ","
            + ((fromId == toId) ? 1 : 0)
            + " amount: "
            + amount);
  }

  private void logTxWarn(String message, Throwable e) {
    if (useCompactLog) {
      String cause = e.getMessage();
      if (e.getCause() != null) {
        cause = cause + " < " + e.getCause().getMessage();
      }
      logWarn(message + ", cause: " + cause);
    } else {
      logWarn(message, e);
    }
  }

  static class GraphQlFailureException extends Exception {

    private final String txId;
    private final JsonArray errors;

    public GraphQlFailureException(String txId, JsonArray errors) {
      this.txId = txId;
      this.errors = errors;
    }

    public String getTxId() {
      return txId;
    }

    @Override
    public String getMessage() {
      return errors.stream().map(JsonValue::toString).collect(Collectors.joining(", "));
    }
  }
}
