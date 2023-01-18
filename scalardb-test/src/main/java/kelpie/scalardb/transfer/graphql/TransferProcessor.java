package kelpie.scalardb.transfer.graphql;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.graphql.GraphQlFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import kelpie.scalardb.Common;
import kelpie.scalardb.transfer.TransferCommon;

public class TransferProcessor extends TimeBasedProcessor {

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
  private final GraphQL graphql;
  private final int numAccounts;
  private final boolean isVerification;
  private final boolean useCompactLog;

  public TransferProcessor(Config config) throws Exception {
    super(config);

    DatabaseConfig databaseConfig = Common.getDatabaseConfig(config);
    GraphQlFactory graphQlFactory =
        GraphQlFactory.newBuilder()
            .transactionFactory(new TransactionFactory(databaseConfig))
            .table(TransferCommon.KEYSPACE, TransferCommon.TABLE)
            .build();
    graphql = graphQlFactory.createGraphQL();

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

  @SuppressWarnings("unchecked")
  private String transfer(int fromId, int toId, int amount) throws Exception {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    Map<String, Object> variablesForKeys =
        ImmutableMap.of("from_id", fromId, "from_type", fromType, "to_id", toId, "to_type", toType);
    ExecutionInput executionInput =
        ExecutionInput.newExecutionInput().query(GET_BALANCES).variables(variablesForKeys).build();
    ExecutionResult executionResult = graphql.execute(executionInput);

    if (!executionResult.getErrors().isEmpty()) {
      throw new GraphQlFailureException("", executionResult.getErrors());
    }

    Map<Object, Object> extensions = executionResult.getExtensions();
    String txId = (String) ((Map<String, Object>) extensions.get("transaction")).get("id");

    Map<String, Map<String, Map<String, Integer>>> data = executionResult.getData();
    int fromBalance =
        data.get("from_account").get(TransferCommon.TABLE).get(TransferCommon.BALANCE);
    int toBalance = data.get("to_account").get(TransferCommon.TABLE).get(TransferCommon.BALANCE);

    Map<String, Object> variables =
        ImmutableMap.<String, Object>builder()
            .putAll(variablesForKeys)
            .put("tx_id", txId)
            .put("from_new_balance", fromBalance - amount)
            .put("to_new_balance", toBalance - amount)
            .build();
    executionInput =
        ExecutionInput.newExecutionInput().query(PUT_BALANCES).variables(variables).build();
    executionResult = graphql.execute(executionInput);

    if (!executionResult.getErrors().isEmpty()) {
      throw new GraphQlFailureException(txId, executionResult.getErrors());
    }

    return txId;
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
    private final List<GraphQLError> errors;

    public GraphQlFailureException(String txId, List<GraphQLError> errors) {
      this.txId = txId;
      this.errors = errors;
    }

    public String getTxId() {
      return txId;
    }

    @Override
    public String getMessage() {
      return errors.stream().map(GraphQLError::getMessage).collect(Collectors.joining(", "));
    }
  }
}