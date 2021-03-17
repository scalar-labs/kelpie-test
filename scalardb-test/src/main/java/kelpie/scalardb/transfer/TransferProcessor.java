package kelpie.scalardb.transfer;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import javax.json.Json;
import javax.json.JsonObjectBuilder;

public class TransferProcessor extends TimeBasedProcessor {
  private final DistributedTransactionManager manager;
  private final int numAccounts;
  private final boolean isVerification;
  private final boolean useCompactLog;

  // for verification
  private final Map<String, List<Integer>> unknownTransactions = new ConcurrentHashMap<>();

  public TransferProcessor(Config config) {
    super(config);
    this.manager = TransferCommon.getTransactionManager(config);

    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    this.isVerification = config.getUserBoolean("test_config", "is_verification", false);
    this.useCompactLog = config.getUserBoolean("test_config", "use_compact_log", true);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    DistributedTransaction transaction = manager.start();
    String txId = transaction.getId();
    logStart(txId, fromId, toId, amount);

    try {
      transfer(transaction, fromId, toId, amount);
    } catch (Exception e) {
      logFailure(txId, fromId, toId, amount, e);
      throw e;
    }
    logSuccess(txId, fromId, toId, amount);
  }

  @Override
  public void close() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    unknownTransactions.forEach(
        (txId, ids) -> {
          builder.add(txId, Json.createArrayBuilder().add(ids.get(0)).add(ids.get(1)).build());
        });

    setState(Json.createObjectBuilder().add("unknown_transaction", builder.build()).build());
  }

  private void transfer(DistributedTransaction transaction, int fromId, int toId, int amount)
      throws TransactionException {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    Get fromGet = TransferCommon.prepareGet(fromId, fromType);
    Get toGet = TransferCommon.prepareGet(toId, toType);

    Optional<Result> fromResult = transaction.get(fromGet);
    Optional<Result> toResult = transaction.get(toGet);
    int fromBalance = TransferCommon.getBalanceFromResult(fromResult.get());
    int toBalance = TransferCommon.getBalanceFromResult(toResult.get());

    Put fromPut = TransferCommon.preparePut(fromId, fromType, fromBalance - amount);
    Put toPut = TransferCommon.preparePut(toId, toType, toBalance + amount);
    transaction.put(fromPut);
    transaction.put(toPut);

    transaction.commit();
  }

  private void logStart(String txId, int fromId, int toId, int amount) {
    if (isVerification) {
      logTxInfo("started", txId, fromId, toId, amount);
    }
  }

  private void logSuccess(String txId, int fromId, int toId, int amount) {
    if (isVerification) {
      logTxInfo("succeeded", txId, fromId, toId, amount);
    }
  }

  private void logFailure(String txId, int fromId, int toId, int amount, Throwable e) {
    if (!isVerification) {
      return;
    }

    if (e instanceof UnknownTransactionStatusException) {
      unknownTransactions.put(txId, Arrays.asList(fromId, toId));
      logTxWarn("the status of the transaction is unknown: " + txId, e);
      logTxInfo("unknown", txId, fromId, toId, amount);
    } else {
      logTxWarn(txId + " failed", e);
      logTxInfo("failed", txId, fromId, toId, amount);
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
}
