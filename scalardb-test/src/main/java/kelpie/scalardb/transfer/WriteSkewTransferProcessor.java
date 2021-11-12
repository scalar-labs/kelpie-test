package kelpie.scalardb.transfer;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.ProcessException;
import com.scalar.kelpie.exception.ProcessFatalException;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.json.Json;
import javax.json.JsonObjectBuilder;

public class WriteSkewTransferProcessor extends TimeBasedProcessor {
  private final DistributedTransactionManager manager;
  private final int numAccounts;
  private final AtomicBoolean isVerification;
  private final AtomicBoolean isSerializable;
  private final AtomicBoolean isExtraRead;
  private final AtomicInteger numUpdates = new AtomicInteger(0);

  // for verification
  private final Map<String, List<Integer>> unknownTransactions = new ConcurrentHashMap<>();

  public WriteSkewTransferProcessor(Config config) {
    super(config);
    this.manager = TransferCommon.getTransactionManager(config);

    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    this.isVerification =
        new AtomicBoolean(config.getUserBoolean("test_config", "is_verification", false));
    this.isSerializable =
        new AtomicBoolean(
            config
                .getUserString("storage_config", "isolation_level", "SNAPSHOT")
                .equals("SERIALIZABLE"));
    this.isExtraRead =
        new AtomicBoolean(
            config
                .getUserString("storage_config", "serializable_strategy", "EXTRA_READ")
                .equals("EXTRA_READ"));
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int fromType = ThreadLocalRandom.current().nextInt(TransferCommon.NUM_TYPES);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toType = ThreadLocalRandom.current().nextInt(TransferCommon.NUM_TYPES);
    if ((fromId == toId) && (fromType == toType)) {
      toType = (fromType + 1) % TransferCommon.NUM_TYPES;
    }
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    DistributedTransaction transaction = manager.start();
    String txId = transaction.getId();
    logStart(txId, fromId, fromType, toId, toType, amount);

    try {
      transfer(transaction, fromId, fromType, toId, toType, amount);
    } catch (Exception e) {
      logFailure(txId, fromId, fromType, toId, toType, amount, e);
      throw e;
    }
    logSuccess(txId, fromId, fromType, toId, toType, amount);
  }

  @Override
  public void close() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    unknownTransactions.forEach(
        (txId, ids) -> {
          builder.add(txId, Json.createArrayBuilder().add(ids.get(0)).add(ids.get(1)).build());
        });

    setState(
        Json.createObjectBuilder()
            .add("num_updates", numUpdates.get())
            .add("unknown_transaction", builder.build())
            .build());
  }

  private void transfer(
      DistributedTransaction transaction,
      int fromId,
      int fromType,
      int toId,
      int toType,
      int amount)
      throws TransactionException {
    int fromBalance = 0;
    int totalBalance = 0;

    for (int i = 0; i < TransferCommon.NUM_TYPES; i++) {
      Get get = TransferCommon.prepareGet(config, fromId, i);
      Optional<Result> result = transaction.get(get);
      int balance = TransferCommon.getBalanceFromResult(result.get());
      if (i == fromType) {
        fromBalance = balance;
      }
      totalBalance += balance;
    }

    if (totalBalance < 0) {
      throw new ProcessFatalException("The total balance of " + fromId + " is negative");
    }

    if ((totalBalance - amount) < 0) {
      throw new ProcessException("the account doesn't have the enough balance");
    }

    Get toGet = TransferCommon.prepareGet(config, toId, toType);
    Optional<Result> toResult = transaction.get(toGet);
    int toBalance = TransferCommon.getBalanceFromResult(toResult.get());

    Put fromPut = TransferCommon.preparePut(config, fromId, fromType, fromBalance - amount);
    Put toPut = TransferCommon.preparePut(config, toId, toType, toBalance + amount);
    transaction.put(fromPut);
    transaction.put(toPut);

    transaction.commit();
  }

  private void logStart(String txId, int fromId, int fromType, int toId, int toType, int amount) {
    if (isVerification.get()) {
      logTxInfo("started", txId, fromId, fromType, toId, toType, amount);
    }
  }

  private void logSuccess(String txId, int fromId, int fromType, int toId, int toType, int amount) {
    if (isVerification.get()) {
      logTxInfo("succeeded", txId, fromId, fromType, toId, toType, amount);
      if (isSerializable.get() && !isExtraRead.get()) {
        if (fromId != toId) {
          numUpdates.getAndAdd(TransferCommon.NUM_TYPES + 1);
        } else {
          numUpdates.getAndAdd(TransferCommon.NUM_TYPES);
        }
      } else {
        numUpdates.getAndAdd(2);
      }
    }
  }

  private void logFailure(
      String txId, int fromId, int fromType, int toId, int toType, int amount, Throwable e) {
    if (!isVerification.get()) {
      return;
    }

    if (e instanceof UnknownTransactionStatusException) {
      unknownTransactions.put(txId, Arrays.asList(fromId, fromType, toId, toType));
      logWarn("the status of the transaction is unknown: " + txId, e);
      logTxInfo("unknown", txId, fromId, fromType, toId, toType, amount);
    } else {
      logWarn(txId + " failed", e);
      logTxInfo("failed", txId, fromId, fromType, toId, toType, amount);
    }
  }

  private void logTxInfo(
      String status, String txId, int fromId, int fromType, int toId, int toType, int amount) {
    logInfo(
        status
            + " - id: "
            + txId
            + " from: "
            + fromId
            + ","
            + fromType
            + " to: "
            + toId
            + ","
            + toType
            + " amount: "
            + amount);
  }
}
