package kelpie.scalardb.transfer;

import static kelpie.scalardb.transfer.LedgerTransferCommon.CONFIG_TABLE_NAME;
import static kelpie.scalardb.transfer.LedgerTransferCommon.DEFAULT_METADATA_SIZE;

import com.google.common.base.Strings;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class LedgerTransferProcessorNoAppend extends TimeBasedProcessor {
  private final DistributedTransactionManager manager;
  private final int numAccounts;
  private final boolean isVerification;
  private final boolean useCompactLog;
  private final int metadataSize;

  public LedgerTransferProcessorNoAppend(Config config) {
    super(config);
    this.manager = LedgerTransferCommon.getTransactionManager(config);

    this.numAccounts = (int) config.getUserLong(CONFIG_TABLE_NAME, "num_accounts");
    this.isVerification = config.getUserBoolean(CONFIG_TABLE_NAME, "is_verification", false);
    this.useCompactLog = config.getUserBoolean(CONFIG_TABLE_NAME, "use_compact_log", true);
    this.metadataSize =
        (int) config.getUserLong(CONFIG_TABLE_NAME, "metadata_size", DEFAULT_METADATA_SIZE);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    DistributedTransaction transaction = manager.start();

    String txId;
    try {
      txId = transaction.getId();
    } catch (UnsupportedOperationException ignored) {
      // JdbcTransaction doesn't support getId()
      txId = "nothing";
    }

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
    manager.close();
  }

  private void transfer(DistributedTransaction transaction, int fromId, int toId, int amount)
      throws Exception {
    try {
      Get fromGet = LedgerTransferCommon.prepareGet(fromId);
      Get toGet = LedgerTransferCommon.prepareGet(toId);

      Optional<Result> fromResult = transaction.get(fromGet);
      Optional<Result> toResult = transaction.get(toGet);
      if (!fromResult.isPresent() || !toResult.isPresent()) {
        throw new IllegalArgumentException("Unexpected results returned");
      }
      int fromBalance = LedgerTransferCommon.getBalanceFromResult(fromResult.get());
      int toBalance = LedgerTransferCommon.getBalanceFromResult(toResult.get());

      String metadata = Strings.repeat("*", metadataSize);
      Put fromPut = LedgerTransferCommon.preparePut(fromId, 0, fromBalance - amount, metadata);
      Put toPut = LedgerTransferCommon.preparePut(toId, 0, toBalance + amount, metadata);
      transaction.put(fromPut);
      transaction.put(toPut);

      transaction.commit();
    } catch (Exception e) {
      e.printStackTrace();
      transaction.abort();
      throw e;
    }
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
