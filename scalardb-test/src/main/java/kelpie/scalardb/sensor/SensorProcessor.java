package kelpie.scalardb.sensor;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.ProcessFatalException;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.json.Json;
import kelpie.scalardb.Common;

public class SensorProcessor extends TimeBasedProcessor {
  private final DistributedTransactionManager manager;
  private final int numDevices;
  private final AtomicBoolean isVerification;
  private final int startTimestamp;
  private final AtomicInteger numAttempts = new AtomicInteger();

  public SensorProcessor(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
    this.numDevices = (int) config.getUserLong("test_config", "num_devices");
    this.isVerification =
        new AtomicBoolean(config.getUserBoolean("test_config", "is_verification", false));
    this.startTimestamp = (int) (System.currentTimeMillis() / 1000L);
  }

  @Override
  public void executeEach() throws Exception {
    DistributedTransaction transaction = manager.start();

    String txId = transaction.getId();
    int timestamp = (int) (System.currentTimeMillis() / 1000L);
    int deviceId = ThreadLocalRandom.current().nextInt(numDevices);
    logStart(txId, timestamp, deviceId);
    try {
      updateRevision(transaction, timestamp, deviceId);
    } catch (Exception e) {
      logFailure(txId, timestamp, deviceId, e);
      throw e;
    }

    logSuccess(txId, timestamp, deviceId);
  }

  @Override
  public void close() {
    try {
      manager.close();
    } catch (Exception e) {
      logWarn("Failed to close the transaction manager", e);
    }

    int endTimestamp = (int) (System.currentTimeMillis() / 1000L);
    setState(
        Json.createObjectBuilder()
            .add("start_timestamp", startTimestamp)
            .add("end_timestamp", endTimestamp)
            .build());
  }

  private void updateRevision(DistributedTransaction transaction, int timestamp, int deviceId)
      throws TransactionException {
    Scan scan = SensorCommon.prepareScan(timestamp);

    boolean hasDuplicatedRevision;
    List<Result> results;
    boolean scannerUsed = numAttempts.getAndIncrement() % 2 == 0;
    if (!scannerUsed) {
      // Use scan()
      results = transaction.scan(scan);
      hasDuplicatedRevision = SensorCommon.hasDuplicatedRevision(results);
    } else {
      // Use getScanner()
      hasDuplicatedRevision = false;
      results = new ArrayList<>();
      try (TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan)) {
        Set<Integer> tempSet = new HashSet<>();
        while (true) {
          Optional<Result> result = scanner.one();
          if (!result.isPresent()) {
            break;
          }

          int revision = SensorCommon.getRevisionFromResult(result.get());
          if (!tempSet.add(revision)) {
            hasDuplicatedRevision = true;
            break;
          }

          results.add(result.get());
        }
      }
    }

    if (hasDuplicatedRevision) {
      throw new ProcessFatalException(
          "A revision is duplicated. timestamp: " + timestamp + "; scannerUsed: " + scannerUsed);
    }

    int revision = SensorCommon.getMaxRevision(results) + 1;
    Put put = SensorCommon.preparePut(timestamp, deviceId, revision);
    transaction.put(put);

    transaction.commit();
  }

  private void logStart(String txId, int timestamp, int deviceId) {
    if (isVerification.get()) {
      logTxInfo("started", txId, timestamp, deviceId);
    }
  }

  private void logSuccess(String txId, int timestamp, int deviceId) {
    if (isVerification.get()) {
      logTxInfo("succeeded", txId, timestamp, deviceId);
    }
  }

  private void logFailure(String txId, int timestamp, int deviceId, Throwable e) {
    if (isVerification.get()) {
      logTxInfo("started", txId, timestamp, deviceId);
    }

    if (e instanceof UnknownTransactionStatusException) {
      logWarn("the status of the transaction is unknown: " + txId, e);
      logTxInfo("unknown", txId, timestamp, deviceId);
    } else {
      logWarn(txId + " failed", e);
      logTxInfo("failed", txId, timestamp, deviceId);
    }
  }

  private void logTxInfo(String status, String txId, int timestamp, int deviceId) {
    logInfo(status + " - id: " + txId + " timestamp: " + timestamp + " deviceId: " + deviceId);
  }
}
