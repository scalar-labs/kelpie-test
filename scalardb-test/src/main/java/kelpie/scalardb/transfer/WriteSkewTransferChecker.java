package kelpie.scalardb.transfer;

import com.google.common.collect.Lists;
import com.scalar.db.api.Result;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.PostProcessException;
import com.scalar.kelpie.modules.PostProcessor;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import kelpie.scalardb.Common;

public class WriteSkewTransferChecker extends PostProcessor {
  private final boolean isSerializable;
  private final boolean isExtraRead;

  public WriteSkewTransferChecker(Config config) {
    super(config);
    this.isSerializable = config.getUserBoolean("test_config", "is_serializable", false);
    this.isExtraRead = config.getUserBoolean("test_config", "is_extra_read", false);
  }

  @Override
  public void execute() {
    boolean isConsistent = false;
    try {
      logInfo("reading latest records ...");
      List<Result> results = TransferCommon.readRecordsWithRetry(config);

      int numUpdates = getNumOfUpdatesFromCoordinator(config);

      isConsistent = isConsistent(results, numUpdates);
    } catch (RuntimeException e) {
      throw new PostProcessException("Failed to read records", e);
    }

    if (!isConsistent) {
      throw new PostProcessException("Inconsistency happened!");
    }
  }

  @Override
  public void close() {}

  private int getNumOfUpdatesFromCoordinator(Config config) {
    Coordinator coordinator = new Coordinator(Common.getStorage(config));
    if (getPreviousState().isNull("unknown_transaction")) {
      // for --only-post
      return 0;
    }
    JsonObject unknownTransactions = getPreviousState().getJsonObject("unknown_transaction");

    int numUpdates = 0;
    for (String txId : unknownTransactions.keySet()) {
      logInfo("checking the status of " + txId);
      boolean isCommitted = Common.isCommitted(coordinator, txId);
      if (isCommitted) {
        JsonArray ids = unknownTransactions.getJsonArray(txId);
        int fromId = ids.getInt(0);
        int fromType = ids.getInt(1);
        int toId = ids.getInt(2);
        int toType = ids.getInt(3);
        logInfo(
            "id: "
                + txId
                + " from: "
                + fromId
                + ","
                + fromType
                + " to: "
                + toId
                + ","
                + toType
                + " succeeded, not failed");
        if (isSerializable && !isExtraRead) {
          if (fromId != toId) {
            numUpdates += TransferCommon.NUM_TYPES + 1;
          } else {
            numUpdates += TransferCommon.NUM_TYPES;
          }
        } else {
          numUpdates += 2;
        }
      }
    }

    return numUpdates;
  }

  private boolean isConsistent(List<Result> results, int addedNumUpdates) {
    int totalVersion = TransferCommon.getActualTotalVersion(results);
    int totalBalance = TransferCommon.getActualTotalBalance(results);

    int numUpdates = 0;
    if (getPreviousState().isNull("num_updates")) {
      logWarn("There is no num_updates since you use `--only-post`");
    } else {
      numUpdates = getPreviousState().getInt("num_updates");
    }
    int expectedTotalVersion = numUpdates + addedNumUpdates;
    int expectedTotalBalance = TransferCommon.getTotalInitialBalance(config);

    List<List<Result>> resultsPerId = Lists.partition(results, TransferCommon.NUM_TYPES);
    boolean noSkew =
        resultsPerId.stream()
            .allMatch(
                rs -> {
                  int total =
                      rs.stream()
                          .reduce(
                              0,
                              (sum, r) -> sum + TransferCommon.getBalanceFromResult(r),
                              (sum1, sum2) -> sum1 + sum2);
                  return total >= 0;
                });

    logInfo("total version: " + totalVersion);
    logInfo("expected total version: " + expectedTotalVersion);
    logInfo("total balance: " + totalBalance);
    logInfo("expected total balance: " + expectedTotalBalance);

    if (totalVersion != expectedTotalVersion) {
      logError("version mismatch !");
      return false;
    }
    if (totalBalance != expectedTotalBalance) {
      logError("balance mismatch !");
      return false;
    }
    if (!noSkew) {
      logError("a total balance is negative !");
      return false;
    }
    return true;
  }
}
