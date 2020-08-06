package kelpie.scalardb.transfer;

import com.scalar.db.api.Result;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.PostProcessException;
import com.scalar.kelpie.modules.PostProcessor;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import kelpie.scalardb.Common;

public class TransferChecker extends PostProcessor {

  public TransferChecker(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    boolean isConsistent = false;
    try {
      logInfo("reading latest records ...");
      List<Result> results = TransferCommon.readRecordsWithRetry(config);

      int committed = getNumOfCommittedFromCoordinator(config);

      isConsistent = isConsistent(results, committed);
    } catch (RuntimeException e) {
      throw new PostProcessException("Failed to read records", e);
    }

    if (!isConsistent) {
      throw new PostProcessException("Inconsistency happened!");
    }
  }

  @Override
  public void close() {}

  private int getNumOfCommittedFromCoordinator(Config config) {
    Coordinator coordinator = new Coordinator(Common.getStorage(config));
    JsonObject unknownTransactions = getPreviousState().getJsonObject("unknown_transaction");
    if (unknownTransactions == null) {
      // for --only-post
      return 0;
    }
    int committed = 0;

    for (String txId : unknownTransactions.keySet()) {
      logInfo("checking the status of " + txId);
      boolean isCommitted = Common.isCommitted(coordinator, txId);
      if (isCommitted) {
        JsonArray ids = unknownTransactions.getJsonArray(txId);
        logInfo(
            "id: "
                + txId
                + " from: "
                + ids.getInt(0)
                + " to: "
                + ids.getInt(1)
                + " succeeded, not failed");
        committed++;
      }
    }

    return committed;
  }

  private boolean isConsistent(List<Result> results, int committed) {
    int totalVersion = TransferCommon.getActualTotalVersion(results);
    int totalBalance = TransferCommon.getActualTotalBalance(results);
    int expectedTotalVersion = ((int) getStats().getSuccessCount() + committed) * 2;
    int expectedTotalBalance = TransferCommon.getTotalInitialBalance(config);

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
    return true;
  }
}
