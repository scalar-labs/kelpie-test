package kelpie.scalardb.transfer;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PostProcessor;

public class TransferReporterWithRetry extends PostProcessor {

  public TransferReporterWithRetry(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    getSummary();
    logInfo(
        "==== Statistics Details ====\n"
            + "Transaction retry count: "
            + getPreviousState().getInt("retry_count")
            + "\n");
  }

  @Override
  public void close() {}
}
