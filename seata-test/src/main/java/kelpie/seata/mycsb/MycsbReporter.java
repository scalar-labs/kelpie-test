package kelpie.seata.mycsb;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PostProcessor;
import com.scalar.kelpie.stats.Stats;

public class MycsbReporter extends PostProcessor {

  public MycsbReporter(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    Stats stats = getStats();
    if (stats == null) {
      return;
    }
    logInfo(
        "==== Statistics Summary ====\n"
            + "Throughput: "
            + stats.getThroughput(config.getRunForSec())
            + " ops\n"
            + "Succeeded operations: "
            + stats.getSuccessCount()
            + "\n"
            + "Failed operations: "
            + stats.getFailureCount()
            + "\n"
            + "Mean latency: "
            + stats.getMeanLatency()
            + " ms\n"
            + "SD of latency: "
            + stats.getStandardDeviation()
            + " ms\n"
            + "Max latency: "
            + stats.getMaxLatency()
            + " ms\n"
            + "Latency at 50 percentile: "
            + stats.getLatencyAtPercentile(50.0)
            + " ms\n"
            + "Latency at 90 percentile: "
            + stats.getLatencyAtPercentile(90.0)
            + " ms\n"
            + "Latency at 99 percentile: "
            + stats.getLatencyAtPercentile(99.0)
            + " ms\n"
            + "Transaction retry count: "
            + getPreviousState().getString("transaction-retry-count"));
  }

  @Override
  public void close() {}
}
