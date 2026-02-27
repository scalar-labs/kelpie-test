package scalardl.transfer;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PostProcessor;

/**
 * Post-processor that reports the summary of the transfer test run.
 */
public class TransferReporter extends PostProcessor {

  /**
   * Creates a TransferReporter with the given Kelpie config.
   *
   * @param config Kelpie configuration
   */
  public TransferReporter(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    getSummary();
  }

  @Override
  public void close() {}
}
