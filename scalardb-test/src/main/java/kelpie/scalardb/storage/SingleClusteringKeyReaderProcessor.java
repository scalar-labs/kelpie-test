package kelpie.scalardb.storage;

import static kelpie.scalardb.storage.SingleClusteringKeySchema.NUM_CLUSTERING_KEY;
import static kelpie.scalardb.storage.SingleClusteringKeySchema.prepareScan;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Scanner;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;

public class SingleClusteringKeyReaderProcessor extends TimeBasedProcessor {
  private final DistributedStorage storage;
  private final int numKeys;
  private final Order ckeyOrder;

  public SingleClusteringKeyReaderProcessor(Config config) {
    super(config);
    storage = TransferCommon.getStorage(config);
    numKeys = (int) config.getUserLong("test_config", "num_keys");

    boolean reverse = config.getUserBoolean("test_config", "reverse_scan", false);
    Order ckey1ClusteringOrder =
        Order.valueOf(config.getUserString("test_config", "ckey_clustering_order", "ASC"));
    ckeyOrder = !reverse ? ckey1ClusteringOrder : StorageCommon.reverseOrder(ckey1ClusteringOrder);
  }

  @Override
  protected void executeEach() throws Exception {
    int pkey = ThreadLocalRandom.current().nextInt(numKeys);
    Scan scan = prepareScan(pkey, ckeyOrder);
    try (Scanner scanner = storage.scan(scan)) {
      List<Result> results = scanner.all();
      if (results.size() != NUM_CLUSTERING_KEY) {
        logWarn(
            "the number of results of the scan for (pkey="
                + pkey
                + ") should be "
                + NUM_CLUSTERING_KEY
                + ", but "
                + results.size());
      }
    }
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      logWarn("Failed to close the storage", e);
    }
  }
}
