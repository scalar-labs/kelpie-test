package kelpie.scalardb.storage;

import static kelpie.scalardb.storage.MultipleClusteringKeySchema.NUM_CLUSTERING_KEY1;
import static kelpie.scalardb.storage.MultipleClusteringKeySchema.NUM_CLUSTERING_KEY2;
import static kelpie.scalardb.storage.MultipleClusteringKeySchema.prepareScan;

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

public class MultipleClusteringKeyReaderProcessor extends TimeBasedProcessor {
  private final DistributedStorage storage;
  private final int numKeys;
  private final Order ckey1Order;
  private final Order ckey2Order;

  public MultipleClusteringKeyReaderProcessor(Config config) {
    super(config);
    storage = TransferCommon.getStorage(config);
    numKeys = (int) config.getUserLong("test_config", "num_keys");

    boolean reverse = config.getUserBoolean("test_config", "reverse_scan", false);
    Order ckey1ClusteringOrder =
        Order.valueOf(config.getUserString("test_config", "ckey1_clustering_order", "ASC"));
    Order ckey2ClusteringOrder =
        Order.valueOf(config.getUserString("test_config", "ckey2_clustering_order", "ASC"));
    ckey1Order = !reverse ? ckey1ClusteringOrder : StorageCommon.reverseOrder(ckey1ClusteringOrder);
    ckey2Order = !reverse ? ckey2ClusteringOrder : StorageCommon.reverseOrder(ckey2ClusteringOrder);
  }

  @Override
  protected void executeEach() throws Exception {
    int pkey = ThreadLocalRandom.current().nextInt(numKeys);
    int ckey1 = ThreadLocalRandom.current().nextInt(NUM_CLUSTERING_KEY1);
    Scan scan = prepareScan(pkey, ckey1, ckey1Order, ckey2Order);
    try (Scanner scanner = storage.scan(scan)) {
      List<Result> results = scanner.all();
      if (results.size() != NUM_CLUSTERING_KEY2) {
        logWarn(
            "the number of results of the scan for (pkey="
                + pkey
                + ", ckey1="
                + ckey1
                + ") should be "
                + NUM_CLUSTERING_KEY2
                + ", but "
                + results.size());
      }
    }
  }

  @Override
  public void close() throws Exception {
    storage.close();
  }
}
