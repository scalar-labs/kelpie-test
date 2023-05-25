package kelpie.scalardb.storage;

import static kelpie.scalardb.storage.MultipleClusteringKeySchema.NUM_CLUSTERING_KEY1;
import static kelpie.scalardb.storage.MultipleClusteringKeySchema.NUM_CLUSTERING_KEY2;
import static kelpie.scalardb.storage.MultipleClusteringKeySchema.preparePut;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;

public class MultipleClusteringKeyWriterProcessor extends TimeBasedProcessor {
  private final DistributedStorage storage;
  private final int numKeys;

  public MultipleClusteringKeyWriterProcessor(Config config) {
    super(config);
    storage = TransferCommon.getStorage(config);
    numKeys = (int) config.getUserLong("test_config", "num_keys");
  }

  @Override
  protected void executeEach() throws Exception {
    int pkey = ThreadLocalRandom.current().nextInt(numKeys);
    int ckey1 = ThreadLocalRandom.current().nextInt(NUM_CLUSTERING_KEY1);
    int ckey2 = ThreadLocalRandom.current().nextInt(NUM_CLUSTERING_KEY2);
    int colValue = ThreadLocalRandom.current().nextInt();

    Put put = preparePut(pkey, ckey1, ckey2, colValue);
    storage.put(put);
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
