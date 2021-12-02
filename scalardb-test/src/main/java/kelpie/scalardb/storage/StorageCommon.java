package kelpie.scalardb.storage;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.ArrayList;
import java.util.List;

public final class StorageCommon {

  public static final long DEFAULT_POPULATION_CONCURRENCY = 32L;

  private StorageCommon() {}

  public static Order reverseOrder(Order order) {
    switch (order) {
      case ASC:
        return Order.DESC;
      case DESC:
        return Order.ASC;
      default:
        throw new AssertionError("unknown order: " + order);
    }
  }

  public static void batchPut(DistributedStorage storage, List<Put> puts, int batchSize)
      throws ExecutionException {
    List<Put> buffer = new ArrayList<>(batchSize);
    for (Put put : puts) {
      buffer.add(put);
      if (buffer.size() == batchSize) {
        storage.put(buffer);
        buffer.clear();
      }
    }
    if (!buffer.isEmpty()) {
      storage.put(buffer);
    }
  }
}
