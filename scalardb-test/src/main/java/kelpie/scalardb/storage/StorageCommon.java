package kelpie.scalardb.storage;

import com.scalar.db.api.Scan.Ordering.Order;

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
}
