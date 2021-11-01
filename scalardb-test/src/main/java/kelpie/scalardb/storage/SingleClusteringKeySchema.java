package kelpie.scalardb.storage;

import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.Key;

public final class SingleClusteringKeySchema {

  public static final String NAMESPACE = "storage";
  public static final String TABLE = "single-clustering-key";
  public static final String PARTITION_KEY = "pkey";
  public static final String CLUSTERING_KEY = "ckey";
  public static final String COL = "col";

  public static final int NUM_CLUSTERING_KEY = 100;

  private SingleClusteringKeySchema() {}

  public static Put preparePut(int pkey, int ckey, int colValue) {
    Key partitionKey = new Key(PARTITION_KEY, pkey);
    Key clusteringKey = new Key(CLUSTERING_KEY, ckey);
    return new Put(partitionKey, clusteringKey)
        .withValue(COL, colValue)
        .forNamespace(NAMESPACE)
        .forTable(TABLE);
  }

  public static Scan prepareScan(int pkey, Order ckeyOrder) {
    Key partitionKey = new Key(PARTITION_KEY, pkey);
    return new Scan(partitionKey)
        .withOrdering(new Ordering(CLUSTERING_KEY, ckeyOrder))
        .forNamespace(NAMESPACE)
        .forTable(TABLE);
  }
}
