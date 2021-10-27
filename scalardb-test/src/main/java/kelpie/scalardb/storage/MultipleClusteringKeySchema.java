package kelpie.scalardb.storage;

import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.Key;

public final class MultipleClusteringKeySchema {

  public static final String NAMESPACE = "storage";
  public static final String TABLE = "multiple-clustering-key";
  public static final String PARTITION_KEY = "pkey";
  public static final String CLUSTERING_KEY1 = "ckey1";
  public static final String CLUSTERING_KEY2 = "ckey2";
  public static final String COL = "col";

  public static final int NUM_CLUSTERING_KEY1 = 10;
  public static final int NUM_CLUSTERING_KEY2 = 100;

  private MultipleClusteringKeySchema() {}

  public static Put preparePut(int pkey, int ckey1, int ckey2, int colValue) {
    Key partitionKey = new Key(PARTITION_KEY, pkey);
    Key clusteringKey =
        Key.newBuilder().addInt(CLUSTERING_KEY1, ckey1).addInt(CLUSTERING_KEY2, ckey2).build();
    return new Put(partitionKey, clusteringKey)
        .withValue(COL, colValue)
        .forNamespace(NAMESPACE)
        .forTable(TABLE);
  }

  public static Scan prepareScan(int pkey, int ckey1, Order ckey1Order, Order ckey2Order) {
    Key partitionKey = new Key(PARTITION_KEY, pkey);
    Key clusteringKey1 = new Key(CLUSTERING_KEY1, ckey1);
    return new Scan(partitionKey)
        .withStart(clusteringKey1)
        .withEnd(clusteringKey1)
        .withOrdering(new Ordering(CLUSTERING_KEY1, ckey1Order))
        .withOrdering(new Ordering(CLUSTERING_KEY2, ckey2Order))
        .forNamespace(NAMESPACE)
        .forTable(TABLE);
  }
}
