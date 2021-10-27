package kelpie.scalardb.storage;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.io.Key;

public class NoClusteringKeySchema {

  public static final String NAMESPACE = "storage";
  public static final String TABLE = "no-clustering-key";
  public static final String PARTITION_KEY = "pkey";
  public static final String COL = "col";

  private NoClusteringKeySchema() {}

  public static Put preparePut(int pkey, int colValue) {
    Key partitionKey = new Key(PARTITION_KEY, pkey);
    return new Put(partitionKey).withValue(COL, colValue).forNamespace(NAMESPACE).forTable(TABLE);
  }

  public static Get prepareGet(int pkey) {
    Key partitionKey = new Key(PARTITION_KEY, pkey);
    return new Get(partitionKey).forNamespace(NAMESPACE).forTable(TABLE);
  }
}
