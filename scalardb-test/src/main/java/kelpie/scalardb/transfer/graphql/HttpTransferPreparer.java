package kelpie.scalardb.transfer.graphql;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.graphql.server.GraphQlServer;
import com.scalar.kelpie.config.Config;
import java.util.Properties;
import kelpie.scalardb.Common;
import kelpie.scalardb.transfer.TransferCommon;
import kelpie.scalardb.transfer.TransferPreparer;

public class HttpTransferPreparer extends TransferPreparer {

  public HttpTransferPreparer(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    super.execute();

    logInfo("Starting HTTP server...");
    DatabaseConfig databaseConfig = Common.getDatabaseConfig(config);
    Properties properties = databaseConfig.getProperties();
    properties.put("scalar.db.graphql.namespaces", TransferCommon.KEYSPACE);
    properties.put("scalar.db.graphql.graphiql", "false");
    properties.put("scalar.db.graphql.port", config.getUserLong("graphql_http", "port"));
    try {
      GraphQlServer server = new GraphQlServer(properties);
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
