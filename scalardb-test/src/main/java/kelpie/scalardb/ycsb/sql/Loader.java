package kelpie.scalardb.ycsb.sql;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class Loader extends PreProcessor {

  private final String jdbcUrl;
  private final int concurrency;

  public Loader(Config config) {
    super(config);
    jdbcUrl = "jdbc:scalardb:" + YcsbCommon.getConfigFile(config);
    concurrency = YcsbCommon.getLoadConcurrency(config);
  }

  @Override
  public void execute() {
    ExecutorService executorService = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> new LoadRunner(config, jdbcUrl, i).run(), executorService);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("All records have been inserted");
  }

  @Override
  public void close() {}
}
