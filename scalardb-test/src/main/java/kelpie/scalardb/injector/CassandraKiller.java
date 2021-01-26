package kelpie.scalardb.injector;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.InjectionException;
import com.scalar.kelpie.modules.Injector;
import io.github.resilience4j.retry.Retry;
import kelpie.scalardb.Common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class CassandraKiller extends Injector {
  private final Random random = new Random(System.currentTimeMillis());
  private final int maxIntervalSec;
  private final String user;
  private final int port;
  private final String privateKeyFile;
  private final String[] nodes;
  private List<String> targets;

  public CassandraKiller(Config config) {
    super(config);

    maxIntervalSec = (int) config.getUserLong("killer_config", "max_kill_interval_sec", 300L);
    user = config.getUserString("killer_config", "ssh_user", "centos");
    port = (int) config.getUserLong("killer_config", "ssh_port", 22L);
    privateKeyFile = config.getUserString("killer_config", "ssh_private_key");
    nodes = config.getUserString("killer_config", "contact_points", "localhost").split(",");
  }

  @Override
  public void inject() {
    try {
      int waitTime = random.nextInt(maxIntervalSec * 1000);
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      // ignore
    }

    List<String> tmp = Arrays.asList(nodes);
    Collections.shuffle(tmp);
    targets = tmp.subList(0, random.nextInt(nodes.length));

    targets.forEach(
        node -> {
          kill(node);
        });
  }

  @Override
  public void eject() {
    try {
      int waitTime = random.nextInt(maxIntervalSec * 1000);
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      // ignore
    }

    targets.forEach(
        node -> {
          restart(node);
        });
  }

  @Override
  public void close() {
    Retry retry = Common.getRetryWithFixedWaitDuration("checkNodeUp", 5, 60000);

    for (String node : nodes) {
      Runnable decorated = Retry.decorateRunnable(retry, () -> checkNode(node));
      try {
        decorated.run();
      } catch (Exception e) {
        throw new InjectionException(node + " couldn't restart", e);
      }
    }
  }

  private void kill(String node) {
    logInfo("Killing cassandra on " + node);
    String killCommand = "pkill -9 -F /var/run/cassandra/cassandra.pid";
    try {
      execCommand(node, killCommand);
    } catch (Exception e) {
      logWarn("Kill command failed", e);
      // ignore this failure
    }
  }

  private void restart(String node) {
    logInfo("Restarting cassandra on " + node);
    String restartCommand = "/etc/init.d/cassandra start";
    try {
      execCommand(node, restartCommand);
    } catch (Exception e) {
      logWarn("Restart command failed", e);
      // the node will be recovered when close()
    }
  }

  private void checkNode(String node) {
    String checkCommand = "ss -at | grep :9042";
    Retry retry = Common.getRetryWithFixedWaitDuration("checkNodeUp", 10, 10000);
    Runnable decorated = Retry.decorateRunnable(retry, () -> execCommand(node, checkCommand));

    logInfo("Checking Cassandra on " + node);
    try {
      decorated.run();
    } catch (Exception e) {
      restart(node);
      throw e;
    }

    logInfo("Cassandra is running on " + node);
  }

  private void execCommand(String node, String commandStr) {
    String[] command =
        new String[] {
          "ssh",
          "-i",
          privateKeyFile,
          user + "@" + node,
          "-p",
          Integer.toString(port),
          "sudo " + commandStr
        };

    logDebug("Executing " + String.join(" ", command));

    ProcessBuilder pb = new ProcessBuilder(command);
    try {
      Process process = pb.start();
      process.waitFor();
      int ret = process.exitValue();
      if (ret != 0) {
        logDebug("The exit code: " + ret);
        logDebugInputStream("STDOUT", process.getInputStream());
        logDebugInputStream("STDERR", process.getErrorStream());
        throw new InjectionException("SSH command failed. The exit code: " + ret);
      }
    } catch (IOException | InterruptedException e) {
      throw new InjectionException("SSH connection failed", e);
    }
  }

  private void logDebugInputStream(String message, InputStream is) {
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      StringBuilder builder = new StringBuilder(message + ": ");
      br.lines().forEach(builder::append);
      logDebug(builder.toString());
    } catch (IOException ignored) {
    }
  }
}
