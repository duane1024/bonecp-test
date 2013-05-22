package com.fun_in_space;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ConnectionPoolTest {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolTest.class);
  private static DataSource dataSource;
  private static ScheduledExecutorService scheduledExecutorService;

  private static final int THREAD_POOL_SIZE = 10;
  private static final int DELAY_UNTIL_NEXT_TASK = 50;

  public static void main(String[] args) throws SQLException {
    dataSource = createDataSource("localhost", "bonecptest", "test", "test", false, false);

    scheduledExecutorService = Executors.newScheduledThreadPool(THREAD_POOL_SIZE, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Scheduled Thread for ConnectionPoolTest");
        t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread thread, Throwable throwable) {
            logger.error("Uncaught Exception in thread " + thread.getName(), throwable);
          }
        });
        return t;
      }
    });
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        String sql = "SELECT * from person";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                rs.getObject(i);
              }
            }
          }
        } catch (SQLException e) {
          logger.error("Error getting database connection", e);
        }
      }
    }, 500, DELAY_UNTIL_NEXT_TASK, TimeUnit.MILLISECONDS);

  }

  private static DataSource createDataSource(String host, String db, String user, String password, boolean ssl, boolean compress) {
    final BoneCPConfig config = new BoneCPConfig();

    int partitionCount = 2;
    int acquireIncrement = 5;
    int minConnectionsPerPartition = 5;
    int maxConnectionsPerPartition = 50;
    int idleConnectionTestPeriod = 300;
    int connectionTimeoutinMs = 1500;
    int idleMaxAgeInSeconds = 60;
    int maxConnectionAgeInSeconds = 180;

    // turn on statistics so we can view what's happening through the MBean
    config.setStatisticsEnabled(true);

    // **** THIS IS FOR DEBUG PURPOSES ONLY     *******
    // **** SHOULD NOT BE ENABLED IN PRODUCTION *******
    config.setCloseConnectionWatch(true);

    config.setJdbcUrl("jdbc:mysql://" + host + "/" + db +
            "?characterEncoding=UTF-8&autoReconnectForPools=true&zeroDateTimeBehavior=convertToNull" +
            (ssl ? "&verifyServerCertificate=false&useSSL=true&requireSSL=true" : "") +
            (compress ? "&useCompression=true" : ""));
    config.setUsername(user);
    config.setPassword(password);
    config.setMinConnectionsPerPartition(minConnectionsPerPartition);
    config.setMaxConnectionsPerPartition(maxConnectionsPerPartition);
    config.setIdleConnectionTestPeriodInSeconds(idleConnectionTestPeriod);
    config.setIdleMaxAgeInSeconds(idleMaxAgeInSeconds);
    config.setMaxConnectionAgeInSeconds(maxConnectionAgeInSeconds);
    config.setConnectionTestStatement("/* ping */ SELECT 1");
    config.setPartitionCount(partitionCount);
    config.setAcquireIncrement(acquireIncrement);
    config.setConnectionTimeoutInMs(connectionTimeoutinMs);
    return new BoneCPDataSource(config);
  }
}
