package codes.showme.config;

import java.util.List;

/**
 * Created by jack on 11/13/16.
 */
public interface Configuration {
    String getAppName();
    String getSparkMasterURL();
    String getSparkCheckpoint();
    String getRedisHost();
    String getKafkaBootstrapServers();
    String getKafkaAutoOffsetReset();
    boolean getKafkaEnableAutoCommit();
}
