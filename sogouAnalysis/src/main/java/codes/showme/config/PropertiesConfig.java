package codes.showme.config;

import codes.showme.exception.LoadSogouAppConfigFromPropertiesException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by jack on 11/13/16.
 */
public class PropertiesConfig implements Configuration {
    private Properties properties = new Properties();

    public PropertiesConfig() {
        InputStream in = getClass().getResourceAsStream("/env.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            throw new LoadSogouAppConfigFromPropertiesException(e);
        }
    }

    public String getAppName() {
        return properties.getProperty("app_name");
    }

    public String getSparkMasterURL() {
        return properties.getProperty("spark_master_url");
    }

    @Override
    public String getSparkCheckpoint() {
        return properties.getProperty("/tmp/log-analyzer-streaming");
    }

    @Override
    public String getRedisHost() {
        return properties.getProperty("redis_host");
    }

    @Override
    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers");
    }

    @Override
    public String getKafkaAutoOffsetReset() {
        return properties.getProperty("kafka.auto.offset.reset");
    }

    @Override
    public boolean getKafkaEnableAutoCommit() {
        return Boolean.valueOf(properties.getProperty("enable.auto.commit", "false"));
    }

    public Properties getProperties() {
        return properties;
    }
}
