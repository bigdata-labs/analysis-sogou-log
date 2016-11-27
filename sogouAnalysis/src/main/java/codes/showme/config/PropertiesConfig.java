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

    public Properties getProperties() {
        return properties;
    }
}
