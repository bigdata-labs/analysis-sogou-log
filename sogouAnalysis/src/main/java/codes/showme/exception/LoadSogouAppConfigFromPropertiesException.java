package codes.showme.exception;

import java.io.IOException;

/**
 * Created by jack on 11/13/16.
 */
public class LoadSogouAppConfigFromPropertiesException extends RuntimeException {
    public LoadSogouAppConfigFromPropertiesException(IOException e) {
        super(e);
    }
}
