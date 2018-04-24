package de.julielab.xmlData.dataBase.util;

public class UnobtainableConnectionException extends RuntimeException {
    public UnobtainableConnectionException() {
    }

    public UnobtainableConnectionException(String message) {
        super(message);
    }

    public UnobtainableConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnobtainableConnectionException(Throwable cause) {
        super(cause);
    }

    public UnobtainableConnectionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
